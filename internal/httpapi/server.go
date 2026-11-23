package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/adapter"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/engine"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/observe"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
)

type ProgressOptions struct {
	Enabled          bool
	Categories       map[string]bool
	MinIntervalMS    int64
	MaxEvents        int64
	TextChars        int64
	IncludeRequestID bool
	Texts            map[string]string
	ToolClasses      map[string]string
}

type Options struct {
	PublicModel          string
	MaxRequestBytes      int64
	MaxMessageChars      int64
	MaxMediaPartBytes    int64
	MaxTools             int64
	DialogueLockWaitMS   int64
	ClientTotalTimeoutMS int64
	StreamWriteTimeoutMS int64
	SessionIdleTTLHours  int64
	Progress             ProgressOptions
}

type Server struct {
	engine              *engine.Engine
	store               *store.Store
	catalog             *catalog.Catalog
	log                 *slog.Logger
	opts                Options
	locks               *keyedLocks
	now                 func() time.Time
	manifestEventID     string
	manifestUpdatedAtMS int64
}

func New(eng *engine.Engine, st *store.Store, cat *catalog.Catalog, log *slog.Logger, opts Options) *Server {
	return &Server{
		engine:              eng,
		store:               st,
		catalog:             cat,
		log:                 log,
		opts:                opts,
		locks:               newKeyedLocks(),
		now:                 time.Now,
		manifestEventID:     store.NewID(),
		manifestUpdatedAtMS: time.Now().UnixMilli(),
	}
}

func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(s.withRequestID)
	r.Use(s.withLogging)
	r.Use(s.withRecovery)
	r.NotFound(func(w http.ResponseWriter, _ *http.Request) {
		writeFailure(w, newFailure(http.StatusNotFound, "not_found"))
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeFailure(w, newFailure(http.StatusMethodNotAllowed, "method_not_allowed"))
	})
	r.Get("/healthz", s.health)
	r.Get("/v1/models", s.models)
	r.Post("/v1/chat/completions", s.chat)
	r.Post("/chat/completions", s.chat)
	r.Post("/v1/responses", s.responses)
	r.Post("/responses", s.responses)
	return r
}

func (s *Server) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get("X-Request-Id"))
		if id == "" {
			id = observe.NewRequestID()
		}
		w.Header().Set("X-Request-Id", id)
		next.ServeHTTP(w, r.WithContext(observe.WithRequestID(r.Context(), id)))
	})
}

func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		writer := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(writer, r)
		status := writer.statusCode()
		if status == 0 {
			// Nothing was written: the handler stopped because the client is
			// gone, so record the interruption instead of a misleading 200.
			status = http.StatusOK
			if clientCanceled(r.Context()) {
				status = statusClientClosedRequest
			}
		}
		s.log.Info("http request",
			"request_id", observe.RequestID(r.Context()),
			"method", r.Method,
			"path", r.URL.Path,
			"status", status,
			"duration_ms", time.Since(started).Milliseconds())
	})
}

func (s *Server) withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.log.Error("http handler panic", "request_id", observe.RequestID(r.Context()), "panic", recovered)
				writeFailure(w, newFailure(http.StatusInternalServerError, "internal"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) models(w http.ResponseWriter, _ *http.Request) {
	raw, err := json.Marshal(s.catalog.Models)
	if err != nil {
		writeFailure(w, newFailure(http.StatusInternalServerError, "internal"))
		return
	}
	sum := sha256.Sum256(raw)
	revision := hex.EncodeToString(sum[:8])
	manifest := adapter.UnionCapabilities(s.catalog.Models, revision,
		s.manifestUpdatedAtMS, s.manifestEventID)
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data": []map[string]any{{
			"id":                    s.opts.PublicModel,
			"object":                "model",
			"owned_by":              "coffeebreak",
			"schema_version":        manifest.SchemaVersion,
			"catalog_revision":      manifest.CatalogRevision,
			"updated_at_ms":         manifest.UpdatedAtMS,
			"event_id":              manifest.EventID,
			"x-coffee-capabilities": manifest.Capabilities,
			"x-coffee-limit":        manifest.Limit,
			"x-coffee-variants":     manifest.Variants,
		}},
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(s.opts.ClientTotalTimeoutMS)*time.Millisecond)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		s.log.Error("health check failed", "request_id", observe.RequestID(r.Context()), "error", err)
		writeFailure(w, &failure{status: http.StatusServiceUnavailable, code: "internal",
			message: "Storage is not ready"})
		return
	}
	if len(s.catalog.Models) == 0 {
		writeFailure(w, &failure{status: http.StatusServiceUnavailable, code: "internal",
			message: "Catalog is empty"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readBody(r *http.Request) ([]byte, *failure) {
	limit := s.opts.MaxRequestBytes
	if r.ContentLength > limit {
		return nil, newFailure(http.StatusRequestEntityTooLarge, "request_too_large")
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, newFailure(http.StatusBadRequest, "invalid_json")
	}
	if int64(len(raw)) > limit {
		return nil, newFailure(http.StatusRequestEntityTooLarge, "request_too_large")
	}
	if len(raw) == 0 {
		return nil, newFailure(http.StatusBadRequest, "invalid_json")
	}
	return raw, nil
}

func decodeJSON(raw []byte, target any) *failure {
	if err := json.Unmarshal(raw, target); err != nil {
		return newFailure(http.StatusBadRequest, "invalid_json")
	}
	return nil
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(body)
}

// statusCode reports the written status, or zero when nothing was written.
func (w *statusWriter) statusCode() int {
	return w.status
}

func (w *statusWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
