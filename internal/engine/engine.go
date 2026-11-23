package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/adapter"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/ledger"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/router"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/transport"
)

type Options struct {
	PublicModel             string
	PausePhrase             string
	StreamTextScope         string
	Proxy                   bool
	OutputLimitMode         string
	MaxGatewayTurns         int64
	MaxDelegationSteps      int64
	MaxOutputTokens         int64
	CompactPacketChars      int64
	CompactEvidenceChars    int64
	DeflateThresholdChars   int64
	DeflatorReasoningTokens int64
	ExpertDelegationOnly    bool
	HistoryCompactPct       int64
	HistorySummaryPct       int64
	CharsPerToken           int64
	MaxTools                int64
	Quota429CooldownMS      int64
	ClientToolsTimeoutMS    int64
	RetryAfterMaxMS         int64
	RotationCooldownMS      int64
	RotationErrorThresh     int64
	ObjectiveMode           string
	ReasoningMode           string
	SubscriptionMode        string
	BalanceTurns            int64
	JevMaxCallsPerTurn      int64
	ToolScopes              map[string][]string
}

type Engine struct {
	store     *store.Store
	catalog   *catalog.Catalog
	router    *router.Router
	ledger    *ledger.Ledger
	transport *transport.Transport
	log       *slog.Logger
	opts      Options
}

func New(st *store.Store, cat *catalog.Catalog, rt *router.Router, led *ledger.Ledger,
	tr *transport.Transport, log *slog.Logger, opts Options) *Engine {
	return &Engine{store: st, catalog: cat, router: rt, ledger: led, transport: tr, log: log, opts: opts}
}

type Input struct {
	Endpoint          string
	Dialogue          *store.Dialogue
	Messages          []transport.Message
	Tools             []transport.Tool
	ToolChoice        json.RawMessage
	Stream            bool
	MaxTokens         *int64
	ReasoningEffort   string
	Temperature       *float64
	TopP              *float64
	FrequencyPenalty  *float64
	PresencePenalty   *float64
	Seed              *int64
	Stop              json.RawMessage
	N                 *int64
	ResponseFormat    json.RawMessage
	ParallelToolCalls *bool
	RequestID         string
	Client            string
	Fingerprint       string
}

type Participant struct {
	ID   string `json:"id"`
	Role string `json:"role"`
}

// ProgressCounts is the per-turn activity summary of the progress stream.
type ProgressCounts struct {
	Commands int64 `json:"commands"`
	Edits    int64 `json:"edits"`
	Reads    int64 `json:"reads"`
	Thoughts int64 `json:"thoughts"`
	Tools    int64 `json:"tools"`
}

// Progress is a transient description of what the turn is doing. It is emitted
// as a stream chunk only: it never reaches message, delivery, model_call or
// journal_event (progress-stream.md §6).
type Progress struct {
	Kind      string          `json:"kind"`
	Stage     string          `json:"stage"`
	Category  string          `json:"category"`
	Role      string          `json:"role,omitempty"`
	ModelID   string          `json:"model_id,omitempty"`
	RequestID string          `json:"request_id,omitempty"`
	Level     int             `json:"level,omitempty"`
	Reason    string          `json:"reason,omitempty"`
	Class     string          `json:"class,omitempty"`
	Counts    *ProgressCounts `json:"counts,omitempty"`
}

// usageRoles fixes both the order and the set of roles in the token report:
// the adjuster is not written to model_call (Q9) and is not reported.
var usageRoles = []string{"assistant", "expert", "deflator"}

type Result struct {
	Content          string
	ReasoningContent string
	ToolCalls        []transport.ToolCall
	FinishReason     string
	Usage            *transport.Usage
	UsageReport      []transport.UsageRow
	Participants     []Participant
	Level            int
	Model            string
	DeliverPhase     string
}

type Sink struct {
	Text         func(text string) error
	Reasoning    func(text string) error
	ToolCall     func(delta transport.ToolCallDelta) error
	Finish       func(reason string) error
	Ping         func() error
	Participants func(list []Participant) error
	UsageReport  func(rows []transport.UsageRow) error
	Progress     func(event Progress) error
}

func (s Sink) text(text string) error {
	if s.Text == nil || text == "" {
		return nil
	}
	return s.Text(text)
}

func (s Sink) reasoning(text string) error {
	if s.Reasoning == nil || text == "" {
		return nil
	}
	return s.Reasoning(text)
}

func (s Sink) toolCall(delta transport.ToolCallDelta) error {
	if s.ToolCall == nil {
		return nil
	}
	return s.ToolCall(delta)
}

func (s Sink) finish(reason string) error {
	if s.Finish == nil {
		return nil
	}
	return s.Finish(reason)
}

func (s Sink) ping() error {
	if s.Ping == nil {
		return nil
	}
	return s.Ping()
}

func (s Sink) participants(list []Participant) error {
	if s.Participants == nil || len(list) == 0 {
		return nil
	}
	return s.Participants(list)
}

func (s Sink) usageReport(rows []transport.UsageRow) error {
	if s.UsageReport == nil || len(rows) == 0 {
		return nil
	}
	return s.UsageReport(rows)
}

func (s Sink) progress(event Progress) error {
	if s.Progress == nil {
		return nil
	}
	return s.Progress(event)
}

// statusClientClosedRequest mirrors the contract's 499 code for a client that
// abandoned the turn. It is never sent back to the client.
const statusClientClosedRequest = 499

type Error struct {
	Code         string
	Status       int
	Message      string
	RetryAfterMS int64
	Err          error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return e.Message
	}
	return fmt.Sprintf("%s: %v", e.Message, e.Err)
}

func (e *Error) Unwrap() error {
	return e.Err
}

func fail(code string, status int, message string, err error) *Error {
	return &Error{Code: code, Status: status, Message: message, Err: err}
}

type turn struct {
	engine               *Engine
	input                Input
	clientMessages       []transport.Message
	dialogue             *store.Dialogue
	sink                 Sink
	settings             router.Settings
	usage                transport.Usage
	actors               *participants
	modelCall            int64
	usageByRole          map[string]*transport.Usage
	reasoningGroup       string
	reasoningSanitized   bool
	reasoningWarningSent bool
	newRequest           bool
	clientHistory        bool
	instructions         []transport.Message
}

func (e *Engine) Run(ctx context.Context, in Input, sink Sink) (Result, error) {
	if in.Dialogue == nil {
		return Result{}, fail("internal", http.StatusInternalServerError, "Dialogue is missing", errors.New("engine: dialogue is nil"))
	}
	clientMessages := append([]transport.Message(nil), in.Messages...)
	if hasCommandTags(in.Messages) {
		e.log.Warn("incoming command tag is stripped", "request_id", in.RequestID)
	}
	in.Messages = sanitizePromptMessages(in.Messages)
	ctx = router.WithBudget(ctx, router.NewBudget(e.opts.JevMaxCallsPerTurn))
	t := &turn{engine: e, input: in, clientMessages: clientMessages,
		dialogue: in.Dialogue, sink: sink, actors: newParticipants(),
		instructions: instructionsOf(in.Messages)}
	result, err := t.run(ctx)
	if errors.Is(err, errTurnStepLimit) {
		return t.finishTurnStepLimit(ctx)
	}
	if err != nil {
		t.reportReasoningSanitization()
	}
	return result, err
}

func (t *turn) run(ctx context.Context) (Result, error) {
	e, in := t.engine, t.input
	settings, err := e.router.Settings(ctx)
	if err != nil {
		return Result{}, fail("internal", http.StatusInternalServerError, "Settings are unavailable", err)
	}
	t.settings = settings
	t.reasoningGroup = store.NewID()
	compactionFailed, err := e.store.CompactionFailed(ctx, t.dialogue.ID)
	if err != nil {
		return Result{}, fail("internal", http.StatusInternalServerError, "Dialogue state is unavailable", err)
	}
	if compactionFailed {
		return Result{}, fail("invalid_request", http.StatusUnprocessableEntity,
			"Context compaction failed; start a new OpenCode session", nil)
	}
	lastRole := ""
	if len(in.Messages) > 0 {
		lastRole = in.Messages[len(in.Messages)-1].Role
	}
	hasToolResults := lastRole == "tool"
	if hasToolResults && e.opts.ClientToolsTimeoutMS > 0 && clientToolsPhase(in.Dialogue.Phase) {
		elapsedMS := e.store.NowMS() - in.Dialogue.UpdatedAtMS
		expired := elapsedMS > e.opts.ClientToolsTimeoutMS
		t.engine.log.Info("client tool wait diagnostics",
			"dialogue_id", t.dialogue.ID,
			"request_id", t.input.RequestID,
			"turn_request_id", t.dialogue.RequestID,
			"phase", t.dialogue.Phase,
			"wait_ms", elapsedMS,
			"timeout_ms", e.opts.ClientToolsTimeoutMS,
			"expired", expired,
		)
		if expired {
			t.writeStopPhase(ctx, "client_tools", "timeout")
			return Result{}, fail("timeout", http.StatusGatewayTimeout,
				"The client did not return tool results in time", nil)
		}
	}
	newHuman := lastRole == "user" && !isServiceReminder(in.Messages[len(in.Messages)-1])
	free := freePhase(in.Dialogue.Phase)
	fresh := in.Dialogue.RequestID == ""
	open := newHuman && (free || fresh)
	t.newRequest = open
	if open {
		requestID := in.RequestID
		if requestID == "" {
			requestID = store.NewID()
		}
		t.dialogue.RequestID = requestID
		t.dialogue.TurnSteps = 0
		edge := "user_to_adjuster_1"
		if e.opts.Proxy {
			edge = "user_to_expert"
		}
		if err := t.phase(ctx, edge); err != nil {
			return Result{}, err
		}
		if err := e.store.UpdateDialogue(ctx, t.dialogue); err != nil {
			return Result{}, fail("internal", http.StatusInternalServerError, "Dialogue is unavailable", err)
		}
	}
	if err := t.syncHistory(ctx, in.Messages); err != nil {
		if errors.Is(err, errCompactionFailed) {
			return Result{}, t.markCompactionFailed(ctx, err)
		}
		return Result{}, fail("internal", http.StatusInternalServerError, "History is unavailable", err)
	}
	if !t.newRequest && t.dialogue.TurnSteps >= e.opts.MaxGatewayTurns {
		return Result{}, errTurnStepLimit
	}
	t.engine.log.Info("turn",
		"dialogue_id", t.dialogue.ID,
		"request_id", t.input.RequestID,
		"phase", t.dialogue.Phase,
		"dialogue_level", t.dialogue.Level,
		"owner", t.dialogue.Owner,
		"new_request", t.newRequest,
		"client_history", t.clientHistory,
		"messages", len(in.Messages),
		"input_tokens", t.estimateInputTokens(),
		"tools", len(in.Tools),
		"stream", in.Stream,
	)
	if e.opts.Proxy {
		if hasToolResults {
			if err := t.phase(ctx, "client_tools_to_expert"); err != nil {
				return Result{}, err
			}
		}
		return t.runExpertOnly(ctx)
	}
	if hasToolResults {
		if _, lookupErr := e.store.ActiveDelegation(ctx, in.Dialogue.ID); lookupErr == nil {
			return t.runExpertWithDelegation(ctx)
		} else if !errors.Is(lookupErr, store.ErrNotFound) {
			return Result{}, fail("internal", http.StatusInternalServerError, "Delegation state is unavailable", lookupErr)
		}
	}
	// Every new human message is decided by the adjuster, even when the dialogue
	// already escalated to the expert on an earlier request (ADR 0009 §8).
	if t.newRequest && !hasToolResults {
		level := "assistant"
		var startErr error
		state := t.jevState(ctx)
		if fit, ok := t.engine.router.FitState(state, "context", "request"); ok {
			level, startErr = t.engine.router.DecideStart(ctx, fit)
			t.releaseJevStart(ctx, startErr)
		} else {
			startErr = errors.New("the state does not fit Jev")
			t.releaseJevStart(ctx, startErr)
		}
		t.engine.log.Info("entry decision", "dialogue_id", t.dialogue.ID,
			"request_id", t.input.RequestID, "route", level, "error", startErr)
		if startErr != nil {
			t.progress(Progress{Kind: "error", Stage: t.dialogue.Phase, Category: "error",
				Reason: "upstream_error"})
		} else {
			t.progress(Progress{Kind: "decision", Stage: t.dialogue.Phase, Category: "decision",
				Reason: "route_" + level})
		}
		if level == "expert" {
			return t.runExpert(ctx)
		}
		t.dialogue.Owner = "assistant"
		t.dialogue.Level = 1
		if err := e.store.UpdateDialogue(ctx, t.dialogue); err != nil {
			return Result{}, fail("internal", http.StatusInternalServerError, "Dialogue is unavailable", err)
		}
	}
	if in.Dialogue.Owner == "expert" {
		if hasToolResults {
			if err := t.phase(ctx, "client_tools_to_expert"); err != nil {
				return Result{}, err
			}
		}
		return t.runExpert(ctx)
	}
	if hasToolResults {
		if err := t.phase(ctx, "client_tools_to_assistant_1"); err != nil {
			return Result{}, err
		}
	}
	return t.runAssistant(ctx)
}

func (t *turn) finishTurnStepLimit(ctx context.Context) (Result, error) {
	delegation, err := t.engine.store.ActiveDelegation(ctx, t.dialogue.ID)
	if err == nil {
		if err := t.engine.store.CompleteDelegationSteps(ctx, delegation.ID, t.incomingToolCallIDs()); err != nil {
			return Result{}, fail("internal", http.StatusInternalServerError, "Delegation steps are unavailable", err)
		}
		if err := t.engine.store.FinishDelegation(ctx, delegation.ID, "budget_exhausted"); err != nil {
			return Result{}, fail("internal", http.StatusInternalServerError, "Delegation is unavailable", err)
		}
		material := delegationMaterial(delegation.AssistantModelID, delegation.Scope,
			delegation.BudgetSteps, "delegation stopped: max_gateway_turns reached")
		if err := t.appendDelegationResult(ctx, material); err != nil {
			return Result{}, fail("internal", http.StatusInternalServerError, "History is unavailable", err)
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return Result{}, fail("internal", http.StatusInternalServerError, "Delegation state is unavailable", err)
	}
	delivered := "adjuster_2_to_user"
	if t.dialogue.Owner == "expert" {
		delivered = "expert_to_user"
	}
	return t.finalize(ctx, "", "", "length", delivered)
}

// releaseJevStart records that the entry decision was unavailable. The request
// then goes to the assistant, which is the safe default.
func (t *turn) releaseJevStart(ctx context.Context, err error) {
	if err == nil {
		return
	}
	_ = t.engine.store.Journal(ctx, store.JournalEvent{DialogueID: t.dialogue.ID, Level: "warn",
		Kind: "jev_failed", Role: "assistant", RequestID: t.dialogue.RequestID,
		Note: "the entry decision is unavailable, the assistant handles the request"})
}

func (t *turn) journalEmptyGroup(group string) {
	t.engine.log.Warn("capability group is empty",
		"request_id", t.dialogue.RequestID, "group_name", group,
		"reason", "no candidate after filters")
}

// freePhase reports whether the last status ended the request, so the next
// human message opens a new request instead of joining the active one.
func freePhase(phase string) bool {
	return phase == "" || strings.HasSuffix(phase, "_to_user") || strings.HasSuffix(phase, "_stop")
}

// clientToolsPhase reports whether the dialogue waits for client tool results.
func clientToolsPhase(phase string) bool {
	switch phase {
	case "assistant_1_to_client_tools", "expert_to_client_tools", "assistant_2_to_client_tools":
		return true
	default:
		return false
	}
}

func (t *turn) runAssistant(ctx context.Context) (Result, error) {
	candidates, err := t.assistantCandidates(ctx, false)
	if err != nil {
		return Result{}, err
	}
	if len(candidates) == 0 {
		t.writeStopPhase(ctx, "adjuster_1", "no_available_model")
		t.journalEmptyGroup("coffee-assistant")
		return Result{}, fail("no_available_model", http.StatusServiceUnavailable, "The coffee-assistant group is empty", nil)
	}
	chosen, reused, err := t.pinAssistant(ctx, candidates)
	if err != nil {
		return Result{}, err
	}
	modelReason := "selected"
	if reused {
		modelReason = "kept"
	}
	t.progress(Progress{Kind: "model", Stage: t.dialogue.Phase, Category: "model",
		Role: "assistant", ModelID: chosen.Model.ID, Reason: modelReason})
	if !reused {
		if err := t.phase(ctx, "adjuster_1_to_assistant_1"); err != nil {
			return Result{}, err
		}
		t.dialogue.Owner = "assistant"
		t.dialogue.AssistantModelID = chosen.Model.ID
		if err := t.engine.store.UpdateDialogue(ctx, t.dialogue); err != nil {
			return Result{}, fail("internal", http.StatusInternalServerError, "Dialogue is unavailable", err)
		}
	}
	tools := append(append([]transport.Tool{}, t.input.Tools...),
		virtualTools(adapter.ToolAssistantNeedsHelp)...)
	if t.modelCall > 0 {
		if err := t.sink.ping(); err != nil {
			return Result{}, fail("internal", http.StatusInternalServerError, "Client stream is unavailable", err)
		}
	}
	messages, err := t.historyMessages(ctx, chosen.Model, assistantPrompt(t.promptOptions()), tools)
	if err != nil {
		return Result{}, err
	}
	outcome, err := t.callActor(ctx, actorAssistant, chosen.Model, messages, tools, false)
	if err != nil {
		return Result{}, err
	}
	if len(outcome.ToolCalls) > 0 {
		if err := t.phase(ctx, "assistant_1_to_client_tools"); err != nil {
			return Result{}, err
		}
		return t.returnToolCalls(ctx, outcome)
	}
	needsExpert := outcome.NeedsExpert
	jevDecided := false
	empty := strings.TrimSpace(outcome.Text) == ""
	suspectReason := ""
	if empty {
		suspectReason = "empty_answer"
	}
	if outcome.Sanitized {
		if suspectReason != "" {
			suspectReason += "+sanitized"
		} else {
			suspectReason = "sanitized"
		}
	}
	suspect := suspectReason != ""
	if !needsExpert && empty && !t.engine.router.JevAvailable() {
		t.writeStop(ctx, actorAssistant, "empty_answer")
		return Result{}, fail("empty_answer", http.StatusBadGateway, "The model returned an empty answer", nil)
	}
	var material strings.Builder
	if !needsExpert {
		if err := t.phase(ctx, "assistant_1_to_adjuster_2"); err != nil {
			return Result{}, err
		}
		accept, reason, checkErr := t.checkAnswer(ctx, outcome.Text, suspect, suspectReason)
		t.engine.log.Info("answer checked",
			"dialogue_id", t.dialogue.ID, "request_id", t.input.RequestID,
			"accepted", accept, "reason", reason, "error", checkErr, "suspect", suspect)
		if checkErr != nil {
			_ = t.engine.store.Journal(ctx, store.JournalEvent{DialogueID: t.dialogue.ID, Level: "warn",
				Kind: "jev_failed", Role: "assistant", RequestID: t.dialogue.RequestID,
				Note: "check_answer failed, answer accepted by default"})
			accept = true
		}
		needsExpert = !accept || empty
		jevDecided = true
		if !accept {
			material.WriteString("check_answer: the assistant answer above was rejected.\n")
			if reason != "" {
				material.WriteString("reason: ")
				material.WriteString(reason)
				material.WriteString("\n")
			}
		}
	}
	if !needsExpert {
		if err := t.appendAssistantText(ctx, outcome.Text, t.reasoningGroupID()); err != nil {
			return Result{}, fail("internal", http.StatusInternalServerError, "History is unavailable", err)
		}
		return t.finalize(ctx, outcome.Text, outcome.ReasoningContent, outcome.FinishReason, "adjuster_2_to_user")
	}
	if suspect {
		material.WriteString("answer_suspect: ")
		material.WriteString(suspectReason)
		material.WriteString("\n")
	}
	if outcome.NeedsExpertReason != "" {
		material.WriteString("assistant_needs_expert: ")
		material.WriteString(outcome.NeedsExpertReason)
		material.WriteString("\n")
	}
	if material.Len() == 0 {
		material.WriteString("assistant_needs_expert: the assistant stopped without a result\n")
	}
	edge := "assistant_1_to_expert"
	if jevDecided {
		edge = "adjuster_2_to_expert"
	}
	if err := t.phase(ctx, edge); err != nil {
		return Result{}, err
	}
	return t.runExpertWithMaterial(ctx, material.String())
}

// pinAssistant keeps the assistant of an unfinished request and asks Jev only when
// a new human message opens the request. A pinned model that is excluded until its
// error expires cannot serve the turn, so the pin is released and another model is
// chosen; the failed model keeps its exclusion.
func (t *turn) pinAssistant(ctx context.Context, candidates []router.Candidate) (router.Candidate, bool, error) {
	if !t.newRequest && t.dialogue.AssistantModelID != "" {
		pinned, ok := findCandidate(candidates, t.dialogue.AssistantModelID)
		if ok {
			return pinned, true, nil
		}
		t.releasePin(ctx, "assistant", t.dialogue.AssistantModelID)
		t.dialogue.AssistantModelID = ""
	}
	chosen, fromJev, err := t.decideAssistant(ctx, candidates)
	if err != nil {
		return router.Candidate{}, false, err
	}
	if !fromJev {
		_ = t.engine.store.Journal(ctx, store.JournalEvent{DialogueID: t.dialogue.ID, Level: "warn",
			Kind: "jev_fallback_applied", Role: "assistant", RequestID: t.dialogue.RequestID,
			Note: "assistant decision fell back to defaults"})
	}
	return chosen, false, nil
}

func findCandidate(candidates []router.Candidate, id string) (router.Candidate, bool) {
	for _, item := range candidates {
		if item.Model.ID == id {
			return item, true
		}
	}
	return router.Candidate{}, false
}

func (t *turn) runExpert(ctx context.Context) (Result, error) {
	return t.runExpertWithMaterial(ctx, "")
}

func (t *turn) runExpertWithMaterial(ctx context.Context, material string) (Result, error) {
	candidates, err := t.expertCandidates(ctx)
	if err != nil {
		return Result{}, err
	}
	if len(candidates) == 0 {
		t.writeStopPhase(ctx, "adjuster_1", "no_available_model")
		t.journalEmptyGroup("coffee-expert")
		return Result{}, fail("no_available_model", http.StatusServiceUnavailable, "The coffee-expert group is empty", nil)
	}
	chosen, kept, err := t.pinExpert(ctx, candidates)
	if err != nil {
		return Result{}, err
	}
	expertReason := "selected"
	if kept {
		expertReason = "kept"
	}
	t.progress(Progress{Kind: "model", Stage: t.dialogue.Phase, Category: "model",
		Role: "expert", ModelID: chosen.Model.ID, Reason: expertReason})
	if !kept && t.dialogue.Phase != "adjuster_1_to_expert" && t.dialogue.Phase != "adjuster_2_to_expert" {
		if err := t.phase(ctx, "adjuster_1_to_expert"); err != nil {
			return Result{}, err
		}
	}
	if t.dialogue.ExpertModelID != chosen.Model.ID {
		if err := t.engine.store.PinExpert(ctx, t.dialogue.ID, "expert", chosen.Model.ID, pinReason(kept)); err != nil {
			return Result{}, fail("internal", http.StatusInternalServerError, "Pin event is unavailable", err)
		}
	}
	t.dialogue.Level = 2
	t.dialogue.Owner = "expert"
	t.dialogue.ExpertModelID = chosen.Model.ID
	if err := t.engine.store.UpdateDialogue(ctx, t.dialogue); err != nil {
		return Result{}, fail("internal", http.StatusInternalServerError, "Dialogue is unavailable", err)
	}
	if material != "" {
		if err := t.appendInternal(ctx, store.Message{Role: "user", Kind: "delegation_result",
			Content: "Assistant material:\n" + compactText(material, t.engine.opts.CompactPacketChars)}); err != nil {
			return Result{}, fail("internal", http.StatusInternalServerError, "History is unavailable", err)
		}
	}
	return t.expertLoop(ctx, chosen.Model)
}

// pinExpert keeps the expert of an unfinished request. A new human message opens
// the request again, so Jev may keep or change the expert. A pinned model that is
// excluded until its error expires is released so the turn continues on another
// model; the failed model keeps its exclusion.
func (t *turn) pinExpert(ctx context.Context, candidates []router.Candidate) (router.Candidate, bool, error) {
	if !t.newRequest && t.dialogue.Owner == "expert" && t.dialogue.ExpertModelID != "" {
		pinned, ok := findCandidate(candidates, t.dialogue.ExpertModelID)
		if ok {
			return pinned, true, nil
		}
		t.releasePin(ctx, "expert", t.dialogue.ExpertModelID)
		t.dialogue.ExpertModelID = ""
	}
	return t.decideExpert(ctx, candidates)
}

// releasePin records that a pin could not be honoured. The excluded model is not
// returned to the rotation early: its cooldown or disable window keeps running.
func (t *turn) releasePin(ctx context.Context, role, modelID string) {
	if err := t.engine.store.Journal(ctx, store.JournalEvent{
		DialogueID: t.dialogue.ID, Level: "warn", Kind: "pin_changed", Role: role,
		RequestID: t.dialogue.RequestID,
		Note:      fmt.Sprintf("pinned model %s is excluded until its error expires; another model is selected", modelID),
	}); err != nil {
		t.engine.log.Warn("write the pin change journal", "error", err, "dialogue_id", t.dialogue.ID)
	}
	t.progress(Progress{Kind: "model", Stage: t.dialogue.Phase, Category: "model",
		Role: role, ModelID: modelID, Reason: "replaced"})
}

func (t *turn) expertLoop(ctx context.Context, model catalog.Model) (Result, error) {
	for step := int64(0); step < t.engine.opts.MaxGatewayTurns; step++ {
		if t.modelCall > 0 {
			if err := t.sink.ping(); err != nil {
				return Result{}, fail("internal", http.StatusInternalServerError, "Client stream is unavailable", err)
			}
		}
		tools := t.expertTools()
		messages, err := t.historyMessages(ctx, model, expertPrompt(t.promptOptions()), tools)
		if err != nil {
			return Result{}, err
		}
		outcome, err := t.callActor(ctx, actorExpert, model, messages, tools, true)
		if err != nil {
			return Result{}, err
		}
		if outcome.Delegation != nil {
			result, err := t.runDelegation(ctx, outcome.Delegation)
			if err != nil {
				return Result{}, err
			}
			if result.pending {
				return result.result, nil
			}
			if err := t.appendDelegationResult(ctx, delegationMaterial(result.assistantID, result.scope, result.budgetSteps, result.text)); err != nil {
				return Result{}, fail("internal", http.StatusInternalServerError, "History is unavailable", err)
			}
			continue
		}
		if len(outcome.ToolCalls) > 0 {
			if err := t.phase(ctx, "expert_to_client_tools"); err != nil {
				return Result{}, err
			}
			return t.returnToolCalls(ctx, outcome)
		}
		if strings.TrimSpace(outcome.Text) == "" {
			t.writeStop(ctx, actorExpert, "empty_answer")
			return Result{}, fail("empty_answer", http.StatusBadGateway, "The model returned an empty answer", nil)
		}
		if err := t.appendAssistantText(ctx, outcome.Text, t.reasoningGroupID()); err != nil {
			return Result{}, fail("internal", http.StatusInternalServerError, "History is unavailable", err)
		}
		// The expert hit its output limit — send the answer as-is rather than
		// stopping with a pause phrase.  The user gets the full expert response
		// even if it was truncated by the upstream model (ADR 0017 fallback).
		if outcome.FinishReason == "length" {
			t.engine.log.Warn("expert hit output limit, sending the answer as-is",
				"dialogue_id", t.dialogue.ID, "request_id", t.input.RequestID,
				"text_chars", len([]rune(outcome.Text)))
			return t.finalize(ctx, outcome.Text, outcome.ReasoningContent, outcome.FinishReason, "expert_to_user")
		}
		return t.finalize(ctx, outcome.Text, outcome.ReasoningContent, outcome.FinishReason, "expert_to_user")
	}
	return t.finalize(ctx, t.engine.opts.PausePhrase, "", "length", "")
}

// expertTools builds the expert tool catalog. With expert_delegation_only the
// client catalog is replaced by the question tool alone: every read, search,
// command, edit and write goes through gateway_assistant (ADR 0017).
func (t *turn) expertTools() []transport.Tool {
	if !t.engine.opts.ExpertDelegationOnly {
		return append(append([]transport.Tool{}, t.input.Tools...),
			virtualTools(adapter.ToolGatewayAssistant)...)
	}
	tools := make([]transport.Tool, 0, 2)
	for _, tool := range t.input.Tools {
		if tool.Function.Name == adapter.QuestionTool {
			tools = append(tools, tool)
		}
	}
	return append(tools, virtualTools(adapter.ToolGatewayAssistant)...)
}

func (t *turn) runExpertOnly(ctx context.Context) (Result, error) {
	candidates, err := t.expertCandidates(ctx)
	if err != nil {
		return Result{}, err
	}
	if len(candidates) == 0 {
		t.writeStop(ctx, actorExpert, "no_available_model")
		t.journalEmptyGroup("coffee-expert")
		return Result{}, fail("no_available_model", http.StatusServiceUnavailable, "The coffee-expert group is empty", nil)
	}
	chosen := candidates[0]
	t.dialogue.Level = 2
	t.dialogue.Owner = "expert"
	t.dialogue.ExpertModelID = chosen.Model.ID
	if err := t.engine.store.UpdateDialogue(ctx, t.dialogue); err != nil {
		return Result{}, fail("internal", http.StatusInternalServerError, "Dialogue is unavailable", err)
	}
	tools := t.expertTools()
	messages, err := t.historyMessages(ctx, chosen.Model, expertPrompt(t.promptOptions()), tools)
	if err != nil {
		return Result{}, err
	}
	outcome, err := t.callActor(ctx, actorExpert, chosen.Model, messages, tools, false)
	if err != nil {
		return Result{}, err
	}
	if len(outcome.ToolCalls) > 0 {
		if err := t.phase(ctx, "expert_to_client_tools"); err != nil {
			return Result{}, err
		}
		return t.returnToolCalls(ctx, outcome)
	}
	if strings.TrimSpace(outcome.Text) == "" {
		t.writeStop(ctx, actorExpert, "empty_answer")
		return Result{}, fail("empty_answer", http.StatusBadGateway, "The model returned an empty answer", nil)
	}
	if err := t.appendAssistantText(ctx, outcome.Text, t.reasoningGroupID()); err != nil {
		return Result{}, fail("internal", http.StatusInternalServerError, "History is unavailable", err)
	}
	return t.finalize(ctx, outcome.Text, outcome.ReasoningContent, outcome.FinishReason, "expert_to_user")
}

// phase records the transition the dialogue leaves its current step by. The
// stored value is the edge itself; its receiver is the state the dialogue is in.
func (t *turn) phase(ctx context.Context, phase string) error {
	if err := t.engine.store.SetDialoguePhase(ctx, t.dialogue.ID, phase); err != nil {
		return fail("internal", http.StatusInternalServerError, "Dialogue is unavailable", err)
	}
	t.dialogue.Phase = phase
	t.progress(progressForPhase(phase))
	return nil
}

// progress emits one transient event. A write failure is not propagated here:
// the stream sink keeps the error and the next chunk write ends the turn, so a
// broken progress channel cannot invent a new stop status.
func (t *turn) progress(event Progress) {
	if event.RequestID == "" {
		event.RequestID = t.dialogue.RequestID
	}
	if event.Level == 0 {
		event.Level = t.dialogue.Level
	}
	if err := t.sink.progress(event); err != nil {
		t.engine.log.Warn("progress event is not written",
			"request_id", event.RequestID, "stage", event.Stage,
			"category", event.Category, "error", err)
	}
}

func (t *turn) reportReasoningSanitization() {
	if !t.reasoningSanitized || t.reasoningWarningSent {
		return
	}
	t.reasoningWarningSent = true
	t.progress(Progress{Kind: "warning", Stage: "turn", Category: "error",
		Role: "assistant", Reason: "reasoning_sanitized"})
}

func progressForPhase(phase string) Progress {
	event := Progress{Kind: "transition", Stage: phase, Category: "stage"}
	switch {
	case strings.HasSuffix(phase, "_to_client_tools"):
		event.Kind = "wait"
		event.Category = "tools"
	case strings.HasPrefix(phase, "client_tools_to_"):
		event.Category = "tools"
	case phase == "expert_to_assistant_2", strings.HasPrefix(phase, "assistant_2_to_"):
		event.Category = "delegation"
	}
	return event
}

// stopPrefix maps an actor to the stage name used by stop statuses.
func stopPrefix(kind actorKind) string {
	switch kind {
	case actorAssistant:
		return "assistant_1"
	case actorExpert:
		return "expert"
	case actorHelper:
		return "assistant_2"
	default:
		return ""
	}
}

// writeStop records a request stop. The phase is written even when the client
// is gone: the request must not stay active forever.
func (t *turn) writeStop(ctx context.Context, kind actorKind, reason string) {
	t.writeStopPhase(ctx, stopPrefix(kind), reason)
}

func (t *turn) writeStopPhase(ctx context.Context, prefix, reason string) {
	if prefix == "" {
		return
	}
	if prefix == "assistant_2" && reason != "canceled" {
		return
	}
	phase := prefix + "_" + reason + "_stop"
	if err := t.engine.store.SetDialoguePhase(context.WithoutCancel(ctx), t.dialogue.ID, phase); err != nil {
		t.engine.log.Error("write stop status", "error", err, "phase", phase)
	}
	t.progress(Progress{Kind: "stop", Stage: phase, Category: "error", Reason: reason})
}

// mergeUsage accumulates the reported usage of one actor under its role. The
// deflator bypasses callActor, so it merges its usage separately in compress.
func (t *turn) mergeUsage(role string, usage *transport.Usage) {
	if usage == nil {
		return
	}
	if t.usageByRole == nil {
		t.usageByRole = make(map[string]*transport.Usage)
	}
	item, ok := t.usageByRole[role]
	if !ok {
		item = &transport.Usage{}
		t.usageByRole[role] = item
	}
	item.Merge(usage)
}

// usageRows renders the per-role report in a fixed order. Roles without a call
// keep only the role name: absent provider fields are omitted, not zeroed.
func (t *turn) usageRows() []transport.UsageRow {
	rows := make([]transport.UsageRow, 0, len(usageRoles))
	for _, role := range usageRoles {
		row := transport.UsageRow{Role: role}
		if usage := t.usageByRole[role]; usage != nil {
			row.InputTokens = usage.PromptTokens
			row.OutputTokens = usage.CompletionTokens
			row.ReasoningTokens = usage.Reasoning()
			if usage.PromptTokensDetails != nil {
				row.CachedTokens = usage.PromptTokensDetails.CachedTokens
			}
		}
		rows = append(rows, row)
	}
	return rows
}

// finalize delivers the answer of the actor named by delivered. The status is
// written by the HTTP layer after the delivery row is stored; a client that
// disconnects during that write turns the turn into a canceled stop.
func (t *turn) finalize(ctx context.Context, text, reasoning, finish, delivered string) (Result, error) {
	if finish == "" {
		finish = "stop"
	}
	if t.engine.opts.StreamTextScope != "all" && strings.TrimSpace(text) != "" {
		if err := t.sink.text(text); err != nil {
			return Result{}, err
		}
	}
	if finish == "length" {
		if err := t.appendInternal(ctx, store.Message{Role: "assistant", Kind: "service",
			Content: t.engine.opts.PausePhrase}); err != nil {
			return Result{}, fail("internal", http.StatusInternalServerError, "History is unavailable", err)
		}
		if err := t.sink.text(t.engine.opts.PausePhrase); err != nil {
			return Result{}, err
		}
		text += t.engine.opts.PausePhrase
	}
	t.reportReasoningSanitization()
	t.progress(Progress{Kind: "summary", Stage: "turn", Category: "tools"})
	participants := t.actors.list()
	if err := t.sink.participants(participants); err != nil {
		return Result{}, err
	}
	if err := t.sink.usageReport(t.usageRows()); err != nil {
		return Result{}, err
	}
	if err := t.sink.finish(finish); err != nil {
		return Result{}, err
	}
	return Result{Content: text, ReasoningContent: reasoning, FinishReason: finish, Usage: &t.usage,
		UsageReport:  t.usageRows(),
		Participants: participants, Level: t.dialogue.Level, Model: t.dialogue.ExpertModelID, DeliverPhase: delivered}, nil
}

func (t *turn) returnToolCalls(ctx context.Context, outcome *actorOutcome) (Result, error) {
	if err := t.appendToolCalls(ctx, outcome.ToolCalls, t.reasoningGroupID()); err != nil {
		return Result{}, fail("internal", http.StatusInternalServerError, "History is unavailable", err)
	}
	for index, call := range outcome.ToolCalls {
		function := call.Function
		if err := t.sink.toolCall(transport.ToolCallDelta{
			Index: index, ID: call.ID, Type: call.Type, Function: &function,
		}); err != nil {
			return Result{}, err
		}
	}
	t.reportReasoningSanitization()
	t.progress(Progress{Kind: "summary", Stage: "turn", Category: "tools"})
	participants := t.actors.list()
	if err := t.sink.participants(participants); err != nil {
		return Result{}, err
	}
	if err := t.sink.usageReport(t.usageRows()); err != nil {
		return Result{}, err
	}
	if err := t.sink.finish("tool_calls"); err != nil {
		return Result{}, err
	}
	return Result{ReasoningContent: outcome.ReasoningContent,
		ToolCalls: outcome.ToolCalls, FinishReason: "tool_calls",
		Usage: &t.usage, UsageReport: t.usageRows(), Participants: participants,
		Level: t.dialogue.Level, Model: t.dialogue.ExpertModelID}, nil
}

func (t *turn) assistantCandidates(ctx context.Context, needTools bool) ([]router.Candidate, error) {
	inputTokens := t.estimateFixedInputTokens()
	modalities := t.requestModalities()
	return t.engine.router.Candidates(ctx, router.Filter{
		Role: "assistant", NeedTools: needTools, InputToken: inputTokens, Modalities: modalities,
		FreeOnly: t.settings.SubscriptionMode == "free",
	})
}

func (t *turn) expertCandidates(ctx context.Context) ([]router.Candidate, error) {
	inputTokens := t.estimateFixedInputTokens()
	modalities := t.requestModalities()
	return t.engine.router.Candidates(ctx, router.Filter{
		Role: "expert", NeedTools: true, InputToken: inputTokens, Modalities: modalities,
		FreeOnly: t.settings.SubscriptionMode == "free",
	})
}

func (t *turn) decideAssistant(ctx context.Context, candidates []router.Candidate) (router.Candidate, bool, error) {
	state := t.jevState(ctx)
	return t.engine.router.DecideAssistant(ctx, state, candidates)
}

func (t *turn) decideExpert(ctx context.Context, candidates []router.Candidate) (router.Candidate, bool, error) {
	state := t.jevState(ctx)
	current := t.dialogue.ExpertModelID
	return t.engine.router.DecideExpert(ctx, state, candidates, current)
}

func markMissingParts(state map[string]any) {
	state["context_missing"] = state["context"] == nil
	state["request_missing"] = state["request"] == nil
	state["answer_missing"] = state["answer"] == nil
}

func (t *turn) checkAnswer(ctx context.Context, text string, suspect bool, suspectReason string) (bool, string, error) {
	state := t.jevState(ctx)
	state["latest_assistant_answer"] = text
	state["request"] = t.latestHuman()
	state["answer"] = text
	if suspect {
		state["answer_suspect"] = true
		state["suspect_reason"] = suspectReason
	}
	markMissingParts(state)
	// The largest packet first: request and answer, then the answer alone. If
	// even the answer does not fit, Jev cannot judge and the answer is accepted.
	fit, ok := t.engine.router.FitState(state, "context", "request")
	if !ok {
		_ = t.engine.store.Journal(ctx, store.JournalEvent{DialogueID: t.dialogue.ID, Level: "warn",
			Kind: "jev_failed", Role: "assistant", RequestID: t.dialogue.RequestID,
			Note: "the answer does not fit the Jev state, accepted without validation"})
		t.progress(Progress{Kind: "decision", Stage: "assistant_1_to_adjuster_2",
			Category: "decision", Reason: "accepted", Role: "assistant"})
		return true, "", nil
	}
	markMissingParts(fit)
	accepted, reason, err := t.engine.router.CheckAnswer(ctx, fit)
	if err == nil {
		decision := "rejected"
		if accepted {
			decision = "accepted"
		}
		t.progress(Progress{Kind: "decision", Stage: "assistant_1_to_adjuster_2",
			Category: "decision", Reason: decision, Role: "assistant"})
	}
	return accepted, reason, err
}

func (t *turn) jevState(ctx context.Context) map[string]any {
	expert, assistant, err := t.engine.store.GroupUsage(ctx, t.dialogue.ID, t.engine.opts.BalanceTurns)
	if err != nil {
		expert, assistant = 0, 0
	}
	state := map[string]any{
		"latest_human_message": t.latestHuman(),
		"current_expert":       t.dialogue.ExpertModelID,
		"level":                t.dialogue.Level,
		"iteration":            t.modelCall,
		"objective_mode":       t.settings.ObjectiveMode,
		"reasoning_mode":       t.settings.ReasoningMode,
		"group_usage":          map[string]int64{"coffee-expert": expert, "coffee-assistant": assistant},
		"catalog":              t.criteriaCatalog(ctx),
		"request":              t.latestHuman(),
		"context":              t.jevContext(),
		"truncated":            false,
	}
	return state
}

func (t *turn) criteriaCatalog(ctx context.Context) string {
	candidates, err := t.assistantCandidates(ctx, false)
	if err != nil {
		return ""
	}
	criteria := t.engine.router.Criteria(candidates, t.settings.ObjectiveMode, nil)
	var b strings.Builder
	for _, item := range candidates {
		if text, ok := criteria[item.Model.ID]; ok {
			b.WriteString(text)
			b.WriteString("\n")
		}
	}
	return b.String()
}

// jevContext is a short tail of human messages for a Jev decision.
func (t *turn) jevContext() string {
	var parts []string
	for i := len(t.input.Messages) - 1; i >= 0 && len(parts) < 4; i-- {
		message := t.input.Messages[i]
		if message.Role != "user" {
			continue
		}
		text, err := message.Text()
		if err != nil || text == "" {
			continue
		}
		parts = append(parts, "user: "+compactTail(text, t.engine.opts.CompactEvidenceChars))
	}
	for left, right := 0, len(parts)-1; left < right; left, right = left+1, right-1 {
		parts[left], parts[right] = parts[right], parts[left]
	}
	return compactTail(strings.Join(parts, "\n"), t.engine.opts.CompactPacketChars)
}

func (t *turn) latestHuman() string {
	return compactTail(t.latestHumanFull(), t.engine.opts.CompactPacketChars)
}

func (t *turn) estimateInputTokens() int64 {
	var chars int64
	for _, message := range t.input.Messages {
		if message.Role != "assistant" {
			text, err := message.Text()
			if err == nil {
				chars += int64(len([]rune(text)))
			}
		}
		for _, call := range message.ToolCalls {
			chars += int64(len([]rune(call.Function.Name)) + len([]rune(call.Function.Arguments)))
		}
	}
	for _, tool := range t.input.Tools {
		chars += int64(len([]rune(tool.Function.Name)) + len([]rune(tool.Function.Description)) +
			len(tool.Function.Parameters))
	}
	if t.engine.opts.CharsPerToken <= 0 {
		return chars
	}
	return chars / t.engine.opts.CharsPerToken
}

func (t *turn) estimateFixedInputTokens() int64 {
	var chars int64
	for _, message := range t.instructions {
		chars += int64(messageChars(message))
	}
	for _, tool := range t.input.Tools {
		chars += int64(len([]rune(tool.Function.Name)) + len([]rune(tool.Function.Description)) +
			len(tool.Function.Parameters))
	}
	if t.engine.opts.CharsPerToken <= 0 {
		return chars
	}
	return chars / t.engine.opts.CharsPerToken
}

func (t *turn) requestModalities() map[string]bool {
	out := map[string]bool{"text": true}
	for _, message := range t.input.Messages {
		if message.Role == "assistant" || len(message.Content) == 0 {
			continue
		}
		var parts []struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(message.Content, &parts); err != nil {
			continue
		}
		for _, part := range parts {
			switch part.Type {
			case "image_url":
				out["image"] = true
			case "input_audio":
				out["audio"] = true
			}
		}
	}
	return out
}

func instructionsOf(messages []transport.Message) []transport.Message {
	out := make([]transport.Message, 0, 2)
	for _, message := range messages {
		if message.Role == "system" || message.Role == "developer" {
			out = append(out, message)
		}
	}
	return out
}

func isServiceReminder(message transport.Message) bool {
	text, err := message.Text()
	if err != nil {
		return false
	}
	for _, prefix := range []string{"<system_reminder>", "<system_notification>", "<user_info>"} {
		if strings.HasPrefix(strings.TrimSpace(text), prefix) {
			return true
		}
	}
	return false
}

func (t *turn) promptOptions() PromptOptions {
	return PromptOptions{
		CompactEvidenceChars: t.engine.opts.CompactEvidenceChars,
		MaxDelegationSteps:   t.engine.opts.MaxDelegationSteps,
		ExpertDelegationOnly: t.engine.opts.ExpertDelegationOnly,
	}
}

func compactText(text string, limit int64) string {
	runes := []rune(text)
	if int64(len(runes)) <= limit {
		return text
	}
	return string(runes[:limit])
}

func compactTail(text string, limit int64) string {
	if limit <= 0 {
		return text
	}
	runes := []rune(text)
	if int64(len(runes)) <= limit {
		return text
	}
	return string(runes[int64(len(runes))-limit:])
}

func pinReason(kept bool) string {
	if kept {
		return "kept by jev"
	}
	return "selected by jev"
}

type participants struct {
	order []Participant
	seen  map[string]bool
}

func newParticipants() *participants {
	return &participants{seen: make(map[string]bool)}
}

func (p *participants) add(id, role string) {
	if id == "" || p.seen[id] {
		return
	}
	p.seen[id] = true
	p.order = append(p.order, Participant{ID: id, Role: role})
}

func (p *participants) list() []Participant {
	return append([]Participant(nil), p.order...)
}
