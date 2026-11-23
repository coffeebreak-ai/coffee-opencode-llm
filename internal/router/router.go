package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/adapter"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/jev"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/ledger"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
)

type Options struct {
	ObjectiveMode           string
	ReasoningMode           string
	ReasoningContextEnabled bool
	Assistant2Reasoning     bool
	SubscriptionMode        string
	BalanceTurns            int64
	CharsPerToken           int64
	CriteriaChars           int64
	StateChars              int64
	MaxRequestBytes         int64
	DeflateMinConfidencePct int64
	MaxQuestions            int64
	MaxCallsPerTurn         int64
	MinStartConfidence      float64
	DefaultExpert           string
	DefaultAssistant        string
	DefaultScope            string
}

type Router struct {
	catalog *catalog.Catalog
	store   *store.Store
	ledger  *ledger.Ledger
	jev     *jev.Client
	opts    Options
}

// Budget caps the number of solver calls inside one turn.
type Budget struct {
	mu    sync.Mutex
	limit int64
	used  int64
}

func NewBudget(limit int64) *Budget {
	return &Budget{limit: limit}
}

func (b *Budget) Take() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used >= b.limit {
		return false
	}
	b.used++
	return true
}

type budgetKey struct{}

func WithBudget(ctx context.Context, budget *Budget) context.Context {
	return context.WithValue(ctx, budgetKey{}, budget)
}

func budgetFrom(ctx context.Context) *Budget {
	value, _ := ctx.Value(budgetKey{}).(*Budget)
	return value
}

var errBudgetExhausted = errors.New("router: solver call budget is exhausted")

func New(cat *catalog.Catalog, st *store.Store, led *ledger.Ledger, client *jev.Client, opts Options) *Router {
	return &Router{catalog: cat, store: st, ledger: led, jev: client, opts: opts}
}

func (r *Router) ask(ctx context.Context, state map[string]any, questions map[string]jev.Question) (map[string]jev.Answer, error) {
	if r.jev == nil {
		return nil, errors.New("router: solver is not configured")
	}
	if int64(len(questions)) > r.opts.MaxQuestions {
		return nil, fmt.Errorf("router: %d questions exceed the limit of %d", len(questions), r.opts.MaxQuestions)
	}
	if budget := budgetFrom(ctx); budget != nil && !budget.Take() {
		return nil, errBudgetExhausted
	}
	state = r.stateForJev(state)
	size, err := r.jev.RequestSizeBytes(state, questions)
	if err != nil {
		return nil, fmt.Errorf("router: measure Jev request: %w", err)
	}
	if int64(size) > r.opts.MaxRequestBytes {
		return nil, errors.New("router: Jev request exceeds the configured input budget")
	}
	return r.jev.Ask(ctx, state, questions)
}

type Filter struct {
	Role        string
	NeedTools   bool
	InputToken  int64
	OutputToken int64
	Modalities  map[string]bool
	FreeOnly    bool
}

type Candidate struct {
	Model     catalog.Model
	Remaining int64
	Unlimited bool
	Limit     *int64
	Window    string
}

type Settings struct {
	ObjectiveMode           string
	ReasoningMode           string
	ReasoningContextEnabled bool
	Assistant2Reasoning     bool
	SubscriptionMode        string
	Source                  string
}

func (r *Router) Settings(ctx context.Context) (Settings, error) {
	out := Settings{
		ObjectiveMode:           r.opts.ObjectiveMode,
		ReasoningMode:           r.opts.ReasoningMode,
		ReasoningContextEnabled: r.opts.ReasoningContextEnabled,
		Assistant2Reasoning:     r.opts.Assistant2Reasoning,
		SubscriptionMode:        r.opts.SubscriptionMode,
		Source:                  "config",
	}
	for key, target := range map[string]*string{
		"objective_mode":    &out.ObjectiveMode,
		"reasoning_mode":    &out.ReasoningMode,
		"subscription_mode": &out.SubscriptionMode,
	} {
		item, err := r.store.Setting(ctx, key)
		if err == nil {
			*target = item.Value
			out.Source = item.SourceKind
			continue
		}
		if !errors.Is(err, store.ErrNotFound) {
			return Settings{}, fmt.Errorf("read setting %s: %w", key, err)
		}
	}
	if item, err := r.store.Setting(ctx, "reasoning_context_enabled"); err == nil {
		switch item.Value {
		case "off":
			out.ReasoningContextEnabled = false
		case "on":
			out.ReasoningContextEnabled = true
		default:
			return Settings{}, fmt.Errorf("reasoning_context_enabled has invalid value %q", item.Value)
		}
		out.Source = item.SourceKind
	} else if !errors.Is(err, store.ErrNotFound) {
		return Settings{}, fmt.Errorf("read setting reasoning_context_enabled: %w", err)
	}
	if item, err := r.store.Setting(ctx, "assistant2_reasoning_enabled"); err == nil {
		switch item.Value {
		case "off":
			out.Assistant2Reasoning = false
		case "on":
			out.Assistant2Reasoning = true
		default:
			return Settings{}, fmt.Errorf("assistant2_reasoning_enabled has invalid value %q", item.Value)
		}
		out.Source = item.SourceKind
	} else if !errors.Is(err, store.ErrNotFound) {
		return Settings{}, fmt.Errorf("read setting assistant2_reasoning_enabled: %w", err)
	}
	return out, nil
}

func (r *Router) Candidates(ctx context.Context, filter Filter) ([]Candidate, error) {
	now := time.Now().UTC().UnixMilli()
	availability, err := r.store.Availability(ctx)
	if err != nil {
		return nil, err
	}
	members, err := r.store.GroupMembers(ctx, groupForRole(filter.Role))
	if err != nil {
		return nil, err
	}
	inGroup := make(map[string]bool)
	for _, id := range members {
		inGroup[id] = true
	}
	var out []Candidate
	for _, model := range r.catalog.ByRole(filter.Role) {
		if !inGroup[model.ID] {
			continue
		}
		state, ok := availability[model.ID]
		if !ok || !state.Enabled {
			continue
		}
		if state.DisabledUntilMS != nil && *state.DisabledUntilMS > now {
			continue
		}
		if state.ManualExcluded {
			continue
		}
		if state.ExcludedUntilMS != nil && *state.ExcludedUntilMS > now {
			continue
		}
		if filter.NeedTools && !has(model.Features, "tools") {
			continue
		}
		outputReservation := int64(model.MaxOutput)
		if filter.OutputToken > 0 && filter.OutputToken < outputReservation {
			outputReservation = filter.OutputToken
		}
		if filter.InputToken+outputReservation > int64(model.ContextTokens) {
			continue
		}
		if !coversModalities(model.Modalities, filter.Modalities) {
			continue
		}
		if filter.FreeOnly && !isFree(model) {
			continue
		}
		cooling, err := r.ledger.CoolingUntil(ctx, model.ID, model.Quota.Group)
		if err != nil {
			return nil, err
		}
		if cooling > now {
			continue
		}
		remaining, unlimited, err := r.ledger.Remaining(ctx, model.ID, model.Quota)
		if err != nil {
			return nil, err
		}
		if !unlimited && remaining <= 0 {
			continue
		}
		out = append(out, Candidate{Model: model, Remaining: remaining, Unlimited: unlimited,
			Limit: windowLimit(model.Quota), Window: windowName(model.Quota)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Unlimited != out[j].Unlimited {
			return out[i].Unlimited
		}
		if out[i].Remaining != out[j].Remaining {
			return out[i].Remaining > out[j].Remaining
		}
		return out[i].Model.ID < out[j].Model.ID
	})
	return out, nil
}

func (r *Router) Default(candidates []Candidate, id string) (Candidate, bool) {
	for _, item := range candidates {
		if item.Model.ID == id {
			return item, true
		}
	}
	if len(candidates) == 0 {
		return Candidate{}, false
	}
	return candidates[0], true
}

func defaultChoice(item Candidate, ok bool) (Candidate, bool, error) {
	return item, ok, nil
}

func (r *Router) Criteria(candidates []Candidate, mode string, groupUsage map[string]int64) map[string]string {
	out := make(map[string]string, len(candidates))
	for _, item := range candidates {
		model := item.Model
		var b strings.Builder
		b.WriteString(model.ID)
		b.WriteString(" — ")
		if has(model.Roles, "expert") {
			b.WriteString("expert")
		} else {
			b.WriteString("assistant")
		}
		fmt.Fprintf(&b, ". Context %d tokens, output up to %d.", model.ContextTokens, model.MaxOutput)
		fmt.Fprintf(&b, " Strengths: %s. Limits: %s.", model.Strengths, model.Limits)
		switch mode {
		case "quality":
			fmt.Fprintf(&b, " Quality tier: %s.", model.QualityTier)
		case "price":
			b.WriteString(" " + priceText(model))
			b.WriteString(" " + remainingText(item))
		default:
			fmt.Fprintf(&b, " Quality tier: %s.", model.QualityTier)
			b.WriteString(" " + priceText(model))
			b.WriteString(" " + remainingText(item))
			fmt.Fprintf(&b, " Group usage: expert %d, assistant %d.", groupUsage["coffee-expert"], groupUsage["coffee-assistant"])
		}
		out[model.ID] = b.String()
	}
	return r.trimCriteria(out, candidates)
}

func (r *Router) trimCriteria(criteria map[string]string, candidates []Candidate) map[string]string {
	total := 0
	for _, text := range criteria {
		total += len([]rune(text))
	}
	if int64(total) <= r.opts.CriteriaChars {
		return criteria
	}
	order := append([]Candidate(nil), candidates...)
	sort.Slice(order, func(i, j int) bool { return order[i].Remaining < order[j].Remaining })
	for _, item := range order {
		if int64(total) <= r.opts.CriteriaChars {
			break
		}
		text, ok := criteria[item.Model.ID]
		if !ok {
			continue
		}
		total -= len([]rune(text))
		delete(criteria, item.Model.ID)
	}
	return criteria
}

func (r *Router) Effort(model catalog.Model, mode, clientEffort string) (string, bool) {
	return adapter.NearestEffort(model.Effort, mode, clientEffort)
}

func (r *Router) EffortOrEmpty(model catalog.Model, mode, clientEffort string) string {
	value, _ := r.Effort(model, mode, clientEffort)
	return value
}

func AllowedEfforts(values []string, mode string) []string {
	return adapter.AllowedEfforts(values, mode)
}

type StartDecision struct {
	Assistant Candidate
	Expert    *Candidate
	Keep      bool
	Reason    string
}

// FitState drops optional payload keys, in the given order, until the state fits
// the Jev limit. It reports false when even the remaining keys do not fit.
func (r *Router) FitState(state map[string]any, optional ...string) (map[string]any, bool) {
	out := make(map[string]any, len(state))
	for key, value := range state {
		out[key] = value
	}
	if r.fits(out) {
		return out, true
	}
	for _, key := range optional {
		delete(out, key)
		if r.fits(out) {
			return out, true
		}
	}
	return nil, false
}

func (r *Router) fits(state map[string]any) bool {
	raw, err := json.Marshal(state)
	if err != nil {
		return true
	}
	return int64(len(raw)) <= r.opts.StateChars
}

// DecideStart asks Jev who handles a new request: the assistant on level 1 or the
// expert on level 2. When Jev is unavailable the assistant is the default.
func (r *Router) DecideStart(ctx context.Context, state map[string]any) (string, error) {
	if r.jev == nil {
		return "assistant", nil
	}
	question := jev.Question{
		Type: "choice",
		Instructions: "Can the assistant on level 1 answer this request completely and reliably on its own? " +
			"Answer yes or no. A negative answer or low confidence means the expert handles it.",
		Criteria: map[string]string{
			"yes": "the assistant can produce the complete, correct answer without expert judgement",
			"no":  "the assistant cannot; the request needs the expert",
		},
	}
	questions := map[string]jev.Question{
		"start": question,
		"explicit_expert": {
			Type: "choice",
			Instructions: "Does the human explicitly ask for the expert, or say that the assistant's " +
				"answer is not enough? Answer yes or no from the request above.",
			Criteria: map[string]string{
				"yes": "the human asks for the expert or rejects the assistant",
				"no":  "the human did not ask for the expert",
			},
		},
	}
	answers, err := r.ask(ctx, state, questions)
	if err != nil {
		return "assistant", err
	}
	if answers["explicit_expert"].Choice == "yes" {
		return "expert", nil
	}
	start := answers["start"]
	if start.Choice != "yes" {
		return "expert", nil
	}
	if start.Confidence > 0 && start.Confidence < r.opts.MinStartConfidence {
		return "expert", nil
	}
	return "assistant", nil
}

func (r *Router) DecideAssistant(ctx context.Context, state map[string]any, candidates []Candidate) (Candidate, bool, error) {
	if len(candidates) == 0 {
		return Candidate{}, false, errors.New("router: no assistant candidates")
	}
	if r.jev == nil {
		return defaultChoice(r.Default(candidates, r.opts.DefaultAssistant))
	}
	question := jev.Question{
		Type:         "choice",
		Instructions: "Which assistant should own this request on level 1? Choose one option from criteria.",
		Criteria:     r.Criteria(candidates, stateString(state, "objective_mode"), groupUsageOf(state)),
	}
	answers, err := r.ask(ctx, state, map[string]jev.Question{"assistant": question})
	if err != nil {
		return defaultChoice(r.Default(candidates, r.opts.DefaultAssistant))
	}
	choice := answers["assistant"].Choice
	for _, item := range candidates {
		if item.Model.ID == choice {
			return item, true, nil
		}
	}
	return defaultChoice(r.Default(candidates, r.opts.DefaultAssistant))
}

func (r *Router) DecideExpert(ctx context.Context, state map[string]any, candidates []Candidate, current string) (Candidate, bool, error) {
	if len(candidates) == 0 {
		return Candidate{}, false, errors.New("router: no expert candidates")
	}
	if r.jev == nil {
		return defaultChoice(r.Default(candidates, r.opts.DefaultExpert))
	}
	questions := make(map[string]jev.Question)
	if current != "" {
		questions["keep_expert"] = jev.Question{
			Type:         "choice",
			Instructions: "Can the current expert continue this task without a quality loss?",
			Criteria:     map[string]string{"keep": "the current expert can continue", "change": "a different expert is required"},
		}
	}
	questions["expert"] = jev.Question{
		Type:         "choice",
		Instructions: "Which expert should produce the final answer? Choose one option from criteria.",
		Criteria:     r.Criteria(candidates, stateString(state, "objective_mode"), groupUsageOf(state)),
	}
	answers, err := r.ask(ctx, state, questions)
	if err != nil {
		return defaultChoice(r.Default(candidates, r.opts.DefaultExpert))
	}
	if current != "" {
		if answers["keep_expert"].Choice == "keep" {
			for _, item := range candidates {
				if item.Model.ID == current {
					return item, true, nil
				}
			}
		}
	}
	choice := answers["expert"].Choice
	for _, item := range candidates {
		if item.Model.ID == choice {
			return item, false, nil
		}
	}
	return defaultChoice(r.Default(candidates, r.opts.DefaultExpert))
}

// JevAvailable reports whether the adjuster can judge an assistant answer.
func (r *Router) JevAvailable() bool {
	return r.jev != nil
}

func (r *Router) CheckAnswer(ctx context.Context, state map[string]any) (bool, string, error) {
	if r.jev == nil {
		return true, "", nil
	}
	question := jev.Question{
		Type: "choice",
		Instructions: "Is the assistant answer complete and correct enough to return to the human? " +
			"An answer with answer_suspect is empty, was emptied by gateway sanitization, or both; " +
			"it is not a usable answer. The flags context_missing, request_missing and answer_missing " +
			"tell which of the three expected parts was omitted to fit the state limit: treat a " +
			"missing part as unknown, not as evidence that it was absent.",
		Criteria: map[string]string{
			"accept": "the answer is complete, non-empty and was not stripped by sanitization; return it to the human",
			"expert": "the answer is empty, fully stripped by sanitization, or otherwise incomplete; the material needs expert synthesis or review",
		},
	}
	answers, err := r.ask(ctx, state, map[string]jev.Question{"check_answer": question})
	if err != nil {
		return true, "", err
	}
	answer := answers["check_answer"]
	return answer.Choice == "accept", strings.TrimSpace(answer.Reason), nil
}

func (r *Router) CheckDeflate(ctx context.Context, state map[string]any) (bool, jev.Answer, error) {
	if r.jev == nil {
		return false, jev.Answer{}, errors.New("router: solver is not configured")
	}
	question := jev.Question{
		Type: "choice",
		Instructions: "Should this tool result be compressed before it reaches the expert? " +
			"Use only the supplied context, human request, tool call and result. " +
			"Choose yes only when compression is needed to reduce context without losing useful evidence.",
		Criteria: map[string]string{
			"yes": "compression is needed; the expert should receive a shorter version",
			"no":  "compression is unnecessary or may lose useful evidence; keep the original",
		},
	}
	questions := map[string]jev.Question{"deflate": question}
	fit, ok, err := r.fitStateTail(state, questions, "evidence")
	if err != nil {
		return false, jev.Answer{}, fmt.Errorf("router: fit deflation decision: %w", err)
	}
	if !ok {
		return false, jev.Answer{}, errors.New("router: deflation decision does not fit the Jev request budget")
	}
	answers, err := r.ask(ctx, fit, questions)
	if err != nil {
		return false, jev.Answer{}, err
	}
	answer, ok := answers["deflate"]
	if !ok {
		return false, jev.Answer{}, errors.New("router: Jev omitted the deflation decision")
	}
	allowed := answer.Choice == "yes" &&
		answer.Confidence > float64(r.opts.DeflateMinConfidencePct)/100
	return allowed, answer, nil
}

func (r *Router) Sufficient(ctx context.Context, state map[string]any) (bool, error) {
	if r.jev == nil {
		return false, nil
	}
	question := jev.Question{
		Type:         "choice",
		Instructions: "Is the collected material sufficient for the expert to produce the final answer?",
		Criteria: map[string]string{
			"sufficient": "enough data, stop the delegation",
			"continue":   "more data is required",
		},
	}
	answers, err := r.ask(ctx, state, map[string]jev.Question{"sufficient": question})
	if err != nil {
		return false, err
	}
	return answers["sufficient"].Choice == "sufficient", nil
}

func (r *Router) fitStateTail(
	state map[string]any, questions map[string]jev.Question, textKey string,
) (map[string]any, bool, error) {
	out := maps.Clone(state)
	fits := func(candidate map[string]any) (bool, error) {
		size, err := r.jev.RequestSizeBytes(r.stateForJev(candidate), questions)
		if err != nil {
			return false, err
		}
		return int64(size) <= r.opts.MaxRequestBytes, nil
	}
	if ok, err := fits(out); err != nil || ok {
		return out, ok, err
	}
	evidence, ok := out[textKey].(string)
	if !ok || evidence == "" || r.opts.MaxRequestBytes <= 0 {
		return nil, false, nil
	}
	runes := []rune(evidence)
	out["truncated"] = true
	low, high := 0, len(runes)
	for low < high {
		middle := low + (high-low+1)/2
		out[textKey] = string(runes[len(runes)-middle:])
		ok, err := fits(out)
		if err != nil {
			return nil, false, err
		}
		if ok {
			low = middle
		} else {
			high = middle - 1
		}
	}
	if low == 0 {
		return nil, false, nil
	}
	out[textKey] = string(runes[len(runes)-low:])
	ok, err := fits(out)
	if err != nil || !ok {
		return nil, false, err
	}
	return out, true, nil
}

func (r *Router) stateForJev(state map[string]any) map[string]any {
	out := make(map[string]any, len(state)+2)
	for key, value := range state {
		out[key] = value
	}
	out["objective_mode"] = stateString(state, "objective_mode")
	out["reasoning_mode"] = stateString(state, "reasoning_mode")
	if text, ok := out["catalog"].(string); ok && int64(len([]rune(text))) > r.opts.StateChars {
		out["catalog"] = truncate(text, r.opts.StateChars)
		out["truncated"] = true
	}
	return out
}

func truncate(text string, limit int64) string {
	runes := []rune(text)
	if int64(len(runes)) <= limit {
		return text
	}
	return string(runes[:limit])
}

func stateString(state map[string]any, key string) string {
	value, ok := state[key].(string)
	if !ok {
		return ""
	}
	return value
}

func groupUsageOf(state map[string]any) map[string]int64 {
	out := make(map[string]int64)
	if raw, ok := state["group_usage"].(map[string]int64); ok {
		return raw
	}
	if raw, ok := state["group_usage"].(map[string]any); ok {
		for key, value := range raw {
			switch typed := value.(type) {
			case int64:
				out[key] = typed
			case int:
				out[key] = int64(typed)
			case float64:
				out[key] = int64(typed)
			}
		}
	}
	return out
}

func groupForRole(role string) string {
	if role == "expert" {
		return "coffee-expert"
	}
	return "coffee-assistant"
}

func coversModalities(have []string, want map[string]bool) bool {
	for modality := range want {
		if !has(have, modality) {
			return false
		}
	}
	return true
}

func has(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func isFree(model catalog.Model) bool {
	return model.CostInPerMTok != nil && model.CostOutPerMTok != nil &&
		*model.CostInPerMTok == 0 && *model.CostOutPerMTok == 0
}

func windowLimit(quota catalog.Quota) *int64 {
	if quota.RPM != nil {
		value := int64(*quota.RPM)
		return &value
	}
	if quota.Day != nil {
		value := int64(*quota.Day)
		return &value
	}
	if quota.Month != nil {
		value := int64(*quota.Month)
		return &value
	}
	return nil
}

func windowName(quota catalog.Quota) string {
	if quota.RPM != nil {
		return "rpm"
	}
	if quota.Day != nil {
		return "day"
	}
	if quota.Month != nil {
		return "month"
	}
	return "unknown"
}

func priceText(model catalog.Model) string {
	if model.CostInPerMTok == nil || model.CostOutPerMTok == nil {
		return "Price is unknown."
	}
	return fmt.Sprintf("Input price %g, output price %g per million.", *model.CostInPerMTok, *model.CostOutPerMTok)
}

func remainingText(item Candidate) string {
	if item.Unlimited {
		return "Quota: limit is not published."
	}
	limit := int64(0)
	if item.Limit != nil {
		limit = *item.Limit
	}
	return fmt.Sprintf("Quota remaining: %d of %d per %s.", item.Remaining, limit, item.Window)
}
