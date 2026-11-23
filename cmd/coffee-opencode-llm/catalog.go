package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/ledger"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
)

type modelView struct {
	ID            string `json:"id"`
	Expert        bool   `json:"expert"`
	Assistant     bool   `json:"assistant"`
	Source        string `json:"source"`
	Enabled       bool   `json:"enabled"`
	QuotaGroup    string `json:"quota_group"`
	Remaining     string `json:"remaining"`
	Excluded      string `json:"excluded"`
	Reason        string `json:"reason"`
	AvailableAtMS *int64 `json:"available_at_ms"`
	AvailableAt   string `json:"available_at"`
}

var rotationGroups = map[string]bool{"coffee-expert": true, "coffee-assistant": true}

type stringList []string

func (l *stringList) String() string { return strings.Join(*l, ",") }

func (l *stringList) Set(value string) error {
	*l = append(*l, value)
	return nil
}

func runCatalog(args []string) error {
	if len(args) == 0 {
		return failf("usage: coffee-opencode-llm catalog <show|group|exclude|include|enable|disable> [flags]")
	}
	switch args[0] {
	case "show":
		return runCatalogShow(args[1:])
	case "group":
		return runCatalogGroup(args[1:])
	case "exclude":
		return runCatalogExclude(args[1:], true)
	case "include":
		return runCatalogExclude(args[1:], false)
	case "enable":
		return runCatalogEnable(args[1:], true)
	case "disable":
		return runCatalogEnable(args[1:], false)
	default:
		return failf("unknown catalog command %q", args[0])
	}
}

type labelState struct {
	active bool
	source string
}

func labelOf(model catalog.Model, group string, stored map[string]store.GroupSource) labelState {
	if item, ok := stored[group]; ok {
		return labelState{active: item.Active, source: item.Source}
	}
	source := model.GroupSources[group]
	if source == "" {
		source = "profile"
	}
	for _, value := range model.Groups {
		if value == group {
			return labelState{active: true, source: source}
		}
	}
	return labelState{source: source}
}

func labelText(state labelState) string {
	if state.active {
		return "yes"
	}
	return "no"
}

func sourceText(model catalog.Model, stored map[string]store.GroupSource) string {
	return fmt.Sprintf("expert=%s assistant=%s",
		labelSource(model, "coffee-expert", stored), labelSource(model, "coffee-assistant", stored))
}

// labelSource prints the label source only when the label is active.
func labelSource(model catalog.Model, group string, stored map[string]store.GroupSource) string {
	state := labelOf(model, group, stored)
	if !state.active {
		return "-"
	}
	return state.source
}

// groupPower counts the models that keep an active label of the group after the
// candidate is removed. A rotation group is never allowed to become empty.
func groupPower(models []catalog.Model, sources map[string]map[string]store.GroupSource,
	group, removed string) int {
	power := 0
	for _, model := range models {
		if model.ID == removed {
			continue
		}
		if labelOf(model, group, sources[model.ID]).active {
			power++
		}
	}
	return power
}

// exclusionText reports the blocking state, the reason of the last disable and
// the moment the model returns to its rotation group. A model whose disable
// window expired keeps its last disable reason but is not blocked anymore.
func exclusionText(item store.Availability, cooling, now int64) (string, string, int64) {
	switch {
	case item.ManualExcluded:
		return "manual", "manual exclusion", 0
	case item.DisabledUntilMS != nil && *item.DisabledUntilMS > now:
		return "disabled", reasonOr(item.DisabledReason, "disabled"), *item.DisabledUntilMS
	case item.ExcludedUntilMS != nil && *item.ExcludedUntilMS > now:
		return "cooldown", reasonOr(item.LastErrorKind, "cooldown"), *item.ExcludedUntilMS
	case cooling > now:
		return "quota_cooldown", reasonOr(item.LastErrorKind, "quota_cooldown"), cooling
	case !item.Enabled:
		return "no", reasonOr(item.DisabledReason, "disabled"), 0
	}
	return "no", item.DisabledReason, 0
}

func reasonOr(reason, fallback string) string {
	if reason == "" {
		return fallback
	}
	return reason
}

// availableAtText prints the UTC minute when the model returns to rotation, or
// a dash when nothing blocks it now.
func availableAtText(untilMS int64) string {
	if untilMS <= 0 {
		return "-"
	}
	return time.UnixMilli(untilMS).UTC().Format("2006-01-02T15:04Z")
}

func remainingText(ctx context.Context, led *ledger.Ledger, model catalog.Model) (string, error) {
	remaining, unlimited, err := led.Remaining(ctx, model.ID, model.Quota)
	if err != nil {
		return "", wrapDB(err, "read remaining quota for %s", model.ID)
	}
	if unlimited {
		return "null", nil
	}
	return strconv.FormatInt(remaining, 10), nil
}

func runCatalogShow(args []string) error {
	var p paths
	var groups stringList
	var asJSON bool
	fs := flag.NewFlagSet("catalog show", flag.ContinueOnError)
	p.register(fs)
	fs.Var(&groups, "group", "filter by rotation group: coffee-expert or coffee-assistant")
	fs.BoolVar(&asJSON, "json", false, "print the catalog as JSON")
	if err := parse(fs, args); err != nil {
		return err
	}
	for _, group := range groups {
		if !rotationGroups[group] {
			return failf("unknown group %q", group)
		}
	}
	cfg, err := loadConfig(p.config)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Limits.HTTPShutdownMS)*time.Millisecond)
	defer cancel()
	st, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()
	led := ledger.New(st, cfg.Limits.QuotaReserveTTLMS)
	effective, err := readEffective(ctx, cfg, st)
	if err != nil {
		return err
	}
	cat, err := loadCatalog(p, effective.Subscription.Value)
	if err != nil {
		return err
	}
	if err := st.UpsertCatalog(ctx, catalogRows(cat.Models)); err != nil {
		return wrapDB(err, "load catalog")
	}
	availability, err := st.Availability(ctx)
	if err != nil {
		return wrapDB(err, "read model availability")
	}
	sources, err := st.GroupSources(ctx)
	if err != nil {
		return wrapDB(err, "read group sources")
	}
	now := time.Now().UTC().UnixMilli()
	views := make([]modelView, 0, len(cat.Models))
	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	if !asJSON {
		if _, err := fmt.Fprintln(writer, "id\tcoffee-expert\tcoffee-assistant\tsource\tenabled\tquota_group\tremaining\texcluded\treason\tuntil"); err != nil {
			return err
		}
	}
	for _, model := range cat.Models {
		stored := sources[model.ID]
		expert := labelOf(model, "coffee-expert", stored)
		assistant := labelOf(model, "coffee-assistant", stored)
		if len(groups) > 0 {
			matched := false
			for _, group := range groups {
				if group == "coffee-expert" && expert.active {
					matched = true
				}
				if group == "coffee-assistant" && assistant.active {
					matched = true
				}
			}
			if !matched {
				continue
			}
		}
		enabled := model.Enabled
		item := store.Availability{ModelID: model.ID, Enabled: enabled}
		if runtimeState, ok := availability[model.ID]; ok {
			item = runtimeState
			enabled = runtimeState.Enabled
		}
		remaining, err := remainingText(ctx, led, model)
		if err != nil {
			return err
		}
		cooling, err := st.CoolingUntil(ctx, model.ID, model.Quota.Group)
		if err != nil {
			return wrapDB(err, "read cooldown for %s", model.ID)
		}
		excluded, reason, untilMS := exclusionText(item, cooling, now)
		var availableAt *int64
		if untilMS > 0 {
			value := untilMS
			availableAt = &value
		}
		quotaGroup := model.Quota.Group
		if quotaGroup == "" {
			quotaGroup = "-"
		}
		view := modelView{
			ID: model.ID, Expert: expert.active, Assistant: assistant.active,
			Source: sourceText(model, stored), Enabled: enabled, QuotaGroup: quotaGroup,
			Remaining: remaining, Excluded: excluded, Reason: reason,
			AvailableAtMS: availableAt, AvailableAt: availableAtText(untilMS),
		}
		views = append(views, view)
		if !asJSON {
			if _, err := fmt.Fprintf(writer, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				view.ID, labelText(expert), labelText(assistant), view.Source,
				enabledText(enabled), quotaGroup, remaining, excluded, reason, view.AvailableAt); err != nil {
				return err
			}
		}
	}
	if asJSON {
		raw, err := json.MarshalIndent(views, "", "  ")
		if err != nil {
			return fmt.Errorf("encode catalog: %w", err)
		}
		fmt.Println(string(raw))
		return nil
	}
	return writer.Flush()
}

func enabledText(enabled bool) string {
	if enabled {
		return "1"
	}
	return "0"
}

func runCatalogGroup(args []string) error {
	if len(args) == 0 {
		return failf("usage: coffee-opencode-llm catalog group <add|remove> <group> <id> [flags]")
	}
	action := args[0]
	fs := flag.NewFlagSet("catalog group "+action, flag.ContinueOnError)
	var p paths
	p.register(fs)
	positional, err := parsePositional(fs, args[1:])
	if err != nil {
		return err
	}
	if len(positional) != 2 {
		return failf("usage: coffee-opencode-llm catalog group <add|remove> <group> <id>")
	}
	group, id := positional[0], positional[1]
	if !rotationGroups[group] {
		return failf("unknown group %q", group)
	}
	if action != "add" && action != "remove" {
		return failf("unknown catalog group action %q", action)
	}
	cfg, err := loadConfig(p.config)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Limits.HTTPShutdownMS)*time.Millisecond)
	defer cancel()
	st, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()
	effective, err := readEffective(ctx, cfg, st)
	if err != nil {
		return err
	}
	cat, err := loadCatalog(p, effective.Subscription.Value)
	if err != nil {
		return err
	}
	if err := st.UpsertCatalog(ctx, catalogRows(cat.Models)); err != nil {
		return wrapDB(err, "load catalog")
	}
	model, ok := cat.Get(id)
	if !ok {
		return failf("model %q is not in the catalog", id)
	}
	if action == "add" {
		available := model.Enabled
		states, err := st.Availability(ctx)
		if err != nil {
			return wrapDB(err, "read model availability")
		}
		if state, ok := states[id]; ok {
			available = state.Enabled
		}
		if !available {
			return failf("model %q is disabled and cannot be added to %s", id, group)
		}
	}
	if action == "remove" {
		sources, err := st.GroupSources(ctx)
		if err != nil {
			return wrapDB(err, "read group sources")
		}
		if groupPower(cat.Models, sources, group, id) == 0 {
			return failf("group %s must keep at least one model: %s is the last one", group, id)
		}
	}
	if action == "add" {
		err = st.AddGroup(ctx, group, id)
	} else {
		err = st.RemoveGroup(ctx, group, id)
	}
	if err != nil {
		return wrapDB(err, "update group %s for %s", group, id)
	}
	fmt.Printf("%s %s %s\n", action, group, id)
	return nil
}

func runCatalogEnable(args []string, enabled bool) error {
	var p paths
	fs := flag.NewFlagSet("catalog enable", flag.ContinueOnError)
	p.register(fs)
	positional, err := parsePositional(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return failf("usage: coffee-opencode-llm catalog <enable|disable> <id>")
	}
	id := positional[0]
	cfg, err := loadConfig(p.config)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Limits.HTTPShutdownMS)*time.Millisecond)
	defer cancel()
	st, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()
	effective, err := readEffective(ctx, cfg, st)
	if err != nil {
		return err
	}
	cat, err := loadCatalog(p, effective.Subscription.Value)
	if err != nil {
		return err
	}
	if _, ok := cat.Get(id); !ok {
		return failf("model %q is not in the catalog", id)
	}
	if err := st.SetEnabledOverride(ctx, id, enabled); err != nil {
		return wrapDB(err, "set enabled override for %s", id)
	}
	action := "enabled"
	if !enabled {
		action = "disabled"
	}
	fmt.Printf("%s %s\n", action, id)
	return nil
}

func runCatalogExclude(args []string, excluded bool) error {
	var p paths
	fs := flag.NewFlagSet("catalog exclude", flag.ContinueOnError)
	p.register(fs)
	positional, err := parsePositional(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return failf("usage: coffee-opencode-llm catalog <exclude|include> <id>")
	}
	id := positional[0]
	cfg, err := loadConfig(p.config)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Limits.HTTPShutdownMS)*time.Millisecond)
	defer cancel()
	st, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()
	effective, err := readEffective(ctx, cfg, st)
	if err != nil {
		return err
	}
	cat, err := loadCatalog(p, effective.Subscription.Value)
	if err != nil {
		return err
	}
	if err := st.UpsertCatalog(ctx, catalogRows(cat.Models)); err != nil {
		return wrapDB(err, "load catalog")
	}
	if _, ok := cat.Get(id); !ok {
		return failf("model %q is not in the catalog", id)
	}
	if err := st.SetManualExcluded(ctx, id, excluded); err != nil {
		return wrapDB(err, "update exclusion for %s", id)
	}
	action := "include"
	if excluded {
		action = "exclude"
	}
	fmt.Printf("%s %s\n", action, id)
	return nil
}
