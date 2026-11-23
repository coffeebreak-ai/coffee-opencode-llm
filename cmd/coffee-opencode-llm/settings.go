package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/config"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
)

type settingValue struct {
	Value  string
	Source string
}

type effectiveSettings struct {
	Objective        settingValue
	Reasoning        settingValue
	ReasoningContext settingValue
	TraceScope       settingValue
	Subscription     settingValue
}

var settingValues = map[string][]string{
	"objective_mode":            {"balanced", "quality", "price"},
	"reasoning_mode":            {"min", "medium", "high", "max", "client"},
	"reasoning_context_enabled": {"off", "on"},
	"reasoning_trace_scope":     {"full", "final"},
	"subscription_mode":         {"free", "go", "go-plus", "go-teams"},
}

func allowedValue(key, value string) error {
	allowed, ok := settingValues[key]
	if !ok {
		return fmt.Errorf("unknown setting %q", key)
	}
	for _, item := range allowed {
		if item == value {
			return nil
		}
	}
	return fmt.Errorf("setting %s = %q is not one of %v", key, value, allowed)
}

func configSource(cfg *config.Config, key string) string {
	if cfg.Sources[key] == "env" {
		return "env"
	}
	return "config"
}

func readEffective(ctx context.Context, cfg *config.Config, st *store.Store) (effectiveSettings, error) {
	out := effectiveSettings{
		Objective:        settingValue{Value: cfg.ObjectiveMode, Source: configSource(cfg, "objective_mode")},
		Reasoning:        settingValue{Value: cfg.ReasoningMode, Source: configSource(cfg, "reasoning_mode")},
		ReasoningContext: settingValue{Value: reasoningContextValue(cfg.ReasoningContextEnabled), Source: configSource(cfg, "reasoning_context_enabled")},
		TraceScope:       settingValue{Value: cfg.ReasoningTraceScope, Source: configSource(cfg, "reasoning_trace_scope")},
		Subscription:     settingValue{Value: cfg.SubscriptionMode, Source: configSource(cfg, "subscription_mode")},
	}
	targets := map[string]*settingValue{
		"objective_mode":            &out.Objective,
		"reasoning_mode":            &out.Reasoning,
		"reasoning_context_enabled": &out.ReasoningContext,
		"reasoning_trace_scope":     &out.TraceScope,
		"subscription_mode":         &out.Subscription,
	}
	keys := make([]string, 0, len(targets))
	for key := range targets {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		item, err := st.Setting(ctx, key)
		if err == nil {
			*targets[key] = settingValue{Value: item.Value, Source: item.SourceKind}
			continue
		}
		if !errors.Is(err, store.ErrNotFound) {
			return effectiveSettings{}, wrapDB(err, "read setting %s", key)
		}
	}
	return out, nil
}

func reasoningContextValue(value int64) string {
	if value == 1 {
		return "on"
	}
	return "off"
}

func runSettings(args []string) error {
	if len(args) == 0 {
		return failf("usage: coffee-opencode-llm settings <show|set> [flags]")
	}
	switch args[0] {
	case "show":
		return runSettingsShow(args[1:])
	case "set":
		return runSettingsSet(args[1:])
	default:
		return failf("unknown settings command %q", args[0])
	}
}

func runSettingsShow(args []string) error {
	var p paths
	fs := flag.NewFlagSet("settings show", flag.ContinueOnError)
	p.register(fs)
	if err := parse(fs, args); err != nil {
		return err
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
	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, item := range []struct {
		key   string
		value settingValue
	}{
		{"objective_mode", effective.Objective},
		{"reasoning_mode", effective.Reasoning},
		{"reasoning_context_enabled", effective.ReasoningContext},
		{"reasoning_trace_scope", effective.TraceScope},
		{"subscription_mode", effective.Subscription},
	} {
		if _, err := fmt.Fprintf(writer, "%s\t%s\tsource=%s\n", item.key, item.value.Value, item.value.Source); err != nil {
			return err
		}
	}
	return writer.Flush()
}

func runSettingsSet(args []string) error {
	var p paths
	fs := flag.NewFlagSet("settings set", flag.ContinueOnError)
	p.register(fs)
	positional, err := parsePositional(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 2 {
		return failf("usage: coffee-opencode-llm settings set <objective_mode|reasoning_mode|reasoning_context_enabled|reasoning_trace_scope> <value>")
	}
	key, value := positional[0], positional[1]
	if key == "subscription_mode" {
		return failf("use coffee-opencode-llm plan set for subscription_mode")
	}
	if err := allowedValue(key, value); err != nil {
		return &cliError{code: exitConfig, err: err}
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
	if err := st.SetSetting(ctx, key, value, "command"); err != nil {
		return wrapDB(err, "write setting %s", key)
	}
	fmt.Printf("%s=%s source=command\n", key, value)
	return nil
}

func runPlan(args []string) error {
	if len(args) == 0 {
		return failf("usage: coffee-opencode-llm plan <show|set> [flags]")
	}
	switch args[0] {
	case "show":
		return runPlanShow(args[1:])
	case "set":
		return runPlanSet(args[1:])
	default:
		return failf("unknown plan command %q", args[0])
	}
}

func runPlanShow(args []string) error {
	var p paths
	fs := flag.NewFlagSet("plan show", flag.ContinueOnError)
	p.register(fs)
	if err := parse(fs, args); err != nil {
		return err
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
	fmt.Printf("subscription_mode=%s source=%s\n", effective.Subscription.Value, effective.Subscription.Source)
	return nil
}

func runPlanSet(args []string) error {
	var p paths
	fs := flag.NewFlagSet("plan set", flag.ContinueOnError)
	p.register(fs)
	positional, err := parsePositional(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return failf("usage: coffee-opencode-llm plan set <free|go|go-plus|go-teams>")
	}
	value := positional[0]
	if err := allowedValue("subscription_mode", value); err != nil {
		return &cliError{code: exitConfig, err: err}
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
	if err := st.SetSetting(ctx, "subscription_mode", value, "command"); err != nil {
		return wrapDB(err, "write subscription_mode")
	}
	fmt.Printf("subscription_mode=%s source=command\n", value)
	return nil
}
