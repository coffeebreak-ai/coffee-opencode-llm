package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
)

func runRetention(args []string) error {
	var p paths
	var apply bool
	var batchRows int64
	fs := flag.NewFlagSet("retention", flag.ContinueOnError)
	p.register(fs)
	fs.BoolVar(&apply, "apply", false, "delete rows older than retention_days")
	fs.Int64Var(&batchRows, "batch-rows", 0, "rows per delete batch (required with -apply)")
	if err := parse(fs, args); err != nil {
		return err
	}
	if apply && batchRows <= 0 {
		return failf("retention: -batch-rows must be positive with -apply")
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

	cutoff := st.NowMS() - cfg.Limits.RetentionDays*int64(24*time.Hour/time.Millisecond)
	counts, err := st.RetentionCounts(ctx, cutoff)
	if err != nil {
		return wrapDB(err, "count retention rows")
	}
	fmt.Printf("cutoff=%d (%s)\n", cutoff, time.UnixMilli(cutoff).UTC().Format(time.RFC3339))
	fmt.Printf("plan %s\n", store.FormatRetentionCounts(counts))
	if !apply {
		fmt.Println("dry run: nothing deleted")
		return nil
	}

	deleted, err := st.RetentionDelete(ctx, cutoff, batchRows)
	if err != nil {
		return wrapDB(err, "delete retention rows")
	}
	fmt.Printf("deleted %s\n", store.FormatRetentionCounts(deleted))
	if err := st.Journal(ctx, store.JournalEvent{
		Level: "info",
		Kind:  "retention_applied",
		Note:  fmt.Sprintf("cutoff=%d deleted %s", cutoff, store.FormatRetentionCounts(deleted)),
	}); err != nil {
		return wrapDB(err, "write retention journal")
	}
	if os.Getenv("RETENTION_VACUUM") == "1" {
		if err := st.Vacuum(ctx); err != nil {
			return wrapDB(err, "vacuum storage")
		}
		fmt.Println("vacuum done")
	}
	return nil
}
