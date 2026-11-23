package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/config"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
)

const (
	exitConfig = 1
	exitDB     = 2
)

type cliError struct {
	code int
	err  error
}

func (e *cliError) Error() string { return e.err.Error() }
func (e *cliError) Unwrap() error { return e.err }

func failf(format string, args ...any) error {
	return &cliError{code: exitConfig, err: fmt.Errorf(format, args...)}
}

func wrapDB(err error, format string, args ...any) error {
	return &cliError{code: exitDB, err: fmt.Errorf(format+": %w", append(args, err)...)}
}

type paths struct {
	config  string
	models  string
	experts string
}

func (p *paths) register(fs *flag.FlagSet) {
	fs.StringVar(&p.config, "config", "config/gateway.yaml", "path to gateway configuration")
	fs.StringVar(&p.models, "models", "config/models.yaml", "path to the model catalog")
	fs.StringVar(&p.experts, "experts", "config/experts.yaml", "path to the user group file")
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		var typed *cliError
		if errors.As(err, &typed) {
			os.Exit(typed.code)
		}
		os.Exit(exitConfig)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return failf("usage: coffee-opencode-llm <serve|migrate|catalog|settings|plan|retention> [flags]")
	}
	switch args[0] {
	case "serve":
		return runServe(args[1:])
	case "migrate":
		return runMigrate(args[1:])
	case "catalog":
		return runCatalog(args[1:])
	case "settings":
		return runSettings(args[1:])
	case "plan":
		return runPlan(args[1:])
	case "retention":
		return runRetention(args[1:])
	default:
		return failf("unknown command %q", args[0])
	}
}

func parse(fs *flag.FlagSet, args []string) error {
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return &cliError{code: exitConfig, err: err}
	}
	if fs.NArg() > 0 {
		return failf("%s: unexpected argument %q", fs.Name(), fs.Arg(0))
	}
	return nil
}

// parsePositional accepts flags and positional arguments in any order.
func parsePositional(fs *flag.FlagSet, args []string) ([]string, error) {
	var flagArgs, positional []string
	for i := 0; i < len(args); i++ {
		item := args[i]
		if strings.HasPrefix(item, "-") && item != "-" {
			flagArgs = append(flagArgs, item)
			if !strings.Contains(item, "=") && i+1 < len(args) {
				flagArgs = append(flagArgs, args[i+1])
				i++
			}
			continue
		}
		positional = append(positional, item)
	}
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(flagArgs); err != nil {
		return nil, &cliError{code: exitConfig, err: err}
	}
	return positional, nil
}

func loadConfig(path string) (*config.Config, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, &cliError{code: exitConfig, err: err}
	}
	return cfg, nil
}

func openStore(ctx context.Context, cfg *config.Config) (*store.Store, error) {
	st, err := store.Open(ctx, cfg.DataPath+"/coffee-opencode-llm.db", store.Options{
		BusyTimeoutMS: cfg.Limits.DBBusyTimeoutMS,
		MaxOpenConns:  int(cfg.Limits.DBMaxOpenConns),
		MaxIdleConns:  int(cfg.Limits.DBMaxIdleConns),
		WriteConns:    int(cfg.Limits.DBWriteConns),
	})
	if err != nil {
		return nil, wrapDB(err, "open storage")
	}
	if err := st.Migrate(ctx, "sqlite"); err != nil {
		_ = st.Close()
		return nil, wrapDB(err, "apply migrations")
	}
	return st, nil
}

func loadCatalog(p paths, profile string) (*catalog.Catalog, error) {
	cat, err := catalog.Load(p.models, p.experts, profile)
	if err != nil {
		return nil, &cliError{code: exitConfig, err: err}
	}
	return cat, nil
}

func runMigrate(args []string) error {
	var p paths
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
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
	st, err := store.Open(ctx, cfg.DataPath+"/coffee-opencode-llm.db", store.Options{
		BusyTimeoutMS: cfg.Limits.DBBusyTimeoutMS,
		MaxOpenConns:  int(cfg.Limits.DBMaxOpenConns),
		MaxIdleConns:  int(cfg.Limits.DBMaxIdleConns),
		WriteConns:    int(cfg.Limits.DBWriteConns),
	})
	if err != nil {
		return wrapDB(err, "open storage")
	}
	defer func() { _ = st.Close() }()
	if err := st.Migrate(ctx, "sqlite"); err != nil {
		return wrapDB(err, "apply migrations")
	}
	fmt.Println("migrations applied")
	return nil
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
