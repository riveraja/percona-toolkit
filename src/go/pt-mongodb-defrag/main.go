// This program is copyright 2026 Percona LLC and/or its affiliates.
//
// THIS PROGRAM IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED
// WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the Free Software
// Foundation, version 2.
//
// You should have received a copy of the GNU General Public License, version 2
// along with this program; if not, see <https://www.gnu.org/licenses/>.

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

	"github.com/percona/percona-toolkit/src/go/lib/versioncheck"
	"github.com/percona/percona-toolkit/src/go/pt-mongodb-defrag/internal/config"
	"github.com/percona/percona-toolkit/src/go/pt-mongodb-defrag/internal/defrag"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	toolname = "pt-mongodb-defrag"
)

// We do not set anything here, these variables are defined by the Makefile
var (
	Build     string //nolint
	GoVersion string //nolint
	Version   string //nolint
	Commit    string //nolint
)

func main() {
	cfg, showVersion, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n\n", err)
		printUsage(os.Stderr)
		os.Exit(2)
	}

	if showVersion {
		fmt.Printf("Version %s\n", Version)
		fmt.Printf("Build: %s using %s\n", Build, GoVersion)
		fmt.Printf("Commit: %s\n", Commit)
		return
	}

	initLogger(cfg.Quiet)

	advice, err := versioncheck.CheckUpdates(toolname, Version)
	if err != nil {
		log.Debug().Err(err).Msg("cannot check version updates")
	} else if advice != "" {
		log.Warn().Msg(advice)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	started := time.Now()

	if cfg.PreventAutoMerge {
		if err := defrag.PreventAutoMerge(ctx, cfg); err != nil {
			if errors.Is(err, context.Canceled) {
				log.Warn().Msg("operation canceled")
				os.Exit(130)
			}
			log.Error().Err(err).Msg("prevent-automerge failed")
			os.Exit(1)
		}
		log.Info().Str("elapsed", time.Since(started).Round(time.Second).String()).Msg("completed")
		return
	}

	if err := defrag.Run(ctx, cfg); err != nil {
		if errors.Is(err, context.Canceled) {
			log.Warn().Msg("operation canceled")
			os.Exit(130)
		}
		log.Error().Err(err).Msg("run failed")
		os.Exit(1)
	}

	log.Info().Str("elapsed", time.Since(started).Round(time.Second).String()).Msg("completed")
}

func initLogger(quiet bool) {
	zerolog.TimeFieldFormat = time.RFC3339
	output := zerolog.ConsoleWriter{Out: os.Stderr, NoColor: true}
	output.FormatTimestamp = func(i interface{}) string {
		t, ok := i.(string)
		if !ok {
			return ""
		}
		return t
	}
	log.Logger = zerolog.New(output).With().Timestamp().Logger()
	if quiet {
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
}

func parseFlags(args []string) (config.Config, bool, error) {
	var cfg config.Config
	var sleep time.Duration
	var splitMergeSleep time.Duration
	var phases string
	var showVersion bool

	fs := flag.NewFlagSet("pt-mongodb-defrag", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.StringVar(&cfg.URI, "uri", "", "MongoDB connection URI for a mongos router")
	fs.StringVar(&cfg.Namespace, "namespace", "", "Target namespace in the form database.collection")
	fs.Var(config.NewHumanBytesValue(&cfg.ChunkSizeBytes), "chunk-size", "Target max chunk size in bytes, or with K/M/G suffixes. Defaults to cluster chunksize")
	fs.DurationVar(&sleep, "sleep", 500*time.Millisecond, "Sleep between non-split/merge metadata-changing operations such as moveRange or clearJumboFlag")
	fs.DurationVar(&splitMergeSleep, "split-merge-sleep", 500*time.Millisecond, "Minimum delay between consecutive split and merge commands, similar to chunkDefragmentationThrottlingMS")
	fs.BoolVar(&cfg.DryRun, "dry-run", false, "Print planned actions without changing the cluster")
	fs.BoolVar(&cfg.AllowMoves, "allow-moves", true, "Allow moveRange before merging adjacent chunks on different shards")
	fs.BoolVar(&cfg.AllowZonedMoves, "allow-zoned-moves", false, "Allow chunk moves on collections with configured zones")
	fs.IntVar(&cfg.MaxMergePasses, "max-merge-passes", 1000, "Maximum merge planning passes")
	fs.StringVar(&cfg.PlanOut, "plan-out", "", "Optional path to write the phase-1 sizing snapshot as JSON")
	fs.StringVar(&phases, "phases", "1,2,3,4", "Comma-separated phases to run")
	fs.BoolVar(&cfg.Quiet, "quiet", false, "Reduce log volume")
	fs.BoolVar(&showVersion, "version", false, "Print version information and exit")

	// Prevent-automerge flags.
	fs.BoolVar(&cfg.PreventAutoMerge, "prevent-automerge", false, "Prevent AutoMerger from re-merging a specific chunk by splitting it and moving one half to a different shard")
	fs.StringVar(&cfg.ChunkID, "chunk-id", "", "Chunk _id from config.chunks (required with -prevent-automerge)")
	fs.StringVar(&cfg.ChunkQuery, "chunk-query", "", "Query document identifying the split point within the chunk (required with -prevent-automerge). Accepts JSON ({\"sk\": 15000}) or key=value (sk=15000) format")
	fs.StringVar(&cfg.TargetShard, "target-shard", "", "Target shard to move the upper half of the split chunk (required with -prevent-automerge)")
	fs.BoolVar(&cfg.AutoApprove, "auto-approve", false, "Skip the confirmation prompt when using -prevent-automerge")

	if err := fs.Parse(args); err != nil {
		return cfg, false, err
	}

	cfg.Sleep = sleep
	cfg.SplitMergeSleep = splitMergeSleep
	enabled, err := config.ParsePhases(phases)
	if err != nil {
		return cfg, false, err
	}
	cfg.EnabledPhases = enabled

	if showVersion {
		return cfg, true, nil
	}

	if strings.TrimSpace(cfg.URI) == "" {
		return cfg, false, errors.New("missing -uri")
	}
	if err := cfg.Validate(); err != nil {
		return cfg, false, err
	}

	return cfg, false, nil
}

func printUsage(w *os.File) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  pt-mongodb-defrag -uri mongodb://mongos:27017 -namespace db.collection [flags]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Defragmentation modes:")
	fmt.Fprintln(w, "  (default)    Run the 4-phase defragmentation pipeline")
	fmt.Fprintln(w, "  -prevent-automerge   Split a chunk and move half to another shard,")
	fmt.Fprintln(w, "                       preventing AutoMerger from re-merging them")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Prevent-automerge flags (with -prevent-automerge):")
	fmt.Fprintln(w, "  -chunk-id       Chunk _id from config.chunks")
	fmt.Fprintln(w, "  -chunk-query    Split point: JSON ({\"sk\":15000}) or key=value (sk=15000)")
	fmt.Fprintln(w, "  -target-shard   Destination shard for the upper half")
	fmt.Fprintln(w, "  -auto-approve   Skip confirmation prompt")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Defragmentation flags (without -prevent-automerge):")
	fmt.Fprintln(w, "  See -help for the full list")
}
