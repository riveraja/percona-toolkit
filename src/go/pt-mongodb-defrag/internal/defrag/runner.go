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

package defrag

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/percona/percona-toolkit/src/go/pt-mongodb-defrag/internal/config"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	// estimationLowerBoundRatio and estimationUpperBoundRatio define the
	// range around the target chunk size within which we trust the estimated
	// dataSize result and skip the more expensive exact (non-estimate) call.
	estimationLowerBoundRatio = 0.8
	estimationUpperBoundRatio = 1.2
)

func Run(ctx context.Context, cfg config.Config) error {
	globalStart := time.Now()
	summary := RunSummary{
		URI:       maskURI(cfg.URI),
		StartedAt: globalStart.UTC(),
		Phases:    make([]PhaseTiming, 0, 4),
	}

	m, err := Connect(ctx, cfg.URI, cfg.MetadataTimeout, cfg.CommandTimeout)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.Close(disconnectCtx); err != nil {
			log.Warn().Err(err).Msg("disconnect failed")
		}
	}()

	if err := m.EnsureMongos(ctx); err != nil {
		return fmt.Errorf("connection must target mongos: %w", err)
	}

	buildInfo, err := m.BuildInfo(ctx)
	if err != nil {
		return fmt.Errorf("buildInfo: %w", err)
	}
	if len(buildInfo.VersionArray) >= 1 && buildInfo.VersionArray[0] < 7 {
		log.Info().Str("version", buildInfo.Version).Msg("MongoDB version below the supported baseline; the tool only targets 7.0+")
	} else {
		log.Info().Str("version", buildInfo.Version).Msg("MongoDB 7.0+ automatically merges adjacent chunks; manual defragmentation is usually only justified for cleanup or other exceptional cases")
	}

	meta, err := m.CollectionMetadata(ctx, cfg.Namespace)
	if err != nil {
		return fmt.Errorf("load collection metadata: %w", err)
	}
	maxChunkBytes := cfg.ChunkSizeBytes
	if maxChunkBytes == 0 {
		chunkSizeMB, err := m.ClusterChunkSizeMB(ctx)
		if err != nil {
			return fmt.Errorf("resolve chunk size: %w", err)
		}
		maxChunkBytes = int64(chunkSizeMB * 1024 * 1024)
	}

	hasZones, err := m.HasZones(ctx, cfg.Namespace)
	if err != nil {
		return fmt.Errorf("check zones: %w", err)
	}
	if hasZones && cfg.AllowMoves && !cfg.AllowZonedMoves {
		log.Info().Msg("collection has zones; cross-shard merge prep is disabled unless -allow-zoned-moves is set")
	}
	if cfg.AllowZonedMoves && !cfg.AllowMoves {
		log.Warn().Msg("-allow-zoned-moves has no effect because -allow-moves is false")
	}
	log.Info().Msg("run manual defragmentation during a shard balancing window when possible to reduce metadata-update impact on CRUD latency")
	log.Info().Msg("merging chunks clears placement history for the merged ranges; snapshot reads and some transactions can transiently fail with stale chunk history errors")

	log.Info().
		Str("namespace", cfg.Namespace).
		Bool("dry_run", cfg.DryRun).
		Str("chunk_size", config.FormatMiB(maxChunkBytes)).
		Bool("allow_moves", cfg.AllowMoves).
		Str("split_merge_sleep", cfg.SplitMergeSleep.String()).
		Msg("starting defragmentation")

	chunks, err := m.Chunks(ctx, meta, cfg.Namespace)
	if err != nil {
		return fmt.Errorf("load chunks: %w", err)
	}

	metrics := make(map[string]ChunkMetrics, len(chunks))

	// Phase execution with timing capture
	if cfg.EnabledPhases[1] {
		phaseStart := time.Now()
		if err := runPhase1(ctx, m, cfg, meta, chunks, maxChunkBytes, metrics); err != nil {
			return err
		}
		summary.Phases = append(summary.Phases, PhaseTiming{
			Phase:   1,
			Started: phaseStart.UTC(),
			Ended:   time.Now().UTC(),
			Elapsed: time.Since(phaseStart),
		})
	}

	if cfg.EnabledPhases[2] {
		phaseStart := time.Now()
		if err := runPhase2(ctx, m, cfg, meta, maxChunkBytes, metrics, hasZones); err != nil {
			return err
		}
		summary.Phases = append(summary.Phases, PhaseTiming{
			Phase:   2,
			Started: phaseStart.UTC(),
			Ended:   time.Now().UTC(),
			Elapsed: time.Since(phaseStart),
		})
	}

	if cfg.EnabledPhases[3] {
		phaseStart := time.Now()
		if err := runPhase3(ctx, m, cfg, meta, maxChunkBytes, metrics); err != nil {
			return err
		}
		summary.Phases = append(summary.Phases, PhaseTiming{
			Phase:   3,
			Started: phaseStart.UTC(),
			Ended:   time.Now().UTC(),
			Elapsed: time.Since(phaseStart),
		})
	}

	if cfg.EnabledPhases[4] {
		phaseStart := time.Now()
		if err := runPhase4(ctx, m, cfg, meta, maxChunkBytes, metrics); err != nil {
			return err
		}
		summary.Phases = append(summary.Phases, PhaseTiming{
			Phase:   4,
			Started: phaseStart.UTC(),
			Ended:   time.Now().UTC(),
			Elapsed: time.Since(phaseStart),
		})
	}

	summary.EndedAt = time.Now().UTC()
	summary.TotalElapsed = summary.EndedAt.Sub(summary.StartedAt)

	// Emit final summary report (text format; JSON export can be added later via --json/--report-out)
	log.Info().
		Str("uri", summary.URI).
		Str("started", summary.StartedAt.Format(time.RFC3339)).
		Str("ended", summary.EndedAt.Format(time.RFC3339)).
		Str("total_elapsed", summary.TotalElapsed.Round(time.Second).String()).
		Msg("defragmentation run complete")

	for _, p := range summary.Phases {
		log.Info().
			Int("phase", p.Phase).
			Str("started", p.Started.Format(time.RFC3339)).
			Str("ended", p.Ended.Format(time.RFC3339)).
			Str("elapsed", p.Elapsed.Round(time.Second).String()).
			Msg("phase timing")
	}

	return nil
}

func runPhase1(
	ctx context.Context,
	m *Mongo,
	cfg config.Config,
	meta CollectionMetadata,
	chunks []Chunk,
	maxChunkBytes int64,
	metrics map[string]ChunkMetrics,
) error {
	log.Info().Int("chunks", len(chunks)).Msg("phase 1/4: calculating chunk sizes")
	started := time.Now()

	snapshot := Snapshot{
		Namespace:      cfg.Namespace,
		CollectedAt:    time.Now().UTC().Format(time.RFC3339),
		ChunkSizeBytes: maxChunkBytes,
		ShardKey:       meta.Key,
		Chunks:         make([]SnapshotChunk, 0, len(chunks)),
	}

	for idx, chunk := range chunks {
		if err := ctx.Err(); err != nil {
			return err
		}

		key := chunkKey(chunk.Min, chunk.Max)
	size, err := ensureChunkMetrics(ctx, m, cfg, meta, chunk, maxChunkBytes, metrics, false)
		if err != nil {
			return fmt.Errorf("calculate chunk size %s: %w", key, err)
		}
		minBounds := formatBounds(chunk.Min)
		maxBounds := formatBounds(chunk.Max)
		snapshot.Chunks = append(snapshot.Chunks, SnapshotChunk{
			Min:       minBounds,
			Max:       maxBounds,
			Shard:     chunk.Shard,
			Jumbo:     chunk.Jumbo,
			Bytes:     size.Bytes,
			Documents: size.Documents,
		})

		if !cfg.Quiet && ((idx+1)%10 == 0 || idx+1 == len(chunks)) {
			pct := float64(idx+1) / float64(len(chunks)) * 100
			elapsed := time.Since(started)
			rate := float64(idx+1) / elapsed.Seconds()
			log.Info().
				Int("done", idx+1).
				Int("total", len(chunks)).
				Str("pct", fmt.Sprintf("%.1f", pct)).
				Str("chunks_per_sec", fmt.Sprintf("%.2f", rate)).
				Msg("phase 1 progress")
		}
	}

	if cfg.PlanOut != "" {
		data, err := json.MarshalIndent(snapshot, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal plan snapshot: %w", err)
		}
		if cfg.DryRun {
			log.Info().Str("path", cfg.PlanOut).Msg("dry-run: plan snapshot not written (use without -dry-run to persist)")
		} else if err := os.WriteFile(cfg.PlanOut, data, 0o644); err != nil {
			return fmt.Errorf("write plan snapshot: %w", err)
		}
	}

	log.Info().Str("elapsed", time.Since(started).Round(time.Second).String()).Msg("phase 1 complete")
	return nil
}

func runPhase2(
	ctx context.Context,
	m *Mongo,
	cfg config.Config,
	meta CollectionMetadata,
	maxChunkBytes int64,
	metrics map[string]ChunkMetrics,
	hasZones bool,
) error {
	log.Info().Msg("phase 2/4: merging adjacent chunks")
	started := time.Now()
	merges := 0
	moves := 0

	// Initial load – subsequent passes only reload when no progress was made
	// or when an error forces a fresh view of the cluster.
	chunks, err := m.Chunks(ctx, meta, cfg.Namespace)
	if err != nil {
		return fmt.Errorf("load chunks: %w", err)
	}

	for pass := 1; pass <= cfg.MaxMergePasses; pass++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		progress := false
		for i := 0; i < len(chunks)-1; {
			left := chunks[i]
			right := chunks[i+1]

	leftSize, err := ensureChunkMetrics(ctx, m, cfg, meta, left, maxChunkBytes, metrics, false)
			if err != nil {
				return err
			}
	rightSize, err := ensureChunkMetrics(ctx, m, cfg, meta, right, maxChunkBytes, metrics, false)
			if err != nil {
				return err
			}

			combined := leftSize.Bytes + rightSize.Bytes
			if combined > maxChunkBytes {
				i++
				continue
			}
	leftSize, err = ensureChunkMetrics(ctx, m, cfg, meta, left, maxChunkBytes, metrics, true)
			if err != nil {
				return err
			}
	rightSize, err = ensureChunkMetrics(ctx, m, cfg, meta, right, maxChunkBytes, metrics, true)
			if err != nil {
				return err
			}
			combined = leftSize.Bytes + rightSize.Bytes
			if combined > maxChunkBytes {
				i++
				continue
			}

			targetShard := left.Shard
			moveChunk := Chunk{}
			moveChunkBytes := int64(0)
			needsMove := left.Shard != right.Shard
			if needsMove {
				if !cfg.AllowMoves || (hasZones && !cfg.AllowZonedMoves) {
					i++
					continue
				}
				if leftSize.Bytes > rightSize.Bytes {
					targetShard = left.Shard
					moveChunk = right
					moveChunkBytes = rightSize.Bytes
				} else {
					targetShard = right.Shard
					moveChunk = left
					moveChunkBytes = leftSize.Bytes
				}

				log.Info().
					Int("pass", pass).
					Str("action", "moveRange").
					Str("from", moveChunk.Shard).
					Str("to", targetShard).
					Str("bytes", metricMiB(moveChunkBytes)).
					Msg("phase 2 action")
				if !cfg.DryRun {
					if err := m.MoveRange(ctx, cfg.Namespace, moveChunk.Min, moveChunk.Max, targetShard); err != nil {
						return wrapMoveRangeError(chunkKey(moveChunk.Min, moveChunk.Max), err)
					}
					sleepContext(ctx, cfg.Sleep)
				}
				moves++
			}

			log.Info().
				Int("pass", pass).
				Str("action", "mergeChunks").
				Str("bounds", chunkKey(left.Min, right.Max)).
				Str("bytes", fmt.Sprintf("%.2fMiB", float64(combined)/1024/1024)).
				Msg("phase 2 action")
			if !cfg.DryRun {
				if err := m.MergeChunks(ctx, cfg.Namespace, left.Min, right.Max); err != nil {
					return fmt.Errorf("mergeChunks %s: %w", chunkKey(left.Min, right.Max), err)
				}
				sleepContext(ctx, cfg.SplitMergeSleep)
			}
			mergedChunk := Chunk{
				Min:   left.Min,
				Max:   right.Max,
				Shard: targetShard,
				Jumbo: left.Jumbo || right.Jumbo,
			}
			metrics[chunkKey(left.Min, right.Max)] = ChunkMetrics{
				Bytes:       combined,
				Documents:   leftSize.Documents + rightSize.Documents,
				Estimated:   false,
				Oversized:   combined > maxChunkBytes,
				MarkedJumbo: mergedChunk.Jumbo,
			}
			delete(metrics, chunkKey(left.Min, left.Max))
			delete(metrics, chunkKey(right.Min, right.Max))
			merges++
			progress = true
			chunks[i] = mergedChunk
			chunks = append(chunks[:i+1], chunks[i+2:]...)
			if i > 0 {
				i--
			}
		}

		if !progress {
			log.Info().
				Int("passes", pass).
				Int("moves", moves).
				Int("merges", merges).
				Str("elapsed", time.Since(started).Round(time.Second).String()).
				Msg("phase 2 complete")
			return nil
		}
	}

	log.Warn().
		Int("passes", cfg.MaxMergePasses).
		Int("moves", moves).
		Int("merges", merges).
		Msg("phase 2 stopped at pass limit")
	return nil
}

func runPhase3(
	ctx context.Context,
	m *Mongo,
	cfg config.Config,
	meta CollectionMetadata,
	maxChunkBytes int64,
	metrics map[string]ChunkMetrics,
) error {
	log.Info().Msg("phase 3/4: re-evaluating jumbo chunks")
	started := time.Now()

	chunks, err := m.Chunks(ctx, meta, cfg.Namespace)
	if err != nil {
		return fmt.Errorf("reload chunks: %w", err)
	}

	jumboChunks := 0
	cleared := 0
	split := 0
	for _, chunk := range chunks {
		if !chunk.Jumbo {
			continue
		}
		jumboChunks++

	metric, err := ensureChunkMetrics(ctx, m, cfg, meta, chunk, maxChunkBytes, metrics, false)
		if err != nil {
			return err
		}
		if metric.Bytes <= maxChunkBytes {
			log.Info().
				Str("action", "clearJumboFlag").
				Str("bounds", chunkKey(chunk.Min, chunk.Max)).
				Str("bytes", fmt.Sprintf("%.2fMiB", float64(metric.Bytes)/1024/1024)).
				Msg("phase 3 action")
			if !cfg.DryRun {
				if err := m.ClearJumboFlag(ctx, cfg.Namespace, chunk.Min, chunk.Max); err != nil {
					return fmt.Errorf("clearJumboFlag %s: %w", chunkKey(chunk.Min, chunk.Max), err)
				}
				sleepContext(ctx, cfg.Sleep)
			}
			cleared++
			continue
		}

		log.Info().
			Str("action", "split").
			Str("bounds", chunkKey(chunk.Min, chunk.Max)).
			Str("bytes", fmt.Sprintf("%.2fMiB", float64(metric.Bytes)/1024/1024)).
			Msg("phase 3 action")
		if !cfg.DryRun {
			if err := m.SplitChunk(ctx, cfg.Namespace, chunk, isHashedShardKey(meta.Key)); err != nil {
				log.Warn().Str("bounds", chunkKey(chunk.Min, chunk.Max)).Err(err).Msg("split failed for jumbo chunk")
				continue
			}
			sleepContext(ctx, cfg.SplitMergeSleep)
		}
		split++
	}

	log.Info().
		Int("jumbo_chunks", jumboChunks).
		Int("cleared", cleared).
		Int("split", split).
		Str("elapsed", time.Since(started).Round(time.Second).String()).
		Msg("phase 3 complete")
	return nil
}

func runPhase4(
	ctx context.Context,
	m *Mongo,
	cfg config.Config,
	meta CollectionMetadata,
	maxChunkBytes int64,
	metrics map[string]ChunkMetrics,
) error {
	log.Info().Msg("phase 4/4: splitting oversized non-jumbo chunks")
	started := time.Now()

	chunks, err := m.Chunks(ctx, meta, cfg.Namespace)
	if err != nil {
		return fmt.Errorf("reload chunks: %w", err)
	}

	oversized := 0
	split := 0
	for _, chunk := range chunks {
		if chunk.Jumbo {
			continue
		}
	metric, err := ensureChunkMetrics(ctx, m, cfg, meta, chunk, maxChunkBytes, metrics, false)
		if err != nil {
			return err
		}
		if metric.Bytes <= maxChunkBytes {
			continue
		}
	metric, err = ensureChunkMetrics(ctx, m, cfg, meta, chunk, maxChunkBytes, metrics, true)
		if err != nil {
			return err
		}
		if metric.Bytes <= maxChunkBytes {
			continue
		}
		oversized++

		log.Info().
			Str("action", "split").
			Str("bounds", chunkKey(chunk.Min, chunk.Max)).
			Str("bytes", fmt.Sprintf("%.2fMiB", float64(metric.Bytes)/1024/1024)).
			Msg("phase 4 action")
		if !cfg.DryRun {
			if err := m.SplitChunk(ctx, cfg.Namespace, chunk, isHashedShardKey(meta.Key)); err != nil {
				log.Warn().Str("bounds", chunkKey(chunk.Min, chunk.Max)).Err(err).Msg("split failed for oversized chunk")
				continue
			}
			sleepContext(ctx, cfg.SplitMergeSleep)
		}
		split++
	}

	log.Info().
		Int("oversized", oversized).
		Int("split", split).
		Str("elapsed", time.Since(started).Round(time.Second).String()).
		Msg("phase 4 complete")
	return nil
}

// ensureChunkMetrics returns cached metrics when available.
// When forceExact is true it always performs a non-estimate dataSize call.
// When forceExact is false it may first try an estimate and only fall back to
// an exact measurement when shouldUseExactMetrics decides it is necessary.
func ensureChunkMetrics(
	ctx context.Context,
	m *Mongo,
	cfg config.Config,
	meta CollectionMetadata,
	chunk Chunk,
	maxChunkBytes int64,
	metrics map[string]ChunkMetrics,
	forceExact bool,
) (ChunkMetrics, error) {
	key := chunkKey(chunk.Min, chunk.Max)
	if metric, ok := metrics[key]; ok {
		if forceExact {
			if !metric.Estimated {
				return metric, nil
			}
		} else if metric.Bytes > 0 || metric.Documents > 0 {
			return metric, nil
		}
	}

	estimate := !forceExact
	metric, err := m.DataSize(ctx, cfg.Database, cfg.Namespace, meta.Key, chunk.Min, chunk.Max, estimate)
	if err != nil {
		return ChunkMetrics{}, fmt.Errorf("calculate chunk size %s: %w", key, err)
	}
	if !forceExact && shouldUseExactMetrics(metric, maxChunkBytes, chunk.Jumbo) {
		metric, err = m.DataSize(ctx, cfg.Database, cfg.Namespace, meta.Key, chunk.Min, chunk.Max, false)
		if err != nil {
			return ChunkMetrics{}, fmt.Errorf("calculate chunk size %s: %w", key, err)
		}
	}
	metric.Oversized = metric.Bytes > maxChunkBytes
	metric.MarkedJumbo = chunk.Jumbo
	metrics[key] = metric
	return metric, nil
}

func shouldUseExactMetrics(metric ChunkMetrics, maxChunkBytes int64, markedJumbo bool) bool {
	if markedJumbo {
		return true
	}
	if maxChunkBytes <= 0 {
		return true
	}
	lowerBound := int64(float64(maxChunkBytes) * estimationLowerBoundRatio)
	upperBound := int64(float64(maxChunkBytes) * estimationUpperBoundRatio)
	return metric.Bytes >= lowerBound && metric.Bytes <= upperBound
}

func chunkKey(min, max bson.D) string {
	return strings.Join([]string{formatBounds(min), formatBounds(max)}, " -> ")
}

func formatBounds(doc bson.D) string {
	bounds, err := marshalBounds(doc)
	if err != nil {
		log.Warn().Err(err).Interface("bounds_doc", doc).Msg("failed to marshal chunk bounds")
		return fmt.Sprintf("<marshal-error:%v>", doc)
	}
	return bounds
}

func isHashedShardKey(key bson.D) bool {
	for _, elem := range key {
		switch v := elem.Value.(type) {
		case string:
			if v == "hashed" {
				return true
			}
		}
	}
	return false
}

func sleepContext(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func wrapMoveRangeError(bounds string, err error) error {
	msg := fmt.Sprintf("moveRange %s", bounds)
	if isOrphanCleanupTimeout(err) {
		return fmt.Errorf("%s: %w; MongoDB timed out deleting donor-side orphaned documents after the range transfer. This is a server-side migration cleanup limit, not the tool's sleep setting. Retry after the cluster settles, or rerun with -allow-moves=false to skip cross-shard merge preparation", msg, err)
	}
	return fmt.Errorf("%s: %w", msg, err)
}

func isOrphanCleanupTimeout(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Failed to delete orphaned") && strings.Contains(msg, "ExceededTimeLimit")
}

func metricMiB(bytes int64) string {
	return fmt.Sprintf("%.2fMiB", float64(bytes)/1024/1024)
}

// maskURI returns a copy of the MongoDB URI with username/password redacted.
// It preserves host, port, replicaSet, authSource, and other query parameters.
func maskURI(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		// best-effort fallback: try to strip the obvious credential portion
		if idx := strings.Index(raw, "@"); idx != -1 {
			if protoIdx := strings.Index(raw, "://"); protoIdx != -1 {
				return raw[:protoIdx+3] + "<redacted>@" + raw[idx+1:]
			}
		}
		return "<redacted>"
	}
	if u.User != nil {
		u.User = url.User("<redacted>")
	}
	return u.String()
}
