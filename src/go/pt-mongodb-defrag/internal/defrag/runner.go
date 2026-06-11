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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/percona/percona-toolkit/src/go/pt-mongodb-defrag/internal/config"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
)

func Run(ctx context.Context, cfg config.Config) error {
	m, err := Connect(ctx, cfg.URI)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer m.Close(context.Background())

	if err := m.EnsureMongos(ctx); err != nil {
		return fmt.Errorf("connection must target mongos: %w", err)
	}

	buildInfo, err := m.BuildInfo(ctx)
	if err != nil {
		return fmt.Errorf("buildInfo: %w", err)
	}
	if len(buildInfo.VersionArray) >= 1 && buildInfo.VersionArray[0] < 7 {
		log.Warn().Str("version", buildInfo.Version).Msg("MongoDB version below the supported baseline; the tool only targets 7.0+")
	} else {
		log.Warn().Str("version", buildInfo.Version).Msg("MongoDB 7.0+ automatically merges adjacent chunks; manual defragmentation is usually only justified for cleanup or other exceptional cases")
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
		log.Warn().Msg("collection has zones; cross-shard merge prep is disabled unless -allow-zoned-moves is set")
	}
	log.Warn().Msg("run manual defragmentation during a shard balancing window when possible to reduce metadata-update impact on CRUD latency")
	log.Warn().Msg("merging chunks clears placement history for the merged ranges; snapshot reads and some transactions can transiently fail with stale chunk history errors")

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
	if cfg.EnabledPhases[1] {
		if err := runPhase1(ctx, m, cfg, meta, chunks, maxChunkBytes, metrics); err != nil {
			return err
		}
	}

	if cfg.EnabledPhases[2] {
		if err := runPhase2(ctx, m, cfg, meta, maxChunkBytes, metrics, hasZones); err != nil {
			return err
		}
	}

	if cfg.EnabledPhases[3] {
		if err := runPhase3(ctx, m, cfg, meta, maxChunkBytes, metrics); err != nil {
			return err
		}
	}

	if cfg.EnabledPhases[4] {
		if err := runPhase4(ctx, m, cfg, meta, maxChunkBytes, metrics); err != nil {
			return err
		}
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
		size, err := m.DataSize(ctx, cfg.Database, cfg.Namespace, meta.Key, chunk.Min, chunk.Max)
		if err != nil {
			return fmt.Errorf("calculate chunk size %s: %w", key, err)
		}
		size.Oversized = size.Bytes > maxChunkBytes
		size.MarkedJumbo = chunk.Jumbo
		metrics[key] = size
		snapshot.Chunks = append(snapshot.Chunks, SnapshotChunk{
			Min:       mustMarshalBounds(chunk.Min),
			Max:       mustMarshalBounds(chunk.Max),
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
			log.Info().Str("path", cfg.PlanOut).Msg("dry-run: would write plan snapshot")
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

	for pass := 1; pass <= cfg.MaxMergePasses; pass++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		chunks, err := m.Chunks(ctx, meta, cfg.Namespace)
		if err != nil {
			return fmt.Errorf("reload chunks: %w", err)
		}

		progress := false
		for i := 0; i < len(chunks)-1; i++ {
			left := chunks[i]
			right := chunks[i+1]

			leftSize, err := ensureChunkMetrics(ctx, m, cfg, meta, left, maxChunkBytes, metrics)
			if err != nil {
				return err
			}
			rightSize, err := ensureChunkMetrics(ctx, m, cfg, meta, right, maxChunkBytes, metrics)
			if err != nil {
				return err
			}

			combined := leftSize.Bytes + rightSize.Bytes
			if combined > maxChunkBytes {
				continue
			}

			targetShard := left.Shard
			moveChunk := Chunk{}
			needsMove := left.Shard != right.Shard
			if needsMove {
				if !cfg.AllowMoves || (hasZones && !cfg.AllowZonedMoves) {
					continue
				}
				if leftSize.Bytes > rightSize.Bytes {
					targetShard = left.Shard
					moveChunk = right
				} else {
					targetShard = right.Shard
					moveChunk = left
				}

				log.Info().
					Int("pass", pass).
					Str("action", "moveRange").
					Str("from", moveChunk.Shard).
					Str("to", targetShard).
					Str("bytes", metricMiB(metricBytes(moveChunk, left, right, leftSize, rightSize))).
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
			metrics[chunkKey(left.Min, right.Max)] = ChunkMetrics{Bytes: combined}
			delete(metrics, chunkKey(left.Min, left.Max))
			delete(metrics, chunkKey(right.Min, right.Max))
			merges++
			progress = true
			break
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

		metric, err := ensureChunkMetrics(ctx, m, cfg, meta, chunk, maxChunkBytes, metrics)
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
		metric, err := ensureChunkMetrics(ctx, m, cfg, meta, chunk, maxChunkBytes, metrics)
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

func ensureChunkMetrics(
	ctx context.Context,
	m *Mongo,
	cfg config.Config,
	meta CollectionMetadata,
	chunk Chunk,
	maxChunkBytes int64,
	metrics map[string]ChunkMetrics,
) (ChunkMetrics, error) {
	key := chunkKey(chunk.Min, chunk.Max)
	if metric, ok := metrics[key]; ok {
		if metric.Bytes > 0 {
			return metric, nil
		}
	}

	metric, err := m.DataSize(ctx, cfg.Database, cfg.Namespace, meta.Key, chunk.Min, chunk.Max)
	if err != nil {
		return ChunkMetrics{}, fmt.Errorf("calculate chunk size %s: %w", key, err)
	}
	metric.Oversized = metric.Bytes > maxChunkBytes
	metric.MarkedJumbo = chunk.Jumbo
	metrics[key] = metric
	return metric, nil
}

func chunkKey(min, max bson.D) string {
	return strings.Join([]string{mustMarshalBounds(min), mustMarshalBounds(max)}, " -> ")
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
	msg := fmt.Sprintf("moveRange %s: %v", bounds, err)
	if isOrphanCleanupTimeout(err) {
		msg += "; MongoDB timed out deleting donor-side orphaned documents after the range transfer. This is a server-side migration cleanup limit, not the tool's sleep setting. Retry after the cluster settles, or rerun with -allow-moves=false to skip cross-shard merge preparation"
	}
	return fmt.Errorf("%s", msg)
}

func isOrphanCleanupTimeout(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Failed to delete orphaned") && strings.Contains(msg, "ExceededTimeLimit")
}

func metricBytes(candidate Chunk, left Chunk, right Chunk, leftSize ChunkMetrics, rightSize ChunkMetrics) int64 {
	if chunkKey(candidate.Min, candidate.Max) == chunkKey(left.Min, left.Max) {
		return leftSize.Bytes
	}
	return rightSize.Bytes
}

func metricMiB(bytes int64) string {
	return fmt.Sprintf("%.2fMiB", float64(bytes)/1024/1024)
}

// PreventAutoMerge disables the AutoMerger for a collection, splits a chunk
// at the given query boundary, moves the upper half to the target shard, and
// re-enables the AutoMerger. This prevents the AutoMerger from re-merging the
// two halves because they no longer reside on the same shard.
func PreventAutoMerge(ctx context.Context, cfg config.Config) error {
	m, err := Connect(ctx, cfg.URI)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer m.Close(context.Background())

	if err := m.EnsureMongos(ctx); err != nil {
		return fmt.Errorf("connection must target mongos: %w", err)
	}

	buildInfo, err := m.BuildInfo(ctx)
	if err != nil {
		return fmt.Errorf("buildInfo: %w", err)
	}
	if len(buildInfo.VersionArray) >= 1 && buildInfo.VersionArray[0] < 7 {
		log.Warn().Str("version", buildInfo.Version).Msg("MongoDB version below the supported baseline; the tool only targets 7.0+")
	}

	// Lookup the chunk by _id.
	chunk, meta, err := m.ChunkByID(ctx, cfg.Namespace, cfg.ChunkID)
	if err != nil {
		return err
	}

	// Reject jumbo chunks — user must run the defrag pipeline first.
	if chunk.Jumbo {
		return fmt.Errorf("chunk %q is marked jumbo; run the defrag pipeline (phases 3/4) to clear or split it first", cfg.ChunkID)
	}

	// Reject hashed shard keys.
	if isHashedShardKey(meta.Key) {
		return fmt.Errorf("hashed shard key not supported for -prevent-automerge; use -chunk-query with a ranged shard key")
	}

	// Parse the chunk query.
	query, err := parseChunkQuery(cfg.ChunkQuery)
	if err != nil {
		return fmt.Errorf("invalid -chunk-query: %w", err)
	}

	// Validate that the query's shard key fields match the collection key.
	if err := validateQueryKey(query, meta.Key); err != nil {
		return fmt.Errorf("-chunk-query does not match shard key: %w", err)
	}

	if chunk.Shard == cfg.TargetShard {
		return fmt.Errorf("-target-shard %q is the same as the chunk's current shard %q; must be a different shard", cfg.TargetShard, chunk.Shard)
	}

	// Warn about zones if present.
	hasZones, err := m.HasZones(ctx, cfg.Namespace)
	if err != nil {
		return fmt.Errorf("check zones: %w", err)
	}
	if hasZones {
		log.Warn().Str("targetShard", cfg.TargetShard).Msg("collection has zone/tag metadata; verify target shard is consistent")
	}

	// Build and display the plan.
	plan := buildPreventPlan(cfg, chunk, meta, query)
	if !cfg.AutoApprove {
		if err := confirmAction(plan); err != nil {
			return err
		}
	} else {
		for _, line := range strings.Split(plan, "\n") {
			log.Info().Msg(line)
		}
	}

	// On any exit, re-enable the AutoMerger.
	var autoMergerReEnabled bool
	defer func() {
		if !autoMergerReEnabled {
			log.Warn().Str("namespace", cfg.Namespace).Msg("re-enabling AutoMerger due to error/exit")
			if err := m.ConfigureAutoMerger(context.Background(), cfg.Namespace, true); err != nil {
				log.Error().Err(err).Str("namespace", cfg.Namespace).Msg("failed to re-enable AutoMerger; manual intervention required")
			}
		}
	}()

	// Read dynamic autoMergerIntervalSecs.
	autoMergerSecs, err := m.AutoMergerIntervalSecs(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("could not read autoMergerIntervalSecs, defaulting to 10s")
		autoMergerSecs = 10
	}

	// Disable the AutoMerger.
	log.Info().Str("namespace", cfg.Namespace).Msg("disabling AutoMerger")
	if err := m.ConfigureAutoMerger(ctx, cfg.Namespace, false); err != nil {
		return fmt.Errorf("disable AutoMerger: %w", err)
	}

	// Wait for any in-flight AutoMerger cycle to drain.
	log.Info().Int64("seconds", autoMergerSecs).Msg("waiting for in-flight AutoMerger cycle to drain")
	sleepContext(ctx, time.Duration(autoMergerSecs)*time.Second)

	// Split the chunk at the query boundary.
	log.Info().Str("chunkID", cfg.ChunkID).Str("query", cfg.ChunkQuery).Msg("splitting chunk")
	if err := m.SplitAt(ctx, cfg.Namespace, query); err != nil {
		return fmt.Errorf("split chunk %s: %w", cfg.ChunkID, err)
	}

	// Move the upper half to the target shard with forceJumbo.
	log.Info().Str("query", cfg.ChunkQuery).Str("target", cfg.TargetShard).Msg("moving chunk to target shard (forceJumbo)")
	if err := m.MoveRangeWithFind(ctx, cfg.Namespace, query, cfg.TargetShard, true); err != nil {
		return fmt.Errorf("moveRange to %s: %w", cfg.TargetShard, err)
	}

	autoMergerReEnabled = true
	log.Info().Str("namespace", cfg.Namespace).Msg("re-enabling AutoMerger")
	if err := m.ConfigureAutoMerger(ctx, cfg.Namespace, true); err != nil {
		return fmt.Errorf("re-enable AutoMerger: %w", err)
	}

	log.Info().Str("chunkID", cfg.ChunkID).Str("targetShard", cfg.TargetShard).Msg("prevent-automerge complete")
	return nil
}

// SplitAt splits a chunk at the document identified by find.
func (m *Mongo) SplitAt(ctx context.Context, ns string, find bson.D) error {
	cmd := bson.D{
		{Key: "split", Value: ns},
		{Key: "find", Value: find},
	}
	return m.client.Database("admin").RunCommand(ctx, cmd).Err()
}

// parseChunkQuery parses a -chunk-query argument.
// Accepted formats:
//   - JSON object:  {"sk": 15000}
//   - key=value:    sk=15000
//   - compound:     a=15000,b=hello
func parseChunkQuery(raw string) (bson.D, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("query cannot be empty")
	}

	// JSON object format.
	if strings.HasPrefix(raw, "{") {
		var doc bson.D
		err := bson.UnmarshalExtJSON([]byte(raw), false, &doc)
		if err != nil {
			return nil, fmt.Errorf("parse as JSON: %w", err)
		}
		if len(doc) == 0 {
			return nil, fmt.Errorf("parsed query is empty")
		}
		return doc, nil
	}

	// key=value or key=value,key=value format.
	parts := strings.Split(raw, ",")
	doc := make(bson.D, 0, len(parts))
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid format %q: expected key=value or JSON object", raw)
		}
		key := strings.TrimSpace(kv[0])
		valStr := strings.TrimSpace(kv[1])
		if key == "" {
			return nil, fmt.Errorf("empty key in %q", part)
		}

		// Try int64.
		if n, err := strconv.ParseInt(valStr, 10, 64); err == nil {
			doc = append(doc, bson.E{Key: key, Value: n})
			continue
		}
		// Try float64.
		if f, err := strconv.ParseFloat(valStr, 64); err == nil {
			doc = append(doc, bson.E{Key: key, Value: f})
			continue
		}
		// Fallback to string.
		doc = append(doc, bson.E{Key: key, Value: valStr})
	}
	return doc, nil
}

// validateQueryKey checks that the query document has the same fields as the
// shard key pattern. It does not validate bounds — MongoDB rejects out-of-range
// splits at command time.
func validateQueryKey(query, shardKey bson.D) error {
	if len(query) != len(shardKey) {
		return fmt.Errorf("query has %d field(s) but shard key has %d", len(query), len(shardKey))
	}

	queryKeys := make(map[string]bool, len(query))
	for _, elem := range query {
		queryKeys[elem.Key] = true
	}

	for _, elem := range shardKey {
		if !queryKeys[elem.Key] {
			return fmt.Errorf("query is missing shard key field %q", elem.Key)
		}
	}
	return nil
}

// buildPreventPlan builds a human-readable plan summary.
func buildPreventPlan(cfg config.Config, chunk chunkDocument, meta CollectionMetadata, query bson.D) string {
	var b strings.Builder
	b.WriteString("Prevent AutoMerger - Planned Action")
	b.WriteString(fmt.Sprintf("\n  Namespace:      %s", cfg.Namespace))
	b.WriteString(fmt.Sprintf("\n  Chunk ID:       %s", cfg.ChunkID))
	b.WriteString(fmt.Sprintf("\n  Current shard:  %s", chunk.Shard))
	b.WriteString(fmt.Sprintf("\n  Target shard:   %s", cfg.TargetShard))
	b.WriteString(fmt.Sprintf("\n  Shard key:      %s", mustMarshalBounds(meta.Key)))
	b.WriteString(fmt.Sprintf("\n  Chunk bounds:   %s -> %s", mustMarshalBounds(chunk.Min), mustMarshalBounds(chunk.Max)))
	b.WriteString(fmt.Sprintf("\n  Split at:       %s", cfg.ChunkQuery))
	b.WriteString(fmt.Sprintf("\n  Actions:"))
	b.WriteString(fmt.Sprintf("\n    1. Disable AutoMerger for this collection"))
	b.WriteString(fmt.Sprintf("\n    2. Wait for in-flight AutoMerger cycle to drain"))
	b.WriteString(fmt.Sprintf("\n    3. Split chunk at the query boundary"))
	b.WriteString(fmt.Sprintf("\n    4. Move the upper half to %s with forceJumbo", cfg.TargetShard))
	b.WriteString(fmt.Sprintf("\n    5. Re-enable AutoMerger"))
	return b.String()
}

// confirmAction prints a plan and prompts the user for approval.
func confirmAction(plan string) error {
	log.Warn().Msg("--- Planned Action ---")
	for _, line := range strings.Split(plan, "\n") {
		log.Warn().Msg(line)
	}
	log.Warn().Msg("")
	log.Warn().Msg("Type 'yes' to confirm, or press Ctrl-C to abort:")

	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return fmt.Errorf("aborted by user")
	}
	input := strings.TrimSpace(strings.ToLower(scanner.Text()))
	if input != "yes" {
		return fmt.Errorf("aborted by user")
	}
	return nil
}
