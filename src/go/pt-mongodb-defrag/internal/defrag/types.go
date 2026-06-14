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
	"go.mongodb.org/mongo-driver/bson"
)

type BuildInfo struct {
	Version      string  `bson:"version"`
	VersionArray []int32 `bson:"versionArray"`
}

type CollectionMetadata struct {
	ID   string `bson:"_id"`
	UUID any    `bson:"uuid"`
	Key  bson.D `bson:"key"`
}

type Chunk struct {
	Min   bson.D `bson:"min"`
	Max   bson.D `bson:"max"`
	Shard string `bson:"shard"`
	Jumbo bool   `bson:"jumbo,omitempty"`
}

type ChunkMetrics struct {
	Bytes       int64 `json:"bytes"`
	Documents   int64 `json:"documents"`
	Millis      int64 `json:"millis"`
	Estimated   bool  `json:"estimated,omitempty"`
	Oversized   bool  `json:"oversized"`
	MarkedJumbo bool  `json:"marked_jumbo"`
}

type Snapshot struct {
	Namespace      string          `json:"namespace"`
	CollectedAt    string          `json:"collected_at"`
	ChunkSizeBytes int64           `json:"chunk_size_bytes"`
	ShardKey       bson.D          `json:"shard_key"`
	Chunks         []SnapshotChunk `json:"chunks"`
}

type SnapshotChunk struct {
	Min       string `json:"min"`
	Max       string `json:"max"`
	Shard     string `json:"shard"`
	Jumbo     bool   `json:"jumbo"`
	Bytes     int64  `json:"bytes"`
	Documents int64  `json:"documents"`
}
