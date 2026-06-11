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
	"errors"
	"strings"
	"testing"

	"github.com/percona/percona-toolkit/src/go/pt-mongodb-defrag/internal/config"
	"go.mongodb.org/mongo-driver/bson"
)

func TestIsHashedShardKey(t *testing.T) {
	if !isHashedShardKey(bson.D{{Key: "_id", Value: "hashed"}}) {
		t.Fatalf("expected hashed shard key to be detected")
	}
	if isHashedShardKey(bson.D{{Key: "_id", Value: 1}}) {
		t.Fatalf("did not expect ranged shard key to be detected as hashed")
	}
}

func TestChunkKeyStable(t *testing.T) {
	min := bson.D{{Key: "_id", Value: 0}}
	max := bson.D{{Key: "_id", Value: 10}}
	if chunkKey(min, max) != chunkKey(min, max) {
		t.Fatalf("expected chunk key to be stable")
	}
}

func TestIsOrphanCleanupTimeout(t *testing.T) {
	err := errors.New("Data transfer error: ExceededTimeLimit: Failed to delete orphaned pt_defrag.orders range [{ sk: 15000 }, { sk: 16000 }) :: caused by :: operation exceeded time limit")
	if !isOrphanCleanupTimeout(err) {
		t.Fatalf("expected orphan cleanup timeout to be detected")
	}
}

func TestParseChunkQueryJSON(t *testing.T) {
	doc, err := parseChunkQuery(`{"sk": 15000}`)
	if err != nil {
		t.Fatalf("parseChunkQuery returned error: %v", err)
	}
	if len(doc) != 1 {
		t.Fatalf("expected 1 element, got %d", len(doc))
	}
	if doc[0].Key != "sk" {
		t.Fatalf("expected key 'sk', got %q", doc[0].Key)
	}
}

func TestParseChunkQueryKeyValue(t *testing.T) {
	doc, err := parseChunkQuery("sk=15000")
	if err != nil {
		t.Fatalf("parseChunkQuery returned error: %v", err)
	}
	if len(doc) != 1 {
		t.Fatalf("expected 1 element, got %d", len(doc))
	}
	if doc[0].Key != "sk" {
		t.Fatalf("expected key 'sk', got %q", doc[0].Key)
	}
	v, ok := doc[0].Value.(int64)
	if !ok || v != 15000 {
		t.Fatalf("expected value 15000 (int64), got %v (%T)", doc[0].Value, doc[0].Value)
	}
}

func TestParseChunkQueryCompound(t *testing.T) {
	doc, err := parseChunkQuery("a=100,b=hello")
	if err != nil {
		t.Fatalf("parseChunkQuery returned error: %v", err)
	}
	if len(doc) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(doc))
	}
	if doc[0].Key != "a" || doc[1].Key != "b" {
		t.Fatalf("expected keys 'a','b', got %q,%q", doc[0].Key, doc[1].Key)
	}
}

func TestParseChunkQueryEmpty(t *testing.T) {
	_, err := parseChunkQuery("")
	if err == nil {
		t.Fatalf("expected error for empty query")
	}
}

func TestParseChunkQueryInvalidFormat(t *testing.T) {
	_, err := parseChunkQuery("justtext")
	if err == nil {
		t.Fatalf("expected error for invalid format")
	}
}

func TestParseChunkQueryStringValue(t *testing.T) {
	doc, err := parseChunkQuery("name=hello")
	if err != nil {
		t.Fatalf("parseChunkQuery returned error: %v", err)
	}
	v, ok := doc[0].Value.(string)
	if !ok || v != "hello" {
		t.Fatalf("expected string 'hello', got %v (%T)", doc[0].Value, doc[0].Value)
	}
}

func TestValidateQueryKeyMatch(t *testing.T) {
	query := bson.D{{Key: "sk", Value: 15000}}
	key := bson.D{{Key: "sk", Value: 1}}
	if err := validateQueryKey(query, key); err != nil {
		t.Fatalf("expected match, got error: %v", err)
	}
}

func TestValidateQueryKeyMismatch(t *testing.T) {
	query := bson.D{{Key: "name", Value: "hello"}}
	key := bson.D{{Key: "sk", Value: 1}}
	if err := validateQueryKey(query, key); err == nil {
		t.Fatalf("expected error for mismatched keys")
	}
}

func TestValidateQueryKeyMissingField(t *testing.T) {
	query := bson.D{{Key: "a", Value: 1}}
	key := bson.D{{Key: "a", Value: 1}, {Key: "b", Value: 1}}
	if err := validateQueryKey(query, key); err == nil {
		t.Fatalf("expected error for missing compound key field")
	}
}

func TestBuildPreventPlanContainsKeyDetails(t *testing.T) {
	cfg := config.Config{
		Namespace:   "db.coll",
		ChunkID:     "chunk_1",
		ChunkQuery:  "sk=15000",
		TargetShard: "shard02",
	}
	chunk := chunkDocument{
		ID:    "chunk_1",
		Min:   bson.D{{Key: "sk", Value: 0}},
		Max:   bson.D{{Key: "sk", Value: 1000}},
		Shard: "shard01",
	}
	meta := CollectionMetadata{
		Key: bson.D{{Key: "sk", Value: 1}},
	}
	query := bson.D{{Key: "sk", Value: 500}}

	plan := buildPreventPlan(cfg, chunk, meta, query)

	checks := []string{"db.coll", "chunk_1", "shard01", "shard02", "500"}
	for _, c := range checks {
		if !strings.Contains(plan, c) {
			t.Fatalf("plan missing %q", c)
		}
	}
}

func TestWrapMoveRangeErrorAddsGuidanceForOrphanCleanupTimeout(t *testing.T) {
	err := errors.New("Data transfer error: ExceededTimeLimit: Failed to delete orphaned pt_defrag.orders range [{ sk: 15000 }, { sk: 16000 }) :: caused by :: operation exceeded time limit")
	wrapped := wrapMoveRangeError(`{"sk":{"$numberInt":"15000"}} -> {"sk":{"$numberInt":"16000"}}`, err)
	msg := wrapped.Error()

	if !strings.Contains(msg, "MongoDB timed out deleting donor-side orphaned documents") {
		t.Fatalf("expected orphan cleanup guidance in wrapped error, got %q", msg)
	}
	if !strings.Contains(msg, "-allow-moves=false") {
		t.Fatalf("expected move disabling guidance in wrapped error, got %q", msg)
	}
}
