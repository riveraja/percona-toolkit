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
	if !errors.Is(wrapped, err) {
		t.Fatalf("expected wrapped error to preserve original error for errors.Is")
	}
}
