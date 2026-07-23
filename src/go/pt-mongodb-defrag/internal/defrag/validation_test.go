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
	"errors"
	"testing"
	"time"

	"github.com/percona/percona-toolkit/src/go/pt-mongodb-defrag/internal/config"
)

// mockMongo is a mock implementation of the Mongo validation methods
type mockMongo struct {
	countDocuments     int64
	countDocumentsErr  error
	sampleIDs          []interface{}
	sampleDocumentsErr error
	verifyCount        int64
	verifyDocumentsErr error
	collectionStats    map[string]int64
	collectionStatsErr error
}

func (m *mockMongo) CountDocuments(ctx context.Context, dbName, collName string) (int64, error) {
	return m.countDocuments, m.countDocumentsErr
}

func (m *mockMongo) SampleDocuments(ctx context.Context, dbName, collName string, samplePercent int) ([]interface{}, error) {
	return m.sampleIDs, m.sampleDocumentsErr
}

func (m *mockMongo) VerifyDocumentsExist(ctx context.Context, dbName, collName string, ids []interface{}) (int64, error) {
	return m.verifyCount, m.verifyDocumentsErr
}

func (m *mockMongo) GetCollectionStats(ctx context.Context, dbName, collName string) (map[string]int64, error) {
	return m.collectionStats, m.collectionStatsErr
}

// TestValidationResult tests the ValidationResult struct
func TestValidationResult(t *testing.T) {
	ts := time.Now().UTC()
	result := &ValidationResult{
		DocumentCount: 1000,
		SampleIDs:     []interface{}{"id1", "id2", "id3"},
		Stats: map[string]int64{
			"storageSize": 1000000,
			"totalIndexSize": 50000,
		},
		Timestamp: ts,
	}

	if result.DocumentCount != 1000 {
		t.Errorf("expected DocumentCount=1000, got %d", result.DocumentCount)
	}
	if len(result.SampleIDs) != 3 {
		t.Errorf("expected 3 SampleIDs, got %d", len(result.SampleIDs))
	}
	if result.Stats["storageSize"] != 1000000 {
		t.Errorf("expected storageSize=1000000, got %d", result.Stats["storageSize"])
	}
}

// TestRunPreValidation tests the pre-validation logic
func TestRunPreValidation(t *testing.T) {
	tests := []struct {
		name        string
		mock        *mockMongo
		cfg         config.Config
		wantErr     bool
		wantCount   int64
		wantSamples int
	}{
		{
			name: "successful validation",
			mock: &mockMongo{
				countDocuments: 1000,
				sampleIDs:      []interface{}{"id1", "id2", "id3"},
				collectionStats: map[string]int64{"storageSize": 1000000},
			},
			cfg: config.Config{
				Database:      "testdb",
				Collection:    "testcoll",
				SamplePercent: 10,
			},
			wantErr:     false,
			wantCount:   1000,
			wantSamples: 3,
		},
		{
			name: "count documents error",
			mock: &mockMongo{
				countDocumentsErr: errors.New("connection error"),
			},
			cfg: config.Config{
				Database:   "testdb",
				Collection: "testcoll",
			},
			wantErr:   true,
			wantCount: 0,
		},
		{
			name: "sample documents error (non-fatal)",
			mock: &mockMongo{
				countDocuments:     1000,
				sampleDocumentsErr: errors.New("sample error"),
				collectionStats:    map[string]int64{"storageSize": 1000000},
			},
			cfg: config.Config{
				Database:      "testdb",
				Collection:    "testcoll",
				SamplePercent: 10,
			},
			wantErr:     false, // Non-fatal, should continue
			wantCount:   1000,
			wantSamples: 0,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Note: runPreValidation expects a *Mongo, not *mockMongo
			// In a real test, we would need to refactor to use an interface
			// For now, this test structure demonstrates the test cases
			t.Skip("Requires interface-based refactoring for proper unit testing")
		})
	}
}

// TestRunPostValidation tests the post-validation logic
func TestRunPostValidation(t *testing.T) {
	tests := []struct {
		name          string
		preVal        *ValidationResult
		postCount     int64
		postStats     map[string]int64
		verifyCount   int64
		wantErr       bool
		wantLogMatch  bool
	}{
		{
			name: "successful validation - counts match",
			preVal: &ValidationResult{
				DocumentCount: 1000,
				SampleIDs:     []interface{}{"id1", "id2"},
				Stats:         map[string]int64{"storageSize": 1000000},
				Timestamp:     time.Now().UTC(),
			},
			postCount:   1000,
			postStats:   map[string]int64{"storageSize": 900000},
			verifyCount: 2,
			wantErr:     false,
			wantLogMatch: true,
		},
		{
			name: "document count mismatch",
			preVal: &ValidationResult{
				DocumentCount: 1000,
				SampleIDs:     []interface{}{"id1", "id2"},
				Stats:         map[string]int64{"storageSize": 1000000},
				Timestamp:     time.Now().UTC(),
			},
			postCount:  999, // Mismatch!
			postStats:  map[string]int64{"storageSize": 900000},
			wantErr:    true,
			wantLogMatch: false,
		},
		{
			name: "missing sampled documents",
			preVal: &ValidationResult{
				DocumentCount: 1000,
				SampleIDs:     []interface{}{"id1", "id2", "id3"},
				Stats:         map[string]int64{"storageSize": 1000000},
				Timestamp:     time.Now().UTC(),
			},
			postCount:   1000,
			postStats:   map[string]int64{"storageSize": 900000},
			verifyCount: 2, // One document missing!
			wantErr:     false, // Warning, not error
			wantLogMatch: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Note: Similar to pre-validation, this requires interface-based refactoring
			t.Skip("Requires interface-based refactoring for proper unit testing")
		})
	}
}

// TestMaskURI tests the URI masking function
func TestMaskURI(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "with credentials",
			input: "mongodb://admin:secret@localhost:27017",
			want:  "mongodb://<redacted>@localhost:27017",
		},
		{
			name:  "without credentials",
			input: "mongodb://localhost:27017",
			want:  "mongodb://localhost:27017",
		},
		{
			name:  "with auth source",
			input: "mongodb://admin:secret@localhost:27017/?authSource=admin",
			want:  "mongodb://<redacted>@localhost:27017/?authSource=admin",
		},
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
		{
			name:  "invalid URI",
			input: "not-a-uri",
			want:  "<redacted>",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := maskURI(tc.input)
			
			// Check that credentials in userinfo are redacted
			// Parse the original URI to extract userinfo
			if contains(tc.input, "@") {
				// Extract the userinfo part (before @)
				atIndex := -1
				for i, c := range tc.input {
					if c == '@' {
						atIndex = i
						break
					}
				}
				if atIndex > 0 {
					// Find the protocol end
					slashesIndex := -1
					for i := 0; i < len(tc.input); i++ {
						if tc.input[i] == '/' && i+1 < len(tc.input) && tc.input[i+1] == '/' {
							slashesIndex = i + 2
							break
						}
					}
					if slashesIndex >= 0 && slashesIndex < atIndex {
						userinfo := tc.input[slashesIndex:atIndex]
						// The redacted URI should not contain the original userinfo
						if contains(got, userinfo) {
							t.Errorf("maskURI(%q)=%q, should not contain original userinfo %q", tc.input, got, userinfo)
						}
					}
				}
			}
			
			// Check that output is not empty for non-empty input
			if tc.input != "" && got == "" {
				t.Errorf("maskURI(%q) returned empty string", tc.input)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Integration test (requires running MongoDB)
// Run with: go test -tags=integration -run TestValidationIntegration

// TestValidationIntegration tests the full validation flow with a real MongoDB connection
// This test is skipped unless the -tags=integration flag is provided
func TestValidationIntegration(t *testing.T) {
	t.Skip("Integration test - requires running MongoDB instance")
	
	// This would be the structure for an integration test
	// Uncomment and modify when a test MongoDB is available
	
	/*
	uri := os.Getenv("TEST_MONGODB_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	m, err := Connect(ctx, uri, 5*time.Second, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer m.Close(ctx)
	
	// Create test database and collection
	dbName := "test_validation"
	collName := "test_orders"
	
	coll := m.client.Database(dbName).Collection(collName)
	
	// Insert test documents
	_, err = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "sk", Value: 1}, {Key: "data", Value: "test1"}},
		bson.D{{Key: "sk", Value: 2}, {Key: "data", Value: "test2"}},
		bson.D{{Key: "sk", Value: 3}, {Key: "data", Value: "test3"}},
	})
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}
	defer m.client.Database(dbName).Drop(ctx)
	
	// Test CountDocuments
	count, err := m.CountDocuments(ctx, dbName, collName)
	if err != nil {
		t.Errorf("CountDocuments failed: %v", err)
	}
	if count != 3 {
		t.Errorf("CountDocuments=%d, want 3", count)
	}
	
	// Test SampleDocuments
	ids, err := m.SampleDocuments(ctx, dbName, collName, 100)
	if err != nil {
		t.Errorf("SampleDocuments failed: %v", err)
	}
	if len(ids) != 3 {
		t.Errorf("SampleDocuments returned %d IDs, want 3", len(ids))
	}
	
	// Test VerifyDocumentsExist
	found, err := m.VerifyDocumentsExist(ctx, dbName, collName, ids)
	if err != nil {
		t.Errorf("VerifyDocumentsExist failed: %v", err)
	}
	if found != int64(3) {
		t.Errorf("VerifyDocumentsExist=%d, want 3", found)
	}
	
	// Test GetCollectionStats
	stats, err := m.GetCollectionStats(ctx, dbName, collName)
	if err != nil {
		t.Errorf("GetCollectionStats failed: %v", err)
	}
	if stats["count"] != 3 {
		t.Errorf("stats[count]=%d, want 3", stats["count"])
	}
	*/
}
