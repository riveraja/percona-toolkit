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

package config

import (
	"testing"
	"time"
)

func TestConfigValidateSamplePercent(t *testing.T) {
	tests := []struct {
		name         string
		samplePercent int
		wantErr      bool
	}{
		{name: "valid 0 percent", samplePercent: 0, wantErr: false},
		{name: "valid 1 percent", samplePercent: 1, wantErr: false},
		{name: "valid 10 percent", samplePercent: 10, wantErr: false},
		{name: "valid 100 percent", samplePercent: 100, wantErr: false},
		{name: "invalid negative", samplePercent: -1, wantErr: true},
		{name: "invalid over 100", samplePercent: 101, wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				URI:            "mongodb://localhost:27017",
				Namespace:      "test.orders",
				Sleep:          500 * time.Millisecond,
				SplitMergeSleep: 500 * time.Millisecond,
				MaxMergePasses:  1000,
				EnabledPhases:  map[int]bool{1: true, 2: true, 3: true, 4: true},
				SamplePercent:  tc.samplePercent,
			}
			err := cfg.Validate()
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for SamplePercent=%d, got nil", tc.samplePercent)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for SamplePercent=%d: %v", tc.samplePercent, err)
			}
		})
	}
}

func TestConfigValidateNamespace(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		wantErr   bool
	}{
		{name: "valid namespace", namespace: "db.collection", wantErr: false},
		{name: "invalid empty", namespace: "", wantErr: true},
		{name: "invalid no dot", namespace: "invalid", wantErr: true},
		{name: "invalid double dot", namespace: "db..collection", wantErr: true},
		{name: "invalid trailing dot", namespace: "db.", wantErr: true},
		{name: "invalid leading dot", namespace: ".collection", wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				URI:              "mongodb://localhost:27017",
				Namespace:        tc.namespace,
				Sleep:            500 * time.Millisecond,
				SplitMergeSleep:  500 * time.Millisecond,
				MaxMergePasses:   1000,
				EnabledPhases:    map[int]bool{1: true},
				MetadataTimeout:  30 * time.Second,
				CommandTimeout:   2 * time.Minute,
			}
			err := cfg.Validate()
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for namespace=%q, got nil", tc.namespace)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for namespace=%q: %v", tc.namespace, err)
			}
			if !tc.wantErr {
				if cfg.Database != "db" {
					t.Errorf("expected Database='db', got %q", cfg.Database)
				}
				if cfg.Collection != "collection" {
					t.Errorf("expected Collection='collection', got %q", cfg.Collection)
				}
			}
		})
	}
}

func TestParseHumanBytes(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{name: "bytes", input: "128", want: 128, wantErr: false},
		{name: "kilobytes", input: "128K", want: 128 * 1024, wantErr: false},
		{name: "megabytes", input: "128M", want: 128 * 1024 * 1024, wantErr: false},
		{name: "gigabytes", input: "1G", want: 1024 * 1024 * 1024, wantErr: false},
		{name: "lowercase", input: "128m", want: 128 * 1024 * 1024, wantErr: false},
		{name: "invalid empty", input: "", wantErr: true},
		{name: "invalid letters", input: "abc", wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseHumanBytes(tc.input)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for input=%q, got nil", tc.input)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for input=%q: %v", tc.input, err)
			}
			if !tc.wantErr && got != tc.want {
				t.Fatalf("ParseHumanBytes(%q)=%d, want %d", tc.input, got, tc.want)
			}
		})
	}
}
