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

import "testing"

func TestParsePhases(t *testing.T) {
	phases, err := ParsePhases("1, 3,4")
	if err != nil {
		t.Fatalf("ParsePhases returned error: %v", err)
	}
	if !phases[1] || !phases[3] || !phases[4] {
		t.Fatalf("expected phases 1,3,4 to be enabled: %#v", phases)
	}
	if phases[2] {
		t.Fatalf("did not expect phase 2 to be enabled")
	}
}

func TestParseHumanBytes(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{input: "1K", want: 1024},
		{input: "1M", want: 1024 * 1024},
		{input: "1G", want: 1024 * 1024 * 1024},
		{input: "64", want: 64},
	}

	for _, tc := range tests {
		got, err := ParseHumanBytes(tc.input)
		if err != nil {
			t.Fatalf("ParseHumanBytes(%q) returned error: %v", tc.input, err)
		}
		if got != tc.want {
			t.Fatalf("ParseHumanBytes(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestValidateRejectsNegativeSplitMergeSleep(t *testing.T) {
	cfg := Config{
		Namespace:       "db.coll",
		MaxMergePasses:  1,
		EnabledPhases:   map[int]bool{1: true},
		SplitMergeSleep: -1,
	}

	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error for negative split merge sleep")
	}
}


