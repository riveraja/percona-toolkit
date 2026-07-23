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
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestIsCommandOK(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want bool
	}{
		{name: "int32 one", in: int32(1), want: true},
		{name: "int64 one", in: int64(1), want: true},
		{name: "float64 one", in: float64(1), want: true},
		{name: "bool true", in: true, want: true},
		{name: "int32 zero", in: int32(0), want: false},
		{name: "string one", in: "1", want: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := isCommandOK(tc.in); got != tc.want {
				t.Fatalf("isCommandOK(%v)=%v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestIsMongosResult(t *testing.T) {
	if !isMongosResult(bson.M{"isdbgrid": 1, "ok": 1}) {
		t.Fatalf("expected isdbgrid field to identify mongos")
	}
	if !isMongosResult(bson.M{"msg": "isdbgrid", "ok": 1}) {
		t.Fatalf("expected msg=isdbgrid to identify mongos")
	}
	if isMongosResult(bson.M{"msg": "not-mongos", "ok": 1}) {
		t.Fatalf("did not expect non-mongos result to pass")
	}
}

func TestMarshalBoundsErrorForUnsupportedType(t *testing.T) {
	_, err := marshalBounds(bson.D{{Key: "_id", Value: func() {}}})
	if err == nil {
		t.Fatalf("expected marshalBounds to fail for unsupported BSON value")
	}
}
