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
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	URI             string
	Namespace       string
	Database        string
	Collection      string
	ChunkSizeBytes  int64
	Sleep           time.Duration
	SplitMergeSleep time.Duration
	DryRun          bool
	AllowMoves      bool
	AllowZonedMoves bool
	MaxMergePasses  int
	PlanOut         string
	EnabledPhases   map[int]bool
	Quiet           bool
	MetadataTimeout time.Duration
	CommandTimeout  time.Duration
}

func (c *Config) Validate() error {
	parts := strings.Split(c.Namespace, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("invalid -namespace %q, expected database.collection", c.Namespace)
	}
	c.Database = parts[0]
	c.Collection = parts[1]

	if c.ChunkSizeBytes < 0 {
		return fmt.Errorf("chunk size must be >= 0")
	}
	if c.Sleep < 0 {
		return fmt.Errorf("sleep must be >= 0")
	}
	if c.SplitMergeSleep < 0 {
		return fmt.Errorf("split-merge-sleep must be >= 0")
	}
	if c.MaxMergePasses <= 0 {
		return fmt.Errorf("max-merge-passes must be > 0")
	}
	if len(c.EnabledPhases) == 0 {
		return fmt.Errorf("at least one phase must be enabled")
	}
	if c.MetadataTimeout < 0 {
		return fmt.Errorf("metadata-timeout must be >= 0")
	}
	if c.CommandTimeout < 0 {
		return fmt.Errorf("timeout must be >= 0")
	}
	return nil
}

func ParsePhases(raw string) (map[int]bool, error) {
	enabled := make(map[int]bool)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		switch part {
		case "1", "2", "3", "4":
			enabled[int(part[0]-'0')] = true
		case "":
		default:
			return nil, fmt.Errorf("unsupported phase %q", part)
		}
	}
	return enabled, nil
}

type HumanBytesValue struct {
	target *int64
	set    bool
}

func NewHumanBytesValue(target *int64) *HumanBytesValue {
	return &HumanBytesValue{target: target}
}

func (v *HumanBytesValue) Set(raw string) error {
	n, err := ParseHumanBytes(raw)
	if err != nil {
		return err
	}
	*v.target = n
	v.set = true
	return nil
}

func (v *HumanBytesValue) String() string {
	if v == nil || v.target == nil || *v.target == 0 {
		return ""
	}
	return FormatMiB(*v.target)
}

func ParseHumanBytes(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, fmt.Errorf("chunk size cannot be empty")
	}

	multiplier := int64(1)
	last := s[len(s)-1]
	switch last {
	case 'K', 'k':
		multiplier = 1024
		s = s[:len(s)-1]
	case 'M', 'm':
		multiplier = 1024 * 1024
		s = s[:len(s)-1]
	case 'G', 'g':
		multiplier = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	}

	value, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid chunk size %q", raw)
	}
	if value < 0 {
		return 0, fmt.Errorf("chunk size must be >= 0")
	}
	return value * multiplier, nil
}

func FormatMiB(bytes int64) string {
	return fmt.Sprintf("%.2fMiB", float64(bytes)/1024/1024)
}
