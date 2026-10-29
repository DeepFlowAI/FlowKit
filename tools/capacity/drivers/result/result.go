// Package result defines the JSON output shared by capacity drivers.
package result

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// Payload is emitted as __CAPACITY_RESULT__=<json>.
type Payload struct {
	Driver       string         `json:"driver"`
	TargetN      int            `json:"target_n"`
	Completed    int            `json:"completed"`
	Failed       int            `json:"failed"`
	SuccessRate  float64        `json:"success_rate"`
	SetupP50Ms   float64        `json:"setup_p50_ms"`
	SetupP99Ms   float64        `json:"setup_p99_ms"`
	AvgSetupMs   float64        `json:"avg_setup_ms"`
	StatusCounts map[string]int `json:"status_counts"`
	Errors       []string       `json:"errors"`
	WallSeconds  float64        `json:"wall_seconds"`
	SetupMs      []float64      `json:"-"`
}

func Emit(p Payload) {
	if p.TargetN > 0 {
		p.SuccessRate = float64(p.Completed) / float64(p.TargetN)
	}
	p.SetupP50Ms = Percentile(p.SetupMs, 0.50)
	p.SetupP99Ms = Percentile(p.SetupMs, 0.99)
	p.AvgSetupMs = Average(p.SetupMs)
	raw, _ := json.Marshal(p)
	fmt.Printf("__CAPACITY_RESULT__=%s\n", raw)
	if p.Completed < p.TargetN {
		os.Exit(1)
	}
}

type Collector struct {
	TargetN      int
	SetupMs      []float64
	StatusCounts map[string]int
	Errors       []string
}

func (c *Collector) AddSetup(ms float64) {
	c.SetupMs = append(c.SetupMs, ms)
}

func (c *Collector) AddError(msg string) {
	c.Errors = append(c.Errors, msg)
	if c.StatusCounts == nil {
		c.StatusCounts = map[string]int{}
	}
	c.StatusCounts["error"]++
}

func (c *Collector) AddOK() {
	if c.StatusCounts == nil {
		c.StatusCounts = map[string]int{}
	}
	c.StatusCounts["200"]++
}

func Percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	cp := append([]float64(nil), values...)
	sort.Float64s(cp)
	idx := int(float64(len(cp)-1) * p)
	if idx < 0 {
		idx = 0
	}
	return cp[idx]
}

func Average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func UniqueStrings(in []string, max int) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
		if len(out) >= max {
			break
		}
	}
	return out
}
