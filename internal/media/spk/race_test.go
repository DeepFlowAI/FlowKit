//go:build race

package spk

// The race detector instruments the Go-side filterbank heavily, so wall-clock
// budgets measured under it say nothing about production.
const raceEnabled = true
