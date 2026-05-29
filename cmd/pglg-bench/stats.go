package main

import (
	"fmt"
	"math"
	"sort"
)

func percentile(samples []int64, p float64) int64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := make([]int64, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func maxInt64(samples []int64) int64 {
	var m int64
	for _, s := range samples {
		if s > m {
			m = s
		}
	}
	return m
}

func humanizeBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func bounded(samples []int64) bool {
	if len(samples) < 2 {
		return true
	}
	med := percentile(samples, 50)
	final := samples[len(samples)-1]
	if med == 0 {
		return final == 0
	}
	return final <= 2*med
}
