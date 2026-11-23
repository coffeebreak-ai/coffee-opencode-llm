package engine

import (
	"strings"
	"testing"
)

func TestDeflateCacheKeyIncludesBudget(t *testing.T) {
	text := "fragment"
	if deflateCacheKey(text, "request", 12000) == deflateCacheKey(text, "request", 6000) {
		t.Fatal("a different budget must produce a different key")
	}
	if deflateCacheKey(text, "request-a", 12000) == deflateCacheKey(text, "request-b", 12000) {
		t.Fatal("a different compression subject must produce a different key")
	}
}

func TestDeflateNeeded(t *testing.T) {
	tests := []struct {
		name      string
		size      int
		threshold int64
		want      bool
	}{
		{name: "below threshold", size: 500, threshold: 2000, want: false},
		{name: "equal to threshold", size: 2000, threshold: 2000, want: false},
		{name: "above threshold", size: 2001, threshold: 2000, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			text := strings.Repeat("x", test.size)
			if got := deflateNeeded(text, test.threshold); got != test.want {
				t.Fatalf("deflateNeeded(size=%d, threshold=%d) = %v, want %v",
					test.size, test.threshold, got, test.want)
			}
		})
	}
}

func TestDeflatorTokenLimit(t *testing.T) {
	tests := []struct {
		name       string
		output     int64
		reserve    int64
		modelLimit int
		want       int64
	}{
		{name: "output plus reasoning reserve", output: 4000, reserve: 4096, modelLimit: 128000, want: 8096},
		{name: "model output cap", output: 12000, reserve: 1000, modelLimit: 4000, want: 4000},
		{name: "no reserve", output: 12000, modelLimit: 128000, want: 12000},
		{name: "minimum token", modelLimit: 10, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := deflatorTokenLimit(test.output, test.reserve, test.modelLimit); got != test.want {
				t.Fatalf("deflatorTokenLimit(%d, %d, %d) = %d, want %d",
					test.output, test.reserve, test.modelLimit, got, test.want)
			}
		})
	}
}
