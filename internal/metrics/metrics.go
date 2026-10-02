// Package metrics is a small Prometheus text-format registry with counters,
// gauges and fixed-bucket histograms.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

var buckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5}

type histogram struct {
	counts []uint64
	sum    float64
	n      uint64
}

// Registry is safe for concurrent use.
type Registry struct {
	mu     sync.Mutex
	values map[string]float64
	hists  map[string]*histogram
}

func NewRegistry() *Registry {
	return &Registry{values: map[string]float64{}, hists: map[string]*histogram{}}
}

func (r *Registry) Inc(name string) { r.Add(name, 1) }

func (r *Registry) Add(name string, v float64) {
	r.mu.Lock()
	r.values[name] += v
	r.mu.Unlock()
}

func (r *Registry) Set(name string, v float64) {
	r.mu.Lock()
	r.values[name] = v
	r.mu.Unlock()
}

func (r *Registry) Get(name string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.values[name]
}

// Observe records a value in seconds into a histogram.
func (r *Registry) Observe(name string, v float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.hists[name]
	if !ok {
		h = &histogram{counts: make([]uint64, len(buckets))}
		r.hists[name] = h
	}
	for i, b := range buckets {
		if v <= b {
			h.counts[i]++
		}
	}
	h.sum += v
	h.n++
}

// Write emits every metric in Prometheus text exposition format.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.values))
	for k := range r.values {
		names = append(names, k)
	}
	sort.Strings(names)
	typed := map[string]bool{}
	for _, k := range names {
		base := k
		if i := strings.IndexByte(k, '{'); i >= 0 {
			base = k[:i]
		}
		if !typed[base] {
			kind := "gauge"
			if strings.HasSuffix(base, "_total") {
				kind = "counter"
			}
			fmt.Fprintf(w, "# TYPE %s %s\n", base, kind)
			typed[base] = true
		}
		fmt.Fprintf(w, "%s %g\n", k, r.values[k])
	}
	hn := make([]string, 0, len(r.hists))
	for k := range r.hists {
		hn = append(hn, k)
	}
	sort.Strings(hn)
	for _, k := range hn {
		h := r.hists[k]
		fmt.Fprintf(w, "# TYPE %s histogram\n", k)
		for i, b := range buckets {
			fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", k, b, h.counts[i])
		}
		fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %g\n%s_count %d\n", k, h.n, k, h.sum, k, h.n)
	}
}
