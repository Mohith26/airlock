// Package control is the replicated control-plane state machine. Every
// replica applies the same committed Raft entries in the same order, so every
// replica holds identical state. All commands are idempotent: re-submitting a
// job or re-sending an acknowledgement never changes the outcome twice.
package control

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/Mohith26/airlock/internal/bundle"
)

// Job statuses.
const (
	Pending    = "pending"
	Succeeded  = "succeeded"
	Rejected   = "rejected"
	Superseded = "superseded"
	Invalid    = "invalid"
)

// Command is the payload of one Raft log entry.
type Command struct {
	Op       string           `json:"op"` // "release", "deploy", "ack"
	Manifest *bundle.Manifest `json:"manifest,omitempty"`
	Artifact []byte           `json:"artifact,omitempty"` // optional: replicate small artifacts through the log
	JobID    string           `json:"job_id,omitempty"`
	Region   string           `json:"region,omitempty"`
	Service  string           `json:"service,omitempty"`
	Version  string           `json:"version,omitempty"`
	Status   string           `json:"status,omitempty"`
	Detail   string           `json:"detail,omitempty"`
	At       int64            `json:"at_ms"`
}

func (c Command) Encode() []byte {
	b, _ := json.Marshal(c)
	return b
}

// Job is one deployment instruction for one region.
type Job struct {
	ID          string `json:"id"`
	Region      string `json:"region"`
	Service     string `json:"service"`
	Version     string `json:"version"`
	Generation  uint64 `json:"generation"`
	Status      string `json:"status"`
	Detail      string `json:"detail,omitempty"`
	SubmittedAt int64  `json:"submitted_at_ms"`
	CompletedAt int64  `json:"completed_at_ms,omitempty"`
	Index       uint64 `json:"log_index"`
}

// Region is the desired and last-confirmed state of one region.
type Region struct {
	Name             string `json:"name"`
	DesiredVersion   string `json:"desired_version"`
	DesiredGen       uint64 `json:"desired_generation"`
	ConfirmedVersion string `json:"confirmed_version"`
	ConfirmedGen     uint64 `json:"confirmed_generation"`
}

// Result describes what applying one entry did.
type Result struct {
	Op        string
	JobID     string
	Duplicate bool
	Status    string
	Err       error
}

// FSM is safe for concurrent reads while entries are applied.
type FSM struct {
	mu       sync.RWMutex
	releases map[string]bundle.Manifest // "service@version"
	blobs    map[string][]byte          // digest -> artifact, when carried in the log
	jobs     map[string]*Job
	byRegion map[string][]*Job // in generation order
	regions  map[string]*Region
	applied  uint64

	DuplicateSubmits uint64
	DuplicateAcks    uint64
}

func New() *FSM {
	return &FSM{releases: map[string]bundle.Manifest{}, blobs: map[string][]byte{}, jobs: map[string]*Job{}, byRegion: map[string][]*Job{}, regions: map[string]*Region{}}
}

func releaseKey(service, version string) string { return service + "@" + version }

// Apply executes one committed entry. Entries with no data (leader no-ops)
// only advance the applied index.
func (f *FSM) Apply(index uint64, data []byte) Result {
	f.mu.Lock()
	defer f.mu.Unlock()
	if index <= f.applied {
		return Result{Duplicate: true}
	}
	f.applied = index
	if len(data) == 0 {
		return Result{Op: "noop"}
	}
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return Result{Err: fmt.Errorf("decode command: %w", err)}
	}
	switch c.Op {
	case "release":
		if c.Manifest == nil {
			return Result{Op: c.Op, Err: fmt.Errorf("release without manifest")}
		}
		k := releaseKey(c.Manifest.Service, c.Manifest.Version)
		if _, ok := f.releases[k]; ok {
			return Result{Op: c.Op, Duplicate: true}
		}
		if len(c.Artifact) > 0 {
			if bundle.Digest(c.Artifact) != c.Manifest.Digest {
				return Result{Op: c.Op, Err: fmt.Errorf("artifact does not match manifest digest")}
			}
			f.blobs[c.Manifest.Digest] = c.Artifact
		}
		f.releases[k] = *c.Manifest
		return Result{Op: c.Op}
	case "deploy":
		if existing, ok := f.jobs[c.JobID]; ok {
			f.DuplicateSubmits++
			return Result{Op: c.Op, JobID: c.JobID, Duplicate: true, Status: existing.Status}
		}
		r := f.region(c.Region)
		j := &Job{ID: c.JobID, Region: c.Region, Service: c.Service, Version: c.Version, SubmittedAt: c.At, Index: index}
		if _, ok := f.releases[releaseKey(c.Service, c.Version)]; !ok {
			j.Status, j.Detail, j.CompletedAt = Invalid, "unknown release", c.At
		} else {
			r.DesiredGen++
			j.Generation = r.DesiredGen
			j.Status = Pending
			r.DesiredVersion = c.Version
		}
		f.jobs[j.ID] = j
		f.byRegion[c.Region] = append(f.byRegion[c.Region], j)
		return Result{Op: c.Op, JobID: j.ID, Status: j.Status}
	case "ack":
		j, ok := f.jobs[c.JobID]
		if !ok {
			return Result{Op: c.Op, JobID: c.JobID, Err: fmt.Errorf("ack for unknown job")}
		}
		if j.Status != Pending {
			f.DuplicateAcks++
			return Result{Op: c.Op, JobID: c.JobID, Duplicate: true, Status: j.Status}
		}
		switch c.Status {
		case Succeeded, Rejected, Superseded:
		default:
			return Result{Op: c.Op, JobID: c.JobID, Err: fmt.Errorf("bad status %q", c.Status)}
		}
		j.Status, j.Detail, j.CompletedAt = c.Status, c.Detail, c.At
		if c.Status == Succeeded {
			r := f.region(j.Region)
			if j.Generation > r.ConfirmedGen {
				r.ConfirmedGen, r.ConfirmedVersion = j.Generation, j.Version
			}
		}
		return Result{Op: c.Op, JobID: c.JobID, Status: j.Status}
	}
	return Result{Op: c.Op, Err: fmt.Errorf("unknown op %q", c.Op)}
}

func (f *FSM) region(name string) *Region {
	r, ok := f.regions[name]
	if !ok {
		r = &Region{Name: name}
		f.regions[name] = r
	}
	return r
}

// Pending returns a region's unfinished jobs in generation order.
func (f *FSM) Pending(region string) []Job {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var out []Job
	for _, j := range f.byRegion[region] {
		if j.Status == Pending {
			out = append(out, *j)
		}
	}
	return out
}

// Release looks up a registered manifest.
func (f *FSM) Release(service, version string) (bundle.Manifest, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	m, ok := f.releases[releaseKey(service, version)]
	return m, ok
}

// Artifact returns artifact bytes replicated through the log.
func (f *FSM) Artifact(digest string) ([]byte, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	b, ok := f.blobs[digest]
	return b, ok
}

func (f *FSM) Job(id string) (Job, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	j, ok := f.jobs[id]
	if !ok {
		return Job{}, false
	}
	return *j, true
}

func (f *FSM) Regions() []Region {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]Region, 0, len(f.regions))
	for _, r := range f.regions {
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (f *FSM) Applied() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.applied
}

// Counts tallies jobs by status.
func (f *FSM) Counts() map[string]int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	c := map[string]int{}
	for _, j := range f.jobs {
		c[j.Status]++
	}
	return c
}

// Jobs returns every job, ordered by log index.
func (f *FSM) Jobs() []Job {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]Job, 0, len(f.jobs))
	for _, j := range f.jobs {
		out = append(out, *j)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out
}

// Digest is a hash of the full replicated state, used to prove that every
// replica converged to exactly the same result.
func (f *FSM) Digest() string {
	jobs := f.Jobs()
	regions := f.Regions()
	f.mu.RLock()
	keys := make([]string, 0, len(f.releases))
	for k := range f.releases {
		keys = append(keys, k)
	}
	f.mu.RUnlock()
	sort.Strings(keys)
	b, _ := json.Marshal(struct {
		R []string
		J []Job
		G []Region
	}{keys, jobs, regions})
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
