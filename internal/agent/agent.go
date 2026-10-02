// Package agent is the per-region deployment agent. It pulls pending jobs
// from the control plane when it can reach it, or imports signed offline
// packages when it cannot. It verifies every artifact locally against keys it
// was provisioned with, records completion in a local ledger before
// acknowledging, and buffers acknowledgements while disconnected.
package agent

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/Mohith26/airlock/internal/bundle"
	"github.com/Mohith26/airlock/internal/control"
)

// ControlPlane is everything an agent needs from the control plane.
type ControlPlane interface {
	Pending(region string) ([]control.Job, error)
	Release(service, version string) (bundle.Manifest, error)
	Artifact(digest string) ([]byte, error)
	Ack(jobID, status, detail string) error
}

// ErrUnreachable signals a network failure between agent and control plane.
var ErrUnreachable = errors.New("control plane unreachable")

// Installer applies a verified artifact. It must be idempotent for the same
// (service, version), the way package managers and image pulls are.
type Installer func(m bundle.Manifest, artifact []byte) error

// Ack is a buffered acknowledgement.
type Ack struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

// Stats counts what the agent did.
type Stats struct {
	Executed            uint64 `json:"executed"`
	DuplicateDeliveries uint64 `json:"duplicate_deliveries_suppressed"`
	Superseded          uint64 `json:"superseded"`
	ArtifactsRejected   uint64 `json:"artifacts_rejected"`
	Refetched           uint64 `json:"clean_refetches"`
	PackagesImported    uint64 `json:"packages_imported"`
	PackagesRejected    uint64 `json:"packages_rejected"`
	StaleRejected       uint64 `json:"stale_generations_rejected"`
	Crashes             uint64 `json:"worker_crashes"`
	AcksSent            uint64 `json:"acks_sent"`
}

// Event reports something notable to the caller (timeline, audit log).
type Event struct {
	Type    string
	Details map[string]string
}

// Config configures an agent.
type Config struct {
	Region      string
	ReleaseKeys bundle.Keyring
	PackageKeys bundle.Keyring // control-plane keys for offline packages
	Ledger      Ledger
	Install     Installer
	OnEvent     func(Event)
}

// Agent is safe for concurrent use; Sync and Import serialize internally.
type Agent struct {
	mu  sync.Mutex
	cfg Config

	mirror     map[string][]byte // content-addressed artifact cache
	outbox     []Ack
	version    string
	generation uint64
	pkgSeq     uint64
	crashNext  bool
	alive      bool
	stats      Stats
}

func New(cfg Config) *Agent {
	if cfg.Ledger == nil {
		cfg.Ledger = NewMemLedger()
	}
	if cfg.Install == nil {
		cfg.Install = func(bundle.Manifest, []byte) error { return nil }
	}
	a := &Agent{cfg: cfg, mirror: map[string][]byte{}, alive: true}
	for _, r := range cfg.Ledger.Records() {
		if r.Status == control.Succeeded && r.Generation > a.generation {
			a.generation, a.version = r.Generation, r.Version
		}
	}
	return a
}

func (a *Agent) emit(typ string, kv ...string) {
	if a.cfg.OnEvent == nil {
		return
	}
	d := map[string]string{"region": a.cfg.Region}
	for i := 0; i+1 < len(kv); i += 2 {
		d[kv[i]] = kv[i+1]
	}
	a.cfg.OnEvent(Event{Type: typ, Details: d})
}

// CrashBeforeNextAck makes the worker die after it installs the next job but
// before it acknowledges, the worst moment for duplicate execution.
func (a *Agent) CrashBeforeNextAck() {
	a.mu.Lock()
	a.crashNext = true
	a.mu.Unlock()
}

// Restart brings a crashed worker back. Its ledger and cache survive; its
// in-memory outbox does not, exactly as with a real process restart.
func (a *Agent) Restart() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.alive {
		a.alive = true
		a.outbox = nil
		a.emit("worker.restarted")
	}
}

func (a *Agent) Alive() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.alive
}

// CorruptCached flips one byte of a cached artifact, simulating disk or
// transfer corruption. It reports whether anything was cached.
func (a *Agent) CorruptCached(digest string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	b, ok := a.mirror[digest]
	if !ok || len(b) == 0 {
		return false
	}
	c := append([]byte(nil), b...)
	c[len(c)/2] ^= 0xFF
	a.mirror[digest] = c
	return true
}

// Cached lists cached artifact digests.
func (a *Agent) Cached() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, 0, len(a.mirror))
	for d := range a.mirror {
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

func (a *Agent) Version() (string, uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.version, a.generation
}

func (a *Agent) Stats() Stats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stats
}

func (a *Agent) Outbox() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.outbox)
}

// Sync runs one reconciliation pass against the control plane: flush buffered
// acknowledgements, then bring the region to its latest desired generation.
// It returns ErrUnreachable if the control plane cannot be reached.
func (a *Agent) Sync(cp ControlPlane) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.alive {
		return nil
	}
	if err := a.flush(cp); err != nil {
		return err
	}
	pending, err := cp.Pending(a.cfg.Region)
	if err != nil {
		return ErrUnreachable
	}
	if len(pending) == 0 {
		return nil
	}
	latest := pending[len(pending)-1]
	for _, j := range pending[:len(pending)-1] {
		a.settleOlder(j)
	}
	if err := a.handleLatest(cp, latest); err != nil {
		return err
	}
	return a.flush(cp)
}

// settleOlder resolves a job that a newer generation for the same region has
// already replaced. Applying it would be wasted work and, if applied after
// the newer one, a rollback.
func (a *Agent) settleOlder(j control.Job) {
	if rec, ok := a.cfg.Ledger.Get(j.ID); ok {
		a.stats.DuplicateDeliveries++
		a.queue(j.ID, rec.Status, rec.Detail)
		return
	}
	a.cfg.Ledger.Put(Record{JobID: j.ID, Status: control.Superseded, Version: j.Version, Generation: j.Generation, Detail: "newer generation pending"})
	a.stats.Superseded++
	a.queue(j.ID, control.Superseded, "newer generation pending")
}

func (a *Agent) handleLatest(cp ControlPlane, j control.Job) error {
	if rec, ok := a.cfg.Ledger.Get(j.ID); ok {
		// Already done; the ack was lost (crash, partition, leader change).
		a.stats.DuplicateDeliveries++
		a.emit("job.duplicate_suppressed", "job", j.ID, "version", j.Version)
		a.queue(j.ID, rec.Status, rec.Detail)
		return nil
	}
	if j.Generation <= a.generation {
		a.stats.StaleRejected++
		a.cfg.Ledger.Put(Record{JobID: j.ID, Status: control.Rejected, Version: j.Version, Generation: j.Generation, Detail: "stale generation"})
		a.emit("job.stale_rejected", "job", j.ID, "generation", fmt.Sprint(j.Generation))
		a.queue(j.ID, control.Rejected, "stale generation")
		return nil
	}
	m, err := cp.Release(j.Service, j.Version)
	if err != nil {
		return ErrUnreachable
	}
	if err := a.cfg.ReleaseKeys.VerifyManifest(m); err != nil {
		a.stats.ArtifactsRejected++
		a.cfg.Ledger.Put(Record{JobID: j.ID, Status: control.Rejected, Version: j.Version, Generation: j.Generation, Detail: err.Error()})
		a.emit("artifact.rejected", "job", j.ID, "reason", err.Error())
		a.queue(j.ID, control.Rejected, err.Error())
		return nil
	}
	art, cached := a.mirror[m.Digest]
	discarded := false
	if cached {
		if err := a.cfg.ReleaseKeys.Verify(m, art); err != nil {
			// A damaged local copy is never trusted: drop it and fetch clean bytes.
			a.stats.ArtifactsRejected++
			a.emit("artifact.rejected", "job", j.ID, "reason", err.Error(), "source", "local cache")
			delete(a.mirror, m.Digest)
			cached = false
			discarded = true
		}
	}
	if !cached {
		b, err := cp.Artifact(m.Digest)
		if err != nil {
			return ErrUnreachable
		}
		if err := a.cfg.ReleaseKeys.Verify(m, b); err != nil {
			a.stats.ArtifactsRejected++
			a.emit("artifact.rejected", "job", j.ID, "reason", err.Error(), "source", "control plane")
			return nil // leave pending; never install unverified bytes
		}
		if discarded {
			a.stats.Refetched++
			a.emit("artifact.refetched", "job", j.ID, "digest", m.Digest[:12])
		}
		a.mirror[m.Digest] = b
		art = b
	}
	return a.install(j.ID, j.Generation, m, art)
}

func (a *Agent) install(jobID string, gen uint64, m bundle.Manifest, art []byte) error {
	if err := a.cfg.Install(m, art); err != nil {
		a.cfg.Ledger.Put(Record{JobID: jobID, Status: control.Rejected, Version: m.Version, Generation: gen, Detail: "install failed: " + err.Error()})
		a.queue(jobID, control.Rejected, "install failed: "+err.Error())
		return nil
	}
	// Write-ahead: completion is durable before anyone is told about it.
	a.cfg.Ledger.Put(Record{JobID: jobID, Status: control.Succeeded, Version: m.Version, Generation: gen})
	a.stats.Executed++
	a.version, a.generation = m.Version, gen
	a.emit("job.executed", "job", jobID, "version", m.Version, "generation", fmt.Sprint(gen))
	if a.crashNext {
		a.crashNext = false
		a.alive = false
		a.stats.Crashes++
		a.emit("worker.crashed", "job", jobID, "when", "after install, before ack")
		return nil
	}
	a.queue(jobID, control.Succeeded, "")
	return nil
}

func (a *Agent) queue(jobID, status, detail string) {
	a.outbox = append(a.outbox, Ack{JobID: jobID, Status: status, Detail: detail})
}

func (a *Agent) flush(cp ControlPlane) error {
	for len(a.outbox) > 0 {
		ack := a.outbox[0]
		if err := cp.Ack(ack.JobID, ack.Status, ack.Detail); err != nil {
			return ErrUnreachable
		}
		a.stats.AcksSent++
		a.outbox = a.outbox[1:]
	}
	return nil
}

// Import applies a signed offline package for a region with no network path.
// The whole package is rejected if its signature, sequence or any artifact
// fails verification. It returns the acknowledgements to carry back.
func (a *Agent) Import(p bundle.Package) ([]Ack, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.alive {
		return nil, errors.New("worker down")
	}
	reject := func(reason string) ([]Ack, error) {
		a.stats.PackagesRejected++
		a.emit("package.rejected", "sequence", fmt.Sprint(p.Sequence), "reason", reason)
		return nil, errors.New(reason)
	}
	if p.Region != a.cfg.Region {
		return reject("package is for region " + p.Region)
	}
	if err := a.cfg.PackageKeys.VerifyPackage(p); err != nil {
		return reject(err.Error())
	}
	if p.Sequence <= a.pkgSeq {
		return reject("replayed or out-of-order package")
	}
	for _, j := range p.Jobs {
		if err := a.cfg.ReleaseKeys.Verify(j.Manifest, p.Artifacts[j.Manifest.Digest]); err != nil {
			a.stats.ArtifactsRejected++
			return reject(fmt.Sprintf("job %s: %v", j.JobID, err))
		}
	}
	a.pkgSeq = p.Sequence
	a.stats.PackagesImported++
	a.emit("package.imported", "sequence", fmt.Sprint(p.Sequence), "jobs", fmt.Sprint(len(p.Jobs)))
	jobs := append([]bundle.OfflineJob(nil), p.Jobs...)
	sort.Slice(jobs, func(i, k int) bool { return jobs[i].Generation < jobs[k].Generation })
	var acks []Ack
	for i, j := range jobs {
		if rec, ok := a.cfg.Ledger.Get(j.JobID); ok {
			a.stats.DuplicateDeliveries++
			acks = append(acks, Ack{JobID: j.JobID, Status: rec.Status, Detail: rec.Detail})
			continue
		}
		if i < len(jobs)-1 {
			a.cfg.Ledger.Put(Record{JobID: j.JobID, Status: control.Superseded, Version: j.Manifest.Version, Generation: j.Generation})
			a.stats.Superseded++
			acks = append(acks, Ack{JobID: j.JobID, Status: control.Superseded, Detail: "newer generation in package"})
			continue
		}
		if j.Generation <= a.generation {
			a.stats.StaleRejected++
			a.cfg.Ledger.Put(Record{JobID: j.JobID, Status: control.Rejected, Version: j.Manifest.Version, Generation: j.Generation, Detail: "stale generation"})
			acks = append(acks, Ack{JobID: j.JobID, Status: control.Rejected, Detail: "stale generation"})
			continue
		}
		art := p.Artifacts[j.Manifest.Digest]
		a.mirror[j.Manifest.Digest] = art
		before := len(a.outbox)
		a.install(j.JobID, j.Generation, j.Manifest, art)
		if len(a.outbox) > before {
			acks = append(acks, a.outbox[before:]...)
			a.outbox = a.outbox[:before]
		}
	}
	return acks, nil
}
