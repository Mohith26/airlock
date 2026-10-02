// Package cluster wires the real Raft, control-plane and agent code into one
// deterministic, fault-injectable environment. The guided demo, the 10,000
// job benchmark and the end-to-end tests all run through it. Nothing here is
// a mock of the logic under test: these are the same types the networked
// binaries use, driven by a seeded clock and network instead of real ones.
package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sort"

	"github.com/Mohith26/airlock/internal/agent"
	"github.com/Mohith26/airlock/internal/audit"
	"github.com/Mohith26/airlock/internal/bundle"
	"github.com/Mohith26/airlock/internal/control"
	"github.com/Mohith26/airlock/internal/raft"
)

// RegionSpec declares a region.
type RegionSpec struct {
	Name      string
	AirGapped bool // reachable only through signed offline packages
}

// Options configure a simulation.
type Options struct {
	Seed           int64
	Nodes          int
	Regions        []RegionSpec
	TickMS         int // wall-clock meaning of one tick, used for reporting
	ElectionTicks  int
	HeartbeatTicks int
	SyncEvery      int // agent poll interval in ticks
	PackageEvery   int // air-gap courier interval in ticks
	RestartAfter   int // ticks before a crashed worker restarts
	DropRate       float64
	Timeline       bool // record a per-event timeline (demo); off for large benchmarks
}

func (o *Options) defaults() {
	if o.Nodes == 0 {
		o.Nodes = 3
	}
	if o.TickMS == 0 {
		o.TickMS = 10
	}
	if o.ElectionTicks == 0 {
		o.ElectionTicks = 15
	}
	if o.HeartbeatTicks == 0 {
		o.HeartbeatTicks = 3
	}
	if o.SyncEvery == 0 {
		o.SyncEvery = 5
	}
	if o.PackageEvery == 0 {
		o.PackageEvery = 100
	}
	if o.RestartAfter == 0 {
		o.RestartAfter = 30
	}
}

// Event is one timeline entry.
type Event struct {
	Tick    int               `json:"tick"`
	MS      int               `json:"ms"`
	Type    string            `json:"type"`
	Details map[string]string `json:"details,omitempty"`
}

// Failover records one leader loss and recovery.
type Failover struct {
	Killed       int `json:"killed_node"`
	NewLeader    int `json:"new_leader"`
	KilledAt     int `json:"killed_tick"`
	ElectedAt    int `json:"elected_tick"`
	FirstCommit  int `json:"first_commit_tick"`
	ElectionMS   int `json:"election_ms"`
	WriteGapMS   int `json:"write_unavailable_ms"`
	pendingIndex uint64
}

type proposal struct {
	tick int
	term uint64
}

type courier struct {
	tamperNext bool
	acks       []agent.Ack // carried back on the next trip
}

// Sim is the running environment.
type Sim struct {
	Opt     Options
	Net     *raft.MemNet
	FSMs    map[int]*control.FSM
	Agents  map[string]*agent.Agent
	Release *bundle.Signer
	CPKey   *bundle.Signer
	Audit   audit.Log

	specs       map[string]RegionSpec
	partitioned map[string]bool
	artifacts   map[string][]byte
	crashedAt   map[string]int
	couriers    map[string]*courier
	pkgSeq      map[string]uint64
	rng         *rand.Rand

	leader      int
	proposals   map[uint64]proposal
	auditIndex  uint64
	submitTick  map[string]int
	execCount   map[string]int
	failovers   []Failover
	openFail    *Failover
	Events      []Event
	eventCounts map[string]int

	OnTick func()

	corruptInstalls int

	CommitLatency  []int // ticks from proposal to first commit
	ConfirmLatency []int // ticks from submit to the region confirming success
	ConfirmByJob   map[string]int
}

// New builds a simulation with deterministic keys derived from the seed.
func New(opt Options) *Sim {
	opt.defaults()
	s := &Sim{
		Opt:          opt,
		Net:          raft.NewMemNet(opt.Nodes, opt.ElectionTicks, opt.HeartbeatTicks, opt.Seed),
		FSMs:         map[int]*control.FSM{},
		Agents:       map[string]*agent.Agent{},
		specs:        map[string]RegionSpec{},
		partitioned:  map[string]bool{},
		artifacts:    map[string][]byte{},
		crashedAt:    map[string]int{},
		couriers:     map[string]*courier{},
		pkgSeq:       map[string]uint64{},
		rng:          rand.New(rand.NewSource(opt.Seed ^ 0x5eed)),
		proposals:    map[uint64]proposal{},
		submitTick:   map[string]int{},
		execCount:    map[string]int{},
		eventCounts:  map[string]int{},
		ConfirmByJob: map[string]int{},
	}
	s.Net.DropRate = opt.DropRate
	s.Release, _ = bundle.NewSigner(bytes.Repeat([]byte{byte(opt.Seed) | 1}, 32))
	s.CPKey, _ = bundle.NewSigner(bytes.Repeat([]byte{byte(opt.Seed) | 2}, 32))
	for id := 1; id <= opt.Nodes; id++ {
		s.FSMs[id] = control.New()
	}
	s.Net.OnCommit = s.onCommit
	for _, r := range opt.Regions {
		r := r
		s.specs[r.Name] = r
		s.couriers[r.Name] = &courier{}
		s.Agents[r.Name] = agent.New(agent.Config{
			Region:      r.Name,
			ReleaseKeys: bundle.NewKeyring(s.Release.Pub),
			PackageKeys: bundle.NewKeyring(s.CPKey.Pub),
			OnEvent:     func(e agent.Event) { s.onAgentEvent(r.Name, e) },
			Install: func(m bundle.Manifest, art []byte) error {
				// Independent check: count any install whose bytes do not match the signed digest.
				if bundle.Digest(art) != m.Digest {
					s.corruptInstalls++
				}
				return nil
			},
		})
	}
	return s
}

func (s *Sim) now() int     { return s.Net.Now() }
func (s *Sim) ms(t int) int { return t * s.Opt.TickMS }

func (s *Sim) record(typ string, kv ...string) {
	s.eventCounts[typ]++
	var d map[string]string
	if len(kv) > 0 {
		d = map[string]string{}
		for i := 0; i+1 < len(kv); i += 2 {
			d[kv[i]] = kv[i+1]
		}
	}
	s.Audit.Append(int64(s.ms(s.now())), typ, actorOf(d), d)
	if s.Opt.Timeline {
		s.Events = append(s.Events, Event{Tick: s.now(), MS: s.ms(s.now()), Type: typ, Details: d})
	}
}

func actorOf(d map[string]string) string {
	if r, ok := d["region"]; ok {
		return "region/" + r
	}
	if n, ok := d["node"]; ok {
		return "control/" + n
	}
	return "control"
}

// EventCount returns how many events of a type were recorded.
func (s *Sim) EventCount(typ string) int { return s.eventCounts[typ] }

func (s *Sim) onAgentEvent(region string, e agent.Event) {
	if e.Type == "job.executed" {
		s.execCount[e.Details["job"]]++
	}
	kv := make([]string, 0, 2*len(e.Details))
	keys := make([]string, 0, len(e.Details))
	for k := range e.Details {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		kv = append(kv, k, e.Details[k])
	}
	s.record(e.Type, kv...)
}

func (s *Sim) onCommit(node int, e raft.Entry) {
	res := s.FSMs[node].Apply(e.Index, e.Data)
	if p, ok := s.proposals[e.Index]; ok && p.term == e.Term {
		s.CommitLatency = append(s.CommitLatency, s.now()-p.tick)
		delete(s.proposals, e.Index)
	}
	if f := s.openFail; f != nil && f.FirstCommit == 0 && f.ElectedAt > 0 && e.Index >= f.pendingIndex && f.pendingIndex > 0 {
		f.FirstCommit = s.now()
		f.WriteGapMS = s.ms(f.FirstCommit - f.KilledAt)
		s.failovers = append(s.failovers, *f)
		s.openFail = nil
	}
	if e.Index <= s.auditIndex {
		return // already recorded when the first replica applied it
	}
	s.auditIndex = e.Index
	if res.Err != nil {
		s.record("command.error", "error", res.Err.Error())
		return
	}
	switch res.Op {
	case "release":
		if !res.Duplicate {
			s.record("release.registered")
		}
	case "deploy":
		j, _ := s.FSMs[node].Job(res.JobID)
		if res.Duplicate {
			s.record("job.duplicate_submit_suppressed", "job", res.JobID)
		} else {
			s.record("job.committed", "job", res.JobID, "region", j.Region, "version", j.Version, "generation", fmt.Sprint(j.Generation))
		}
	case "ack":
		j, _ := s.FSMs[node].Job(res.JobID)
		if res.Duplicate {
			s.record("ack.duplicate_ignored", "job", res.JobID)
			return
		}
		s.record("job."+j.Status, "job", res.JobID, "region", j.Region, "version", j.Version)
		if j.Status == control.Succeeded {
			if t, ok := s.submitTick[res.JobID]; ok {
				lat := s.now() - t
				s.ConfirmLatency = append(s.ConfirmLatency, lat)
				s.ConfirmByJob[res.JobID] = lat
			}
		}
	}
}

// ErrNoLeader means no live leader is available to accept a write.
var ErrNoLeader = errors.New("no leader available")

func (s *Sim) propose(c control.Command) error {
	l := s.Net.Leader()
	if l == 0 {
		return ErrNoLeader
	}
	c.At = int64(s.ms(s.now()))
	idx, term, err := s.Net.Nodes[l].Propose(c.Encode())
	if err != nil {
		return err
	}
	s.proposals[idx] = proposal{tick: s.now(), term: term}
	return nil
}

// RegisterRelease signs an artifact and registers its manifest.
func (s *Sim) RegisterRelease(service, version string, artifact []byte) (bundle.Manifest, error) {
	m := s.Release.Sign(service, version, artifact, epoch(s.ms(s.now())))
	s.artifacts[m.Digest] = append([]byte(nil), artifact...)
	return m, s.propose(control.Command{Op: "release", Manifest: &m})
}

// Submit proposes a deployment job. Re-submitting the same id is safe.
func (s *Sim) Submit(jobID, region, service, version string) error {
	if _, ok := s.submitTick[jobID]; !ok {
		s.submitTick[jobID] = s.now()
	}
	return s.propose(control.Command{Op: "deploy", JobID: jobID, Region: region, Service: service, Version: version})
}

// Leader returns the current live leader id, or 0.
func (s *Sim) Leader() int { return s.Net.Leader() }

// LeaderFSM returns the leader's state machine, or any live replica's if
// there is no leader.
func (s *Sim) LeaderFSM() *control.FSM {
	if l := s.Net.Leader(); l != 0 {
		return s.FSMs[l]
	}
	for id := 1; id <= s.Opt.Nodes; id++ {
		if !s.Net.IsDown(id) {
			return s.FSMs[id]
		}
	}
	return s.FSMs[1]
}

// KillLeader crashes the current leader and returns its id.
func (s *Sim) KillLeader() int {
	l := s.Net.Leader()
	if l == 0 {
		return 0
	}
	s.Net.Stop(l)
	s.openFail = &Failover{Killed: l, KilledAt: s.now()}
	s.record("fault.leader_killed", "node", fmt.Sprint(l))
	return l
}

// Revive restarts a crashed control-plane node.
func (s *Sim) Revive(id int) {
	if s.Net.IsDown(id) {
		s.Net.Start(id)
		s.record("node.restarted", "node", fmt.Sprint(id))
	}
}

// Partition cuts a region off from the control plane.
func (s *Sim) Partition(region string) {
	if !s.partitioned[region] {
		s.partitioned[region] = true
		s.record("fault.region_partitioned", "region", region)
	}
}

// Reconnect restores a region's network path.
func (s *Sim) Reconnect(region string) {
	if s.partitioned[region] {
		delete(s.partitioned, region)
		s.record("region.reconnected", "region", region)
	}
}

func (s *Sim) Partitioned(region string) bool { return s.partitioned[region] }

// CrashWorker makes a region's worker die after its next install, before it
// acknowledges. It restarts automatically after RestartAfter ticks.
func (s *Sim) CrashWorker(region string) {
	s.Agents[region].CrashBeforeNextAck()
	s.record("fault.worker_crash_armed", "region", region)
}

// CorruptCache damages one cached artifact in a region. It reports whether a
// cached artifact existed to corrupt.
func (s *Sim) CorruptCache(region string) bool {
	a := s.Agents[region]
	c := a.Cached()
	if len(c) == 0 {
		return false
	}
	d := c[s.rng.Intn(len(c))]
	if a.CorruptCached(d) {
		s.record("fault.cache_corrupted", "region", region, "digest", d[:12])
		return true
	}
	return false
}

// TamperNextPackage flips a byte in the next offline package for a region.
func (s *Sim) TamperNextPackage(region string) {
	s.couriers[region].tamperNext = true
	s.record("fault.package_tamper_armed", "region", region)
}

// client is the control-plane view a connected agent gets.
type client struct {
	s      *Sim
	region string
}

func (c client) reachable() error {
	if c.s.partitioned[c.region] || c.s.specs[c.region].AirGapped || c.s.Net.Leader() == 0 {
		return agent.ErrUnreachable
	}
	return nil
}

func (c client) Pending(region string) ([]control.Job, error) {
	if err := c.reachable(); err != nil {
		return nil, err
	}
	return c.s.FSMs[c.s.Net.Leader()].Pending(region), nil
}

func (c client) Release(service, version string) (bundle.Manifest, error) {
	if err := c.reachable(); err != nil {
		return bundle.Manifest{}, err
	}
	m, ok := c.s.FSMs[c.s.Net.Leader()].Release(service, version)
	if !ok {
		return bundle.Manifest{}, fmt.Errorf("unknown release %s@%s", service, version)
	}
	return m, nil
}

func (c client) Artifact(digest string) ([]byte, error) {
	if err := c.reachable(); err != nil {
		return nil, err
	}
	b, ok := c.s.artifacts[digest]
	if !ok {
		return nil, fmt.Errorf("unknown artifact %s", digest)
	}
	return append([]byte(nil), b...), nil
}

func (c client) Ack(jobID, status, detail string) error {
	if err := c.reachable(); err != nil {
		return err
	}
	return c.s.propose(control.Command{Op: "ack", JobID: jobID, Status: status, Detail: detail})
}

// Tick advances the environment by one tick.
func (s *Sim) Tick() {
	s.Net.Tick()
	t := s.now()
	if l := s.Net.Leader(); l != s.leader {
		if l != 0 {
			s.record("leader.elected", "node", fmt.Sprint(l), "term", fmt.Sprint(s.Net.Nodes[l].Term()))
			if f := s.openFail; f != nil && f.ElectedAt == 0 && l != f.Killed {
				f.ElectedAt = t
				f.NewLeader = l
				f.ElectionMS = s.ms(t - f.KilledAt)
				f.pendingIndex = s.Net.Nodes[l].LastIndex() // the new leader's no-op
			}
		}
		s.leader = l
	}
	for region, at := range s.crashedAt {
		if t-at >= s.Opt.RestartAfter {
			s.Agents[region].Restart()
			delete(s.crashedAt, region)
		}
	}
	names := s.regionNames()
	if t%s.Opt.SyncEvery == 0 {
		for _, r := range names {
			if s.specs[r].AirGapped {
				continue
			}
			a := s.Agents[r]
			a.Sync(client{s: s, region: r})
			if !a.Alive() {
				if _, ok := s.crashedAt[r]; !ok {
					s.crashedAt[r] = t
				}
			}
		}
	}
	if t%s.Opt.PackageEvery == 0 {
		for _, r := range names {
			if s.specs[r].AirGapped {
				s.courierTrip(r)
			}
		}
	}
	if s.OnTick != nil {
		s.OnTick()
	}
}

func (s *Sim) regionNames() []string {
	names := make([]string, 0, len(s.specs))
	for n := range s.specs {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// courierTrip carries acknowledgements back from an air-gapped region, then
// carries a freshly signed package of its pending work in.
func (s *Sim) courierTrip(region string) {
	c := s.couriers[region]
	if s.Net.Leader() == 0 {
		return // no one to sign or receive; try next trip
	}
	kept := c.acks[:0]
	for _, a := range c.acks {
		if err := s.propose(control.Command{Op: "ack", JobID: a.JobID, Status: a.Status, Detail: a.Detail}); err != nil {
			kept = append(kept, a)
		}
	}
	c.acks = kept
	p, ok := s.ExportPackage(region)
	if !ok {
		return
	}
	if c.tamperNext {
		c.tamperNext = false
		for d, b := range p.Artifacts {
			if len(b) > 0 {
				bad := append([]byte(nil), b...)
				bad[len(bad)/2] ^= 0x01
				p.Artifacts[d] = bad
				break
			}
		}
		s.record("fault.package_tampered_in_transit", "region", region, "sequence", fmt.Sprint(p.Sequence))
	}
	a := s.Agents[region]
	acks, err := a.Import(p)
	if err == nil {
		c.acks = append(c.acks, acks...)
	}
	if !a.Alive() {
		if _, ok := s.crashedAt[region]; !ok {
			s.crashedAt[region] = s.now()
		}
	}
}

// ExportPackage builds a signed offline package with a region's pending jobs.
func (s *Sim) ExportPackage(region string) (bundle.Package, bool) {
	f := s.LeaderFSM()
	pending := f.Pending(region)
	if len(pending) == 0 {
		return bundle.Package{}, false
	}
	s.pkgSeq[region]++
	p := bundle.Package{Region: region, Sequence: s.pkgSeq[region], IssuedAt: epoch(s.ms(s.now())), Artifacts: map[string][]byte{}}
	for _, j := range pending {
		m, ok := f.Release(j.Service, j.Version)
		if !ok {
			continue
		}
		p.Jobs = append(p.Jobs, bundle.OfflineJob{JobID: j.ID, Generation: j.Generation, Manifest: m})
		p.Artifacts[m.Digest] = append([]byte(nil), s.artifacts[m.Digest]...)
	}
	return s.CPKey.SignPackage(p), true
}

// RunFor advances n ticks.
func (s *Sim) RunFor(n int) {
	for i := 0; i < n; i++ {
		s.Tick()
	}
}

// RunUntil advances until cond holds or max ticks pass; it reports success.
func (s *Sim) RunUntil(max int, cond func() bool) bool {
	for i := 0; i < max; i++ {
		if cond() {
			return true
		}
		s.Tick()
	}
	return cond()
}

// Converged reports whether every region has confirmed its desired generation
// and no job is still pending.
func (s *Sim) Converged() bool {
	f := s.LeaderFSM()
	if s.Net.Leader() == 0 {
		return false
	}
	for _, r := range f.Regions() {
		if r.DesiredGen != r.ConfirmedGen {
			return false
		}
		if len(f.Pending(r.Name)) > 0 {
			return false
		}
	}
	return true
}

// ReplicasAgree reports whether every live replica has the same state digest
// at the same applied index.
func (s *Sim) ReplicasAgree() (bool, string) {
	var digest string
	var applied uint64
	for id := 1; id <= s.Opt.Nodes; id++ {
		f := s.FSMs[id]
		if digest == "" {
			digest, applied = f.Digest(), f.Applied()
			continue
		}
		if f.Applied() != applied || f.Digest() != digest {
			return false, ""
		}
	}
	return true, digest
}

// DoubleExecutions counts jobs installed more than once in their region.
func (s *Sim) DoubleExecutions() int {
	n := 0
	for _, c := range s.execCount {
		if c > 1 {
			n++
		}
	}
	return n
}

// Failovers returns completed failover measurements.
func (s *Sim) Failovers() []Failover { return append([]Failover(nil), s.failovers...) }

// CorruptInstalls counts installs of bytes that did not match their manifest.
func (s *Sim) CorruptInstalls() int { return s.corruptInstalls }
