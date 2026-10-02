package cluster

import (
	"fmt"
	"time"

	"github.com/Mohith26/airlock/internal/control"
)

func epoch(ms int) time.Time {
	return time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(ms) * time.Millisecond)
}

// NodeView is one control-plane replica as a dashboard sees it.
type NodeView struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	State   string `json:"state"` // leader, follower, candidate, down
	Term    uint64 `json:"term"`
	Commit  uint64 `json:"commit"`
	Applied uint64 `json:"applied"`
}

// RegionView is one region as a dashboard sees it.
type RegionView struct {
	Name             string `json:"name"`
	Mode             string `json:"mode"` // connected, partitioned, air-gapped
	Worker           string `json:"worker"`
	DesiredVersion   string `json:"desired_version"`
	DesiredGen       uint64 `json:"desired_generation"`
	ConfirmedVersion string `json:"confirmed_version"`
	ConfirmedGen     uint64 `json:"confirmed_generation"`
	RunningVersion   string `json:"running_version"`
	Pending          int    `json:"pending_jobs"`
	BufferedAcks     int    `json:"buffered_acks"`
	Executed         uint64 `json:"executed"`
	Duplicates       uint64 `json:"duplicates_suppressed"`
	Rejected         uint64 `json:"artifacts_rejected"`
}

// Snapshot is the whole environment at one instant.
type Snapshot struct {
	Tick      int            `json:"tick"`
	MS        int            `json:"ms"`
	Leader    int            `json:"leader"`
	Nodes     []NodeView     `json:"nodes"`
	Regions   []RegionView   `json:"regions"`
	Jobs      map[string]int `json:"jobs"`
	AuditLen  int            `json:"audit_events"`
	AuditHead string         `json:"audit_head"`
}

// Snapshot captures the current state.
func (s *Sim) Snapshot() Snapshot {
	snap := Snapshot{Tick: s.now(), MS: s.ms(s.now()), Leader: s.Net.Leader(), AuditLen: s.Audit.Len(), AuditHead: s.Audit.Head()[:16]}
	for id := 1; id <= s.Opt.Nodes; id++ {
		n := s.Net.Nodes[id]
		st := n.State().String()
		if s.Net.IsDown(id) {
			st = "down"
		}
		snap.Nodes = append(snap.Nodes, NodeView{ID: id, Name: fmt.Sprintf("cp-%d", id), State: st, Term: n.Term(), Commit: n.CommitIndex(), Applied: s.FSMs[id].Applied()})
	}
	f := s.LeaderFSM()
	desired := map[string]control.Region{}
	for _, r := range f.Regions() {
		desired[r.Name] = r
	}
	for _, name := range s.regionNames() {
		a := s.Agents[name]
		st := a.Stats()
		mode := "connected"
		if s.specs[name].AirGapped {
			mode = "air-gapped"
		} else if s.partitioned[name] {
			mode = "partitioned"
		}
		worker := "running"
		if !a.Alive() {
			worker = "crashed"
		}
		running, _ := a.Version()
		d := desired[name]
		snap.Regions = append(snap.Regions, RegionView{
			Name: name, Mode: mode, Worker: worker,
			DesiredVersion: d.DesiredVersion, DesiredGen: d.DesiredGen,
			ConfirmedVersion: d.ConfirmedVersion, ConfirmedGen: d.ConfirmedGen,
			RunningVersion: running, Pending: len(f.Pending(name)), BufferedAcks: a.Outbox() + len(s.couriers[name].acks),
			Executed: st.Executed, Duplicates: st.DuplicateDeliveries, Rejected: st.ArtifactsRejected + st.PackagesRejected,
		})
	}
	snap.Jobs = f.Counts()
	return snap
}
