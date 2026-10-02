// Package server runs one control-plane replica as a network service: a Raft
// node talking to its peers over HTTP, the replicated state machine, and the
// public API that operators and region agents use.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Mohith26/airlock/internal/audit"
	"github.com/Mohith26/airlock/internal/bundle"
	"github.com/Mohith26/airlock/internal/control"
	"github.com/Mohith26/airlock/internal/metrics"
	"github.com/Mohith26/airlock/internal/raft"
)

// Config configures a replica.
type Config struct {
	ID          int
	Peers       map[int]string // id -> base URL, including this node
	Listen      string
	Tick        time.Duration
	Election    int // ticks
	Heartbeat   int // ticks
	Storage     raft.Storage
	ReleaseKeys bundle.Keyring // manifests must verify against these to be registered
	PackageKey  *bundle.Signer // signs offline packages; optional
	Seed        int64
}

// Server is one control-plane replica.
type Server struct {
	cfg  Config
	mu   sync.Mutex
	node *raft.Node
	fsm  *control.FSM
	log  audit.Log
	m    *metrics.Registry

	waiters map[uint64]waiter
	pkgSeq  map[string]uint64

	httpSrv  *http.Server
	listener net.Listener
	client   *http.Client
	outbound map[int]chan raft.Message
	stop     chan struct{}
	stopped  bool
	wg       sync.WaitGroup
	lastLead int
}

type waiter struct {
	term uint64
	ch   chan control.Result
}

// New builds a replica. Call Start to begin serving.
func New(cfg Config) (*Server, error) {
	if cfg.Tick == 0 {
		cfg.Tick = 10 * time.Millisecond
	}
	ids := make([]int, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	n, err := raft.NewNode(raft.Config{ID: cfg.ID, Peers: ids, ElectionTicks: cfg.Election, HeartbeatTicks: cfg.Heartbeat, Seed: cfg.Seed, Storage: cfg.Storage})
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg: cfg, node: n, fsm: control.New(), m: metrics.NewRegistry(),
		waiters: map[uint64]waiter{}, pkgSeq: map[string]uint64{},
		client:   &http.Client{Timeout: 200 * time.Millisecond},
		outbound: map[int]chan raft.Message{},
		stop:     make(chan struct{}),
	}
	for id := range cfg.Peers {
		if id != cfg.ID {
			s.outbound[id] = make(chan raft.Message, 1024)
		}
	}
	return s, nil
}

// Start listens and begins ticking. If ln is nil, it listens on cfg.Listen.
func (s *Server) Start(ln net.Listener) error {
	if ln == nil {
		var err error
		if ln, err = net.Listen("tcp", s.cfg.Listen); err != nil {
			return err
		}
	}
	s.listener = ln
	s.httpSrv = &http.Server{Handler: s.routes(), ReadHeaderTimeout: 2 * time.Second}
	s.wg.Add(2 + len(s.outbound))
	go func() { defer s.wg.Done(); s.httpSrv.Serve(ln) }()
	go func() { defer s.wg.Done(); s.tickLoop() }()
	for id, q := range s.outbound {
		go func(id int, q chan raft.Message) { defer s.wg.Done(); s.sendLoop(id, q) }(id, q)
	}
	return nil
}

// Stop simulates a crash: the listener closes and the node stops ticking.
func (s *Server) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	close(s.stop)
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	s.httpSrv.Shutdown(ctx)
	s.httpSrv.Close()
	s.wg.Wait()
}

func (s *Server) tickLoop() {
	t := time.NewTicker(s.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.mu.Lock()
			s.node.Tick()
			s.drainLocked()
			s.mu.Unlock()
		}
	}
}

// drainLocked ships outbound messages and applies newly committed entries.
func (s *Server) drainLocked() {
	msgs, committed := s.node.Ready()
	for _, m := range msgs {
		q, ok := s.outbound[m.To]
		if !ok {
			continue
		}
		select {
		case q <- m:
		default:
			s.m.Inc("airlock_raft_messages_dropped_total") // back-pressure: Raft tolerates loss
		}
	}
	for _, e := range committed {
		res := s.fsm.Apply(e.Index, e.Data)
		s.m.Inc("airlock_entries_applied_total")
		s.auditLocked(e, res)
		if w, ok := s.waiters[e.Index]; ok {
			if w.term != e.Term {
				res = control.Result{Err: errors.New("leadership lost before commit; retry")}
			}
			w.ch <- res
			delete(s.waiters, e.Index)
		}
	}
	if l := s.node.Leader(); l != s.lastLead {
		s.lastLead = l
		if l == s.cfg.ID {
			s.m.Inc("airlock_leader_elections_total")
			s.log.Append(time.Now().UnixMilli(), "leader.elected", fmt.Sprintf("control/%d", s.cfg.ID), map[string]string{"term": fmt.Sprint(s.node.Term())})
		}
	}
}

func (s *Server) auditLocked(e raft.Entry, res control.Result) {
	if res.Op == "" || res.Op == "noop" {
		return
	}
	d := map[string]string{"index": fmt.Sprint(e.Index)}
	if res.JobID != "" {
		d["job"] = res.JobID
	}
	if res.Status != "" {
		d["status"] = res.Status
	}
	typ := res.Op + ".applied"
	if res.Duplicate {
		typ = res.Op + ".duplicate"
		s.m.Inc("airlock_duplicates_suppressed_total")
	}
	if res.Err != nil {
		typ, d["error"] = res.Op+".error", res.Err.Error()
	}
	s.log.Append(time.Now().UnixMilli(), typ, fmt.Sprintf("control/%d", s.cfg.ID), d)
}

// sendLoop delivers messages to one peer, batching whatever has queued up.
func (s *Server) sendLoop(peer int, q chan raft.Message) {
	url := s.cfg.Peers[peer]
	for {
		select {
		case <-s.stop:
			return
		case m := <-q:
			batch := []raft.Message{m}
		more:
			for len(batch) < 64 {
				select {
				case n := <-q:
					batch = append(batch, n)
				default:
					break more
				}
			}
			b, _ := json.Marshal(batch)
			resp, err := s.client.Post(url+"/raft/message", "application/json", bytes.NewReader(b))
			if err != nil {
				s.m.Inc("airlock_raft_send_errors_total")
				continue
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			s.m.Add("airlock_raft_messages_sent_total", float64(len(batch)))
		}
	}
}

// Status is the replica's view of the cluster.
type Status struct {
	raft.Status
	LeaderURL string           `json:"leader_url,omitempty"`
	Applied   uint64           `json:"applied"`
	Regions   []control.Region `json:"regions"`
	Jobs      map[string]int   `json:"jobs"`
	Digest    string           `json:"state_digest"`
}

func (s *Server) Status() Status {
	s.mu.Lock()
	st := s.node.Status()
	s.mu.Unlock()
	return Status{Status: st, LeaderURL: s.cfg.Peers[st.Leader], Applied: s.fsm.Applied(), Regions: s.fsm.Regions(), Jobs: s.fsm.Counts(), Digest: s.fsm.Digest()}
}

func (s *Server) FSM() *control.FSM { return s.fsm }
func (s *Server) URL() string       { return s.cfg.Peers[s.cfg.ID] }

// propose replicates a command and waits for it to be applied.
func (s *Server) propose(ctx context.Context, c control.Command) (control.Result, error) {
	c.At = time.Now().UnixMilli()
	s.mu.Lock()
	idx, term, err := s.node.Propose(c.Encode())
	if err != nil {
		s.mu.Unlock()
		return control.Result{}, err
	}
	ch := make(chan control.Result, 1)
	s.waiters[idx] = waiter{term: term, ch: ch}
	s.drainLocked()
	s.mu.Unlock()
	start := time.Now()
	select {
	case r := <-ch:
		s.m.Observe("airlock_commit_seconds", time.Since(start).Seconds())
		return r, r.Err
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.waiters, idx)
		s.mu.Unlock()
		return control.Result{}, fmt.Errorf("commit timed out: %w", ctx.Err())
	}
}

// ---- HTTP ----

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func (s *Server) notLeader(w http.ResponseWriter) {
	st := s.Status()
	writeJSON(w, http.StatusMisdirectedRequest, map[string]any{"error": "not the leader", "leader": st.Leader, "leader_url": st.LeaderURL})
}

func (s *Server) isLeader() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.node.State() == raft.Leader
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/message", func(w http.ResponseWriter, r *http.Request) {
		var msgs []raft.Message
		if err := json.NewDecoder(io.LimitReader(r.Body, 64<<20)).Decode(&msgs); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		for _, m := range msgs {
			if m.To == s.cfg.ID {
				s.node.Step(m)
			}
		}
		s.drainLocked()
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, s.Status()) })
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("POST /v1/releases", s.handleRelease)
	mux.HandleFunc("POST /v1/jobs", s.handleJob)
	mux.HandleFunc("POST /v1/acks", s.handleAck)
	mux.HandleFunc("GET /v1/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		j, ok := s.fsm.Job(r.PathValue("id"))
		if !ok {
			writeJSON(w, 404, map[string]string{"error": "no such job"})
			return
		}
		writeJSON(w, 200, j)
	})
	mux.HandleFunc("GET /v1/regions/{region}/pending", func(w http.ResponseWriter, r *http.Request) {
		if !s.isLeader() {
			s.notLeader(w)
			return
		}
		writeJSON(w, 200, s.fsm.Pending(r.PathValue("region")))
	})
	mux.HandleFunc("GET /v1/releases/{service}/{version}", func(w http.ResponseWriter, r *http.Request) {
		m, ok := s.fsm.Release(r.PathValue("service"), r.PathValue("version"))
		if !ok {
			writeJSON(w, 404, map[string]string{"error": "no such release"})
			return
		}
		writeJSON(w, 200, m)
	})
	mux.HandleFunc("GET /v1/artifacts/{digest}", func(w http.ResponseWriter, r *http.Request) {
		b, ok := s.fsm.Artifact(r.PathValue("digest"))
		if !ok {
			http.Error(w, "no such artifact", 404)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(b)
	})
	mux.HandleFunc("GET /v1/offline/{region}", s.handleExport)
	mux.HandleFunc("GET /v1/audit", func(w http.ResponseWriter, r *http.Request) {
		ev := s.log.Events()
		writeJSON(w, 200, map[string]any{"verified": audit.Verify(ev) == nil, "events": ev})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		st := s.Status()
		leader := 0.0
		if st.State == "leader" {
			leader = 1
		}
		s.m.Set("airlock_raft_term", float64(st.Term))
		s.m.Set("airlock_raft_commit_index", float64(st.Commit))
		s.m.Set("airlock_is_leader", leader)
		for _, k := range []string{control.Pending, control.Succeeded, control.Rejected, control.Superseded, control.Invalid} {
			s.m.Set(`airlock_jobs{status="`+k+`"}`, float64(st.Jobs[k]))
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		s.m.Write(w)
	})
	return mux
}

type releaseReq struct {
	Manifest bundle.Manifest `json:"manifest"`
	Artifact []byte          `json:"artifact"`
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	var req releaseReq
	if err := json.NewDecoder(io.LimitReader(r.Body, 32<<20)).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	// The control plane refuses to even store a release it cannot verify.
	if err := s.cfg.ReleaseKeys.Verify(req.Manifest, req.Artifact); err != nil {
		s.m.Inc("airlock_releases_rejected_total")
		writeJSON(w, 422, map[string]string{"error": err.Error()})
		return
	}
	s.commit(w, r, control.Command{Op: "release", Manifest: &req.Manifest, Artifact: req.Artifact})
}

func (s *Server) handleJob(w http.ResponseWriter, r *http.Request) {
	var c control.Command
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&c); err != nil || c.JobID == "" || c.Region == "" {
		writeJSON(w, 400, map[string]string{"error": "job_id, region, service and version are required"})
		return
	}
	c.Op = "deploy"
	s.commit(w, r, c)
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	var c control.Command
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&c); err != nil || c.JobID == "" {
		writeJSON(w, 400, map[string]string{"error": "job_id and status are required"})
		return
	}
	c.Op = "ack"
	s.commit(w, r, c)
}

func (s *Server) commit(w http.ResponseWriter, r *http.Request, c control.Command) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	res, err := s.propose(ctx, c)
	if errors.Is(err, raft.ErrNotLeader) {
		s.notLeader(w)
		return
	}
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"op": res.Op, "job_id": res.JobID, "status": res.Status, "duplicate": res.Duplicate})
}

func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	if s.cfg.PackageKey == nil {
		writeJSON(w, 501, map[string]string{"error": "no package signing key configured"})
		return
	}
	if !s.isLeader() {
		s.notLeader(w)
		return
	}
	region := r.PathValue("region")
	pending := s.fsm.Pending(region)
	s.mu.Lock()
	s.pkgSeq[region]++
	seq := s.pkgSeq[region]
	s.mu.Unlock()
	p := bundle.Package{Region: region, Sequence: uint64(time.Now().UnixMilli())*1000 + seq, IssuedAt: time.Now().UTC(), Artifacts: map[string][]byte{}}
	for _, j := range pending {
		m, ok := s.fsm.Release(j.Service, j.Version)
		if !ok {
			continue
		}
		art, _ := s.fsm.Artifact(m.Digest)
		p.Jobs = append(p.Jobs, bundle.OfflineJob{JobID: j.ID, Generation: j.Generation, Manifest: m})
		p.Artifacts[m.Digest] = art
	}
	writeJSON(w, 200, s.cfg.PackageKey.SignPackage(p))
}

// ParsePeers parses "1=http://a:7001,2=http://b:7002".
func ParsePeers(s string) (map[int]string, error) {
	out := map[int]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		var id int
		if len(kv) != 2 {
			return nil, fmt.Errorf("bad peer %q", part)
		}
		if _, err := fmt.Sscan(kv[0], &id); err != nil {
			return nil, fmt.Errorf("bad peer id %q", kv[0])
		}
		out[id] = strings.TrimRight(kv[1], "/")
	}
	return out, nil
}
