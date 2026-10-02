package server

import (
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/Mohith26/airlock/internal/bundle"
	"github.com/Mohith26/airlock/internal/raft"
)

// LocalCluster is a set of replicas on loopback sockets, each with its own
// write-ahead log on disk. Used by tests and the failover benchmark.
type LocalCluster struct {
	Dir     string
	Peers   map[int]string
	Servers map[int]*Server
	addrs   map[int]string
	cfg     Config
	Release *bundle.Signer
	CPKey   *bundle.Signer
}

// StartLocalCluster starts n replicas with real HTTP transport and fsync'd WALs.
func StartLocalCluster(n int, dir string, seed int64) (*LocalCluster, error) {
	rel, _ := bundle.NewSigner(nil)
	cp, _ := bundle.NewSigner(nil)
	lc := &LocalCluster{Dir: dir, Peers: map[int]string{}, Servers: map[int]*Server{}, addrs: map[int]string{}, Release: rel, CPKey: cp}
	lns := map[int]net.Listener{}
	for id := 1; id <= n; id++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		lns[id] = ln
		lc.addrs[id] = ln.Addr().String()
		lc.Peers[id] = "http://" + ln.Addr().String()
	}
	lc.cfg = Config{Peers: lc.Peers, Tick: 10 * time.Millisecond, Election: 15, Heartbeat: 3, ReleaseKeys: bundle.NewKeyring(rel.Pub), PackageKey: cp, Seed: seed}
	for id := 1; id <= n; id++ {
		if err := lc.startNode(id, lns[id]); err != nil {
			return nil, err
		}
	}
	return lc, nil
}

func (lc *LocalCluster) startNode(id int, ln net.Listener) error {
	st, err := raft.OpenFileStorage(filepath.Join(lc.Dir, fmt.Sprintf("node-%d.wal", id)), true)
	if err != nil {
		return err
	}
	cfg := lc.cfg
	cfg.ID, cfg.Storage = id, st
	s, err := New(cfg)
	if err != nil {
		return err
	}
	if ln == nil {
		// Rebind the same address so peers can reach the restarted node.
		for i := 0; i < 50; i++ {
			if ln, err = net.Listen("tcp", lc.addrs[id]); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil {
			return err
		}
	}
	lc.Servers[id] = s
	return s.Start(ln)
}

// Kill crashes a replica.
func (lc *LocalCluster) Kill(id int) {
	if s, ok := lc.Servers[id]; ok {
		s.Stop()
		if fs, ok := s.cfg.Storage.(*raft.FileStorage); ok {
			fs.Close()
		}
		delete(lc.Servers, id)
	}
}

// Restart brings a killed replica back from its write-ahead log.
func (lc *LocalCluster) Restart(id int) error { return lc.startNode(id, nil) }

// Leader returns the id of a live replica that believes it leads, with the
// highest term, or 0.
func (lc *LocalCluster) Leader() int {
	best, term := 0, uint64(0)
	for id, s := range lc.Servers {
		st := s.Status()
		if st.State == "leader" && st.Term >= term {
			best, term = id, st.Term
		}
	}
	return best
}

// WaitLeader polls until a leader other than exclude exists.
func (lc *LocalCluster) WaitLeader(exclude int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := lc.Leader(); l != 0 && l != exclude {
			return l, nil
		}
		time.Sleep(time.Millisecond)
	}
	return 0, fmt.Errorf("no leader within %s", timeout)
}

// Client returns a client that knows every replica.
func (lc *LocalCluster) Client() *Client {
	urls := make([]string, 0, len(lc.Peers))
	ids := make([]int, 0, len(lc.Peers))
	for id := range lc.Peers {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		urls = append(urls, lc.Peers[id])
	}
	return NewClient(urls...)
}

// Close stops every replica.
func (lc *LocalCluster) Close() {
	for id := range lc.Servers {
		lc.Kill(id)
	}
}

// FailoverTrial is one measured leader crash on a real networked cluster.
type FailoverTrial struct {
	Trial      int     `json:"trial"`
	Killed     int     `json:"killed"`
	NewLeader  int     `json:"new_leader"`
	ElectionMS float64 `json:"election_ms"`
	WriteGapMS float64 `json:"write_unavailable_ms"`
	Committed  int     `json:"jobs_committed_before_kill"`
	Lost       int     `json:"committed_jobs_lost"`
}

// FailoverReport aggregates trials.
type FailoverReport struct {
	Trials        []FailoverTrial `json:"trials"`
	ElectionP50   float64         `json:"election_p50_ms"`
	ElectionP99   float64         `json:"election_p99_ms"`
	ElectionMax   float64         `json:"election_max_ms"`
	WriteGapP50   float64         `json:"write_unavailable_p50_ms"`
	WriteGapP99   float64         `json:"write_unavailable_p99_ms"`
	WriteGapMax   float64         `json:"write_unavailable_max_ms"`
	CommittedLost int             `json:"committed_jobs_lost"`
	Config        map[string]any  `json:"config"`
}

func pct(v []float64, p float64) float64 {
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	i := int(math.Ceil(p*float64(len(s)))) - 1
	if i < 0 {
		i = 0
	}
	return math.Round(s[i]*10) / 10
}

// MeasureFailover crashes the leader of a fresh 3-node networked cluster,
// trials times, timing (a) until another replica wins an election and (b)
// until a client write succeeds again. It also checks no committed job is lost.
func MeasureFailover(trials int, baseDir string) (*FailoverReport, error) {
	rep := &FailoverReport{Config: map[string]any{"replicas": 3, "tick_ms": 10, "election_timeout_ms": "150-300 randomized", "heartbeat_ms": 30, "transport": "HTTP/1.1 over loopback", "wal": "fsync per write"}}
	for t := 1; t <= trials; t++ {
		dir, err := os.MkdirTemp(baseDir, "failover-")
		if err != nil {
			return nil, err
		}
		lc, err := StartLocalCluster(3, dir, int64(t))
		if err != nil {
			return nil, err
		}
		if _, err := lc.WaitLeader(0, 5*time.Second); err != nil {
			lc.Close()
			return nil, err
		}
		cl := lc.Client()
		art := []byte(fmt.Sprintf("artifact for trial %d", t))
		m := lc.Release.Sign("svc", "1.0.0", art, time.Now())
		if _, err := cl.RegisterRelease(m, art); err != nil {
			lc.Close()
			return nil, err
		}
		const before = 20
		for i := 0; i < before; i++ {
			if _, err := cl.Submit(fmt.Sprintf("t%d-pre-%d", t, i), "us-east", "svc", "1.0.0"); err != nil {
				lc.Close()
				return nil, err
			}
		}
		killed := lc.Leader()
		t0 := time.Now()
		lc.Kill(killed)
		nl, err := lc.WaitLeader(killed, 10*time.Second)
		if err != nil {
			lc.Close()
			return nil, err
		}
		elected := time.Since(t0)
		wc := lc.Client()
		wc.Backoff = time.Millisecond
		wc.Retries = 10000
		if _, err := wc.Submit(fmt.Sprintf("t%d-post", t), "us-east", "svc", "1.0.0"); err != nil {
			lc.Close()
			return nil, err
		}
		writable := time.Since(t0)
		lost := 0
		for i := 0; i < before; i++ {
			if _, ok := lc.Servers[nl].FSM().Job(fmt.Sprintf("t%d-pre-%d", t, i)); !ok {
				lost++
			}
		}
		rep.Trials = append(rep.Trials, FailoverTrial{Trial: t, Killed: killed, NewLeader: nl, ElectionMS: float64(elected.Microseconds()) / 1000, WriteGapMS: float64(writable.Microseconds()) / 1000, Committed: before, Lost: lost})
		rep.CommittedLost += lost
		lc.Close()
		os.RemoveAll(dir)
	}
	var el, wg []float64
	for _, tr := range rep.Trials {
		el = append(el, tr.ElectionMS)
		wg = append(wg, tr.WriteGapMS)
	}
	rep.ElectionP50, rep.ElectionP99, rep.ElectionMax = pct(el, .5), pct(el, .99), pct(el, 1)
	rep.WriteGapP50, rep.WriteGapP99, rep.WriteGapMax = pct(wg, .5), pct(wg, .99), pct(wg, 1)
	return rep, nil
}
