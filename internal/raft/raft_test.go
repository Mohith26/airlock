package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

func runUntil(t *testing.T, net *MemNet, max int, cond func() bool) int {
	t.Helper()
	for i := 0; i < max; i++ {
		if cond() {
			return i
		}
		net.Tick()
	}
	if !cond() {
		t.Fatalf("condition not reached within %d ticks", max)
	}
	return max
}

func TestElectsExactlyOneLeader(t *testing.T) {
	net := NewMemNet(3, 10, 2, 1)
	runUntil(t, net, 200, func() bool { return net.Leader() != 0 })
	leaders := 0
	for _, n := range net.Nodes {
		if n.State() == Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("want 1 leader, got %d", leaders)
	}
}

func TestReplicatesAndCommitsOnEveryNode(t *testing.T) {
	net := NewMemNet(3, 10, 2, 2)
	commits := map[int][]string{}
	net.OnCommit = func(id int, e Entry) {
		if len(e.Data) > 0 {
			commits[id] = append(commits[id], string(e.Data))
		}
	}
	runUntil(t, net, 200, func() bool { return net.Leader() != 0 })
	l := net.Nodes[net.Leader()]
	for i := 0; i < 5; i++ {
		if _, _, err := l.Propose([]byte(fmt.Sprintf("op-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	runUntil(t, net, 100, func() bool { return len(commits[1]) == 5 && len(commits[2]) == 5 && len(commits[3]) == 5 })
	for id := 1; id <= 3; id++ {
		for i, v := range commits[id] {
			if v != fmt.Sprintf("op-%d", i) {
				t.Fatalf("node %d entry %d = %q", id, i, v)
			}
		}
	}
}

func TestFollowerRejectsProposal(t *testing.T) {
	net := NewMemNet(3, 10, 2, 3)
	runUntil(t, net, 200, func() bool { return net.Leader() != 0 })
	for id, n := range net.Nodes {
		if id != net.Leader() {
			if _, _, err := n.Propose([]byte("x")); err != ErrNotLeader {
				t.Fatalf("follower accepted proposal: %v", err)
			}
		}
	}
}

func TestLeaderCrashElectsNewLeaderAndKeepsCommitting(t *testing.T) {
	net := NewMemNet(3, 10, 2, 4)
	var got []string
	net.OnCommit = func(id int, e Entry) {
		if id == 2 || id == 3 || id == 1 {
			if len(e.Data) > 0 && id == firstLive(net) {
				got = append(got, string(e.Data))
			}
		}
	}
	runUntil(t, net, 200, func() bool { return net.Leader() != 0 })
	old := net.Leader()
	net.Nodes[old].Propose([]byte("before"))
	runUntil(t, net, 50, func() bool { return net.Nodes[old].CommitIndex() >= 2 })
	net.Stop(old)
	ticks := runUntil(t, net, 300, func() bool { l := net.Leader(); return l != 0 && l != old })
	if ticks > 3*10+5 {
		t.Fatalf("failover took %d ticks, want at most two election timeouts", ticks)
	}
	nl := net.Nodes[net.Leader()]
	if _, _, err := nl.Propose([]byte("after")); err != nil {
		t.Fatal(err)
	}
	runUntil(t, net, 100, func() bool { return contains(got, "after") })
	if !contains(got, "before") {
		t.Fatalf("committed entry lost across failover: %v", got)
	}
}

func firstLive(net *MemNet) int {
	for id := 1; id <= len(net.Nodes); id++ {
		if !net.IsDown(id) {
			return id
		}
	}
	return 0
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func TestIsolatedLeaderStepsDownAndDivergentEntriesAreReplaced(t *testing.T) {
	net := NewMemNet(3, 10, 2, 5)
	runUntil(t, net, 200, func() bool { return net.Leader() != 0 })
	old := net.Leader()
	net.Isolate(old)
	// Entries proposed to the isolated leader can never commit.
	net.Nodes[old].Propose([]byte("orphan"))
	runUntil(t, net, 400, func() bool { l := net.Leader(); return l != 0 && l != old })
	runUntil(t, net, 400, func() bool { return net.Nodes[old].State() != Leader })
	nl := net.Nodes[net.Leader()]
	nl.Propose([]byte("winner"))
	net.Heal()
	runUntil(t, net, 400, func() bool {
		return net.Nodes[old].CommitIndex() == nl.CommitIndex() && nl.CommitIndex() > 0 && net.Nodes[old].LastIndex() == nl.LastIndex()
	})
	for i := uint64(1); i <= nl.LastIndex(); i++ {
		if string(net.Nodes[old].log[i].Data) == "orphan" {
			t.Fatal("uncommitted entry from the old term survived")
		}
	}
}

func TestFileStorageRecoversStateAndTruncations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.jsonl")
	s, err := OpenFileStorage(path, true)
	if err != nil {
		t.Fatal(err)
	}
	s.SaveHardState(HardState{Term: 3, Vote: 2})
	s.Append([]Entry{{Term: 1, Index: 1, Data: []byte("a")}, {Term: 1, Index: 2, Data: []byte("b")}, {Term: 2, Index: 3, Data: []byte("c")}})
	s.TruncateFrom(3)
	s.Append([]Entry{{Term: 3, Index: 3, Data: []byte("d")}})
	s.Close()

	s2, _ := OpenFileStorage(path, true)
	n, err := NewNode(Config{ID: 1, Peers: []int{1, 2, 3}, ElectionTicks: 10, HeartbeatTicks: 2, Storage: s2})
	if err != nil {
		t.Fatal(err)
	}
	if n.Term() != 3 || n.votedFor != 2 {
		t.Fatalf("hard state = term %d vote %d", n.Term(), n.votedFor)
	}
	if n.LastIndex() != 3 || !bytes.Equal(n.log[3].Data, []byte("d")) || n.log[3].Term != 3 {
		t.Fatalf("log not recovered: %+v", n.log)
	}
}

// TestSafetyUnderRandomFaults drives many seeded runs with crashes, restarts,
// partitions and message loss, then checks the two core Raft guarantees:
// at most one leader per term, and every node's committed prefix is identical.
func TestSafetyUnderRandomFaults(t *testing.T) {
	seeds := 60
	if testing.Short() {
		seeds = 10
	}
	for seed := int64(1); seed <= int64(seeds); seed++ {
		net := NewMemNet(5, 10, 2, seed)
		net.DropRate = 0.05
		rng := rand.New(rand.NewSource(seed))
		committed := map[int][]Entry{}
		net.OnCommit = func(id int, e Entry) { committed[id] = append(committed[id], e) }
		leaderOfTerm := map[uint64]int{}
		proposed := 0
		for tick := 0; tick < 3000; tick++ {
			switch r := rng.Intn(1000); {
			case r < 4:
				net.Stop(1 + rng.Intn(5))
			case r < 12:
				net.Start(1 + rng.Intn(5))
			case r < 14:
				net.Isolate(1 + rng.Intn(5))
			case r < 20:
				net.Heal()
			}
			if l := net.Leader(); l != 0 && rng.Intn(4) == 0 {
				if _, _, err := net.Nodes[l].Propose([]byte(fmt.Sprintf("s%d-%d", seed, proposed))); err == nil {
					proposed++
				}
			}
			net.Tick()
			for id, n := range net.Nodes {
				if n.State() == Leader {
					if prev, ok := leaderOfTerm[n.Term()]; ok && prev != id {
						t.Fatalf("seed %d: two leaders in term %d (%d and %d)", seed, n.Term(), prev, id)
					}
					leaderOfTerm[n.Term()] = id
				}
			}
		}
		// Let the cluster settle so every node catches up.
		for id := 1; id <= 5; id++ {
			net.Start(id)
		}
		net.Heal()
		net.DropRate = 0
		for i := 0; i < 600; i++ {
			net.Tick()
		}
		ref := committed[1]
		for id := 2; id <= 5; id++ {
			c := committed[id]
			n := len(c)
			if len(ref) < n {
				n = len(ref)
			}
			for i := 0; i < n; i++ {
				if c[i].Index != ref[i].Index || c[i].Term != ref[i].Term || !bytes.Equal(c[i].Data, ref[i].Data) {
					t.Fatalf("seed %d: node %d diverges from node 1 at committed position %d", seed, id, i)
				}
			}
		}
		if proposed == 0 {
			t.Fatalf("seed %d: no proposals accepted", seed)
		}
	}
}
