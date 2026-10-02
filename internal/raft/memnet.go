package raft

import "math/rand"

// MemNet is a deterministic in-memory network for a group of nodes. Each
// message is delivered after a fixed number of ticks unless the link is cut,
// the receiver is down, or the seeded RNG drops it.
type MemNet struct {
	Nodes    map[int]*Node
	down     map[int]bool
	cut      map[[2]int]bool
	inflight []timed
	now      int
	Delay    int     // ticks per hop; at least 1
	DropRate float64 // probability any message is lost
	rng      *rand.Rand

	// Committed receives every entry each node commits, keyed by node id.
	OnCommit func(node int, e Entry)
	Sent     uint64
	Dropped  uint64
}

type timed struct {
	at  int
	msg Message
}

// NewMemNet builds a cluster of size nodes with ids 1..size.
func NewMemNet(size int, electionTicks, heartbeatTicks int, seed int64) *MemNet {
	peers := make([]int, size)
	for i := range peers {
		peers[i] = i + 1
	}
	net := &MemNet{Nodes: map[int]*Node{}, down: map[int]bool{}, cut: map[[2]int]bool{}, Delay: 1, rng: rand.New(rand.NewSource(seed))}
	for _, id := range peers {
		n, err := NewNode(Config{ID: id, Peers: peers, ElectionTicks: electionTicks, HeartbeatTicks: heartbeatTicks, Seed: seed})
		if err != nil {
			panic(err)
		}
		net.Nodes[id] = n
	}
	return net
}

// Now is the current tick.
func (m *MemNet) Now() int { return m.now }

// Stop crashes a node: it stops ticking and receiving. Its durable state is kept.
func (m *MemNet) Stop(id int) { m.down[id] = true }

// Start restarts a stopped node from its retained state, as a process restart
// from its write-ahead log would.
func (m *MemNet) Start(id int) {
	if m.down[id] {
		delete(m.down, id)
		n := m.Nodes[id]
		n.outbox = nil
		n.becomeFollower(n.term, 0)
	}
}

func (m *MemNet) IsDown(id int) bool { return m.down[id] }

// Isolate cuts every link between id and the rest of the cluster.
func (m *MemNet) Isolate(id int) {
	for other := range m.Nodes {
		if other != id {
			m.cut[[2]int{id, other}] = true
			m.cut[[2]int{other, id}] = true
		}
	}
}

// Heal restores every link.
func (m *MemNet) Heal() { m.cut = map[[2]int]bool{} }

// Leader returns the id of the live leader with the highest term, or 0.
func (m *MemNet) Leader() int {
	best, term := 0, uint64(0)
	for id, n := range m.Nodes {
		if !m.down[id] && n.State() == Leader && n.Term() >= term {
			best, term = id, n.Term()
		}
	}
	return best
}

// Tick advances the whole cluster by one tick.
func (m *MemNet) Tick() {
	m.now++
	for id := 1; id <= len(m.Nodes); id++ {
		if !m.down[id] {
			m.Nodes[id].Tick()
		}
	}
	m.Flush()
	// Deliver messages that are due.
	due := m.inflight[:0]
	var deliver []Message
	for _, t := range m.inflight {
		if t.at <= m.now {
			deliver = append(deliver, t.msg)
		} else {
			due = append(due, t)
		}
	}
	m.inflight = due
	for _, msg := range deliver {
		if m.down[msg.To] || m.cut[[2]int{msg.From, msg.To}] {
			m.Dropped++
			continue
		}
		m.Nodes[msg.To].Step(msg)
	}
	m.Flush()
}

// Flush collects outbound messages and committed entries from every live node.
func (m *MemNet) Flush() {
	delay := m.Delay
	if delay < 1 {
		delay = 1
	}
	for id := 1; id <= len(m.Nodes); id++ {
		if m.down[id] {
			continue
		}
		msgs, committed := m.Nodes[id].Ready()
		for _, msg := range msgs {
			m.Sent++
			if m.DropRate > 0 && m.rng.Float64() < m.DropRate {
				m.Dropped++
				continue
			}
			m.inflight = append(m.inflight, timed{at: m.now + delay, msg: msg})
		}
		if m.OnCommit != nil {
			for _, e := range committed {
				m.OnCommit(id, e)
			}
		}
	}
}
