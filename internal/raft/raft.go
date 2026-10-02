// Package raft implements the Raft consensus algorithm as a deterministic,
// tick-driven state machine. A Node never touches the network or the clock
// directly: the caller advances time with Tick, feeds inbound messages with
// Step, and drains outbound messages and newly committed entries with Ready.
// That separation lets the same code run over real HTTP in production and
// over a seeded, fault-injectable in-memory network in tests.
package raft

import (
	"errors"
	"math/rand"
	"sort"
)

// State is the role a node currently plays.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	default:
		return "follower"
	}
}

// MsgType identifies a Raft RPC.
type MsgType int

const (
	MsgVote MsgType = iota + 1
	MsgVoteResp
	MsgAppend
	MsgAppendResp
)

// Entry is one replicated log record.
type Entry struct {
	Term  uint64 `json:"term"`
	Index uint64 `json:"index"`
	Data  []byte `json:"data,omitempty"`
}

// Message is a Raft RPC or RPC response.
type Message struct {
	Type    MsgType `json:"type"`
	From    int     `json:"from"`
	To      int     `json:"to"`
	Term    uint64  `json:"term"`
	LogTerm uint64  `json:"log_term,omitempty"` // vote: candidate last log term; append: prev log term
	Index   uint64  `json:"index,omitempty"`    // vote: candidate last index; append: prev index; resp: match or hint
	Entries []Entry `json:"entries,omitempty"`
	Commit  uint64  `json:"commit,omitempty"`
	Reject  bool    `json:"reject,omitempty"`
}

// Config holds timing parameters expressed in ticks.
type Config struct {
	ID             int
	Peers          []int // every member including ID
	ElectionTicks  int   // base election timeout; actual timeout is randomized in [base, 2*base)
	HeartbeatTicks int
	Seed           int64
	Storage        Storage // optional; nil keeps state in memory only
}

// ErrNotLeader is returned when a proposal reaches a non-leader.
var ErrNotLeader = errors.New("raft: not the leader")

// Node is a single Raft participant.
type Node struct {
	id    int
	peers []int

	term     uint64
	votedFor int
	log      []Entry // log[0] is a sentinel at index 0, term 0
	commit   uint64
	applied  uint64

	state  State
	leader int

	electionBase     int
	electionTimeout  int
	electionElapsed  int
	heartbeatTicks   int
	heartbeatElapsed int
	rng              *rand.Rand

	votes  map[int]bool
	next   map[int]uint64
	match  map[int]uint64
	active map[int]bool // peers heard from during the current check-quorum window

	outbox  []Message
	storage Storage

	// Elections counts how many times this node became leader.
	Elections uint64
}

// NewNode builds a node, restoring durable state from cfg.Storage if present.
func NewNode(cfg Config) (*Node, error) {
	if cfg.ElectionTicks <= 0 {
		cfg.ElectionTicks = 15
	}
	if cfg.HeartbeatTicks <= 0 {
		cfg.HeartbeatTicks = 3
	}
	if cfg.HeartbeatTicks >= cfg.ElectionTicks {
		return nil, errors.New("raft: heartbeat must be shorter than election timeout")
	}
	peers := append([]int(nil), cfg.Peers...)
	sort.Ints(peers)
	n := &Node{
		id:             cfg.ID,
		peers:          peers,
		log:            []Entry{{}},
		electionBase:   cfg.ElectionTicks,
		heartbeatTicks: cfg.HeartbeatTicks,
		rng:            rand.New(rand.NewSource(cfg.Seed + int64(cfg.ID)*7919)),
		storage:        cfg.Storage,
	}
	if n.storage != nil {
		hs, entries, err := n.storage.Load()
		if err != nil {
			return nil, err
		}
		n.term, n.votedFor = hs.Term, hs.Vote
		n.log = append(n.log, entries...)
	}
	n.becomeFollower(n.term, 0)
	return n, nil
}

func (n *Node) ID() int             { return n.id }
func (n *Node) State() State        { return n.state }
func (n *Node) Term() uint64        { return n.term }
func (n *Node) Leader() int         { return n.leader }
func (n *Node) CommitIndex() uint64 { return n.commit }
func (n *Node) LastIndex() uint64   { return n.log[len(n.log)-1].Index }
func (n *Node) lastTerm() uint64    { return n.log[len(n.log)-1].Term }

func (n *Node) quorum() int { return len(n.peers)/2 + 1 }

func (n *Node) termAt(i uint64) uint64 {
	if i >= uint64(len(n.log)) {
		return 0
	}
	return n.log[i].Term
}

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.electionTimeout = n.electionBase + n.rng.Intn(n.electionBase)
}

func (n *Node) persistHardState() {
	if n.storage != nil {
		if err := n.storage.SaveHardState(HardState{Term: n.term, Vote: n.votedFor}); err != nil {
			panic(err) // losing a vote or term record would break safety; crash instead
		}
	}
}

func (n *Node) becomeFollower(term uint64, leader int) {
	if term != n.term {
		n.term = term
		n.votedFor = 0
		n.persistHardState()
	}
	n.state = Follower
	n.leader = leader
	n.resetElectionTimer()
}

func (n *Node) campaign() {
	n.state = Candidate
	n.term++
	n.votedFor = n.id
	n.leader = 0
	n.persistHardState()
	n.resetElectionTimer()
	n.votes = map[int]bool{n.id: true}
	if len(n.votes) >= n.quorum() {
		n.becomeLeader()
		return
	}
	for _, p := range n.peers {
		if p != n.id {
			n.send(Message{Type: MsgVote, To: p, Index: n.LastIndex(), LogTerm: n.lastTerm()})
		}
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.leader = n.id
	n.Elections++
	n.heartbeatElapsed = 0
	n.electionElapsed = 0
	n.next = map[int]uint64{}
	n.match = map[int]uint64{}
	n.active = map[int]bool{n.id: true}
	for _, p := range n.peers {
		n.next[p] = n.LastIndex() + 1
	}
	// A no-op in the new term lets the leader commit entries from earlier terms.
	n.appendLocal(nil)
	n.broadcastAppend()
}

func (n *Node) appendLocal(data []byte) Entry {
	e := Entry{Term: n.term, Index: n.LastIndex() + 1, Data: data}
	n.log = append(n.log, e)
	if n.storage != nil {
		if err := n.storage.Append([]Entry{e}); err != nil {
			panic(err)
		}
	}
	n.match[n.id] = e.Index
	n.next[n.id] = e.Index + 1
	n.maybeCommit()
	return e
}

func (n *Node) send(m Message) {
	m.From = n.id
	m.Term = n.term
	n.outbox = append(n.outbox, m)
}

func (n *Node) sendAppend(to int) {
	next := n.next[to]
	if next < 1 {
		next = 1
	}
	prev := next - 1
	var ents []Entry
	if next <= n.LastIndex() {
		ents = append([]Entry(nil), n.log[next:]...)
		if len(ents) > 256 {
			ents = ents[:256]
		}
	}
	n.send(Message{Type: MsgAppend, To: to, Index: prev, LogTerm: n.termAt(prev), Entries: ents, Commit: n.commit})
}

func (n *Node) broadcastAppend() {
	for _, p := range n.peers {
		if p != n.id {
			n.sendAppend(p)
		}
	}
}

// Tick advances logical time by one unit.
func (n *Node) Tick() {
	switch n.state {
	case Leader:
		n.heartbeatElapsed++
		n.electionElapsed++
		if n.heartbeatElapsed >= n.heartbeatTicks {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
		// Check quorum: a leader that cannot reach a majority steps down so
		// clients stop sending it writes it can never commit.
		if n.electionElapsed >= n.electionBase*2 {
			n.electionElapsed = 0
			if len(n.active) < n.quorum() {
				n.becomeFollower(n.term, 0)
				return
			}
			n.active = map[int]bool{n.id: true}
		}
	default:
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			n.campaign()
		}
	}
}

// Propose appends data to the log if this node is the leader.
func (n *Node) Propose(data []byte) (index, term uint64, err error) {
	if n.state != Leader {
		return 0, 0, ErrNotLeader
	}
	e := n.appendLocal(data)
	n.broadcastAppend()
	return e.Index, e.Term, nil
}

// Step processes one inbound message.
func (n *Node) Step(m Message) {
	if m.Term > n.term {
		leader := 0
		if m.Type == MsgAppend {
			leader = m.From
		}
		n.becomeFollower(m.Term, leader)
	}
	if m.Term < n.term {
		// Stale sender: tell it about the newer term so it steps down.
		switch m.Type {
		case MsgAppend:
			n.send(Message{Type: MsgAppendResp, To: m.From, Reject: true, Index: n.LastIndex()})
		case MsgVote:
			n.send(Message{Type: MsgVoteResp, To: m.From, Reject: true})
		}
		return
	}
	switch m.Type {
	case MsgVote:
		upToDate := m.LogTerm > n.lastTerm() || (m.LogTerm == n.lastTerm() && m.Index >= n.LastIndex())
		canVote := n.votedFor == 0 || n.votedFor == m.From
		if canVote && upToDate && n.state != Leader {
			n.votedFor = m.From
			n.persistHardState()
			n.resetElectionTimer()
			n.send(Message{Type: MsgVoteResp, To: m.From})
		} else {
			n.send(Message{Type: MsgVoteResp, To: m.From, Reject: true})
		}
	case MsgVoteResp:
		if n.state != Candidate {
			return
		}
		n.votes[m.From] = !m.Reject
		granted := 0
		for _, ok := range n.votes {
			if ok {
				granted++
			}
		}
		if granted >= n.quorum() {
			n.becomeLeader()
		}
	case MsgAppend:
		n.handleAppend(m)
	case MsgAppendResp:
		if n.state != Leader {
			return
		}
		n.active[m.From] = true
		if m.Reject {
			// Back off to the follower's hint and retry immediately.
			next := n.next[m.From] - 1
			if m.Index+1 < next {
				next = m.Index + 1
			}
			if next < 1 {
				next = 1
			}
			n.next[m.From] = next
			n.sendAppend(m.From)
			return
		}
		if m.Index > n.match[m.From] {
			n.match[m.From] = m.Index
		}
		if m.Index+1 > n.next[m.From] {
			n.next[m.From] = m.Index + 1
		}
		if n.maybeCommit() {
			n.broadcastAppend() // tell followers the new commit index promptly
		} else if n.next[m.From] <= n.LastIndex() {
			n.sendAppend(m.From)
		}
	}
}

func (n *Node) handleAppend(m Message) {
	if n.state != Follower {
		n.becomeFollower(m.Term, m.From)
	}
	n.leader = m.From
	n.resetElectionTimer()
	if m.Index > n.LastIndex() || n.termAt(m.Index) != m.LogTerm {
		hint := n.LastIndex()
		if m.Index <= hint {
			hint = m.Index - 1
		}
		n.send(Message{Type: MsgAppendResp, To: m.From, Reject: true, Index: hint})
		return
	}
	var fresh []Entry
	for i, e := range m.Entries {
		if e.Index <= n.LastIndex() {
			if n.termAt(e.Index) == e.Term {
				continue
			}
			// Conflict: drop our divergent suffix. Committed entries never conflict.
			n.log = n.log[:e.Index]
			if n.storage != nil {
				if err := n.storage.TruncateFrom(e.Index); err != nil {
					panic(err)
				}
			}
		}
		fresh = m.Entries[i:]
		break
	}
	if len(fresh) > 0 {
		n.log = append(n.log, fresh...)
		if n.storage != nil {
			if err := n.storage.Append(fresh); err != nil {
				panic(err)
			}
		}
	}
	lastNew := m.Index + uint64(len(m.Entries))
	if m.Commit > n.commit {
		c := m.Commit
		if lastNew < c {
			c = lastNew
		}
		if c > n.commit {
			n.commit = c
		}
	}
	n.send(Message{Type: MsgAppendResp, To: m.From, Index: lastNew})
}

// maybeCommit advances the commit index to the highest entry from the current
// term that a majority has stored. It reports whether the index moved.
func (n *Node) maybeCommit() bool {
	if n.state != Leader {
		return false
	}
	matches := make([]uint64, 0, len(n.peers))
	for _, p := range n.peers {
		matches = append(matches, n.match[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	candidate := matches[n.quorum()-1]
	if candidate > n.commit && n.termAt(candidate) == n.term {
		n.commit = candidate
		return true
	}
	return false
}

// Ready drains outbound messages and entries that became committed since the
// previous call. Entries must be applied to the state machine in order.
func (n *Node) Ready() (msgs []Message, committed []Entry) {
	msgs, n.outbox = n.outbox, nil
	if n.commit > n.applied {
		committed = append([]Entry(nil), n.log[n.applied+1:n.commit+1]...)
		n.applied = n.commit
	}
	return msgs, committed
}

// Status is a read-only summary for dashboards and metrics.
type Status struct {
	ID        int    `json:"id"`
	State     string `json:"state"`
	Term      uint64 `json:"term"`
	Leader    int    `json:"leader"`
	Commit    uint64 `json:"commit"`
	LastIndex uint64 `json:"last_index"`
}

func (n *Node) Status() Status {
	return Status{ID: n.id, State: n.state.String(), Term: n.term, Leader: n.leader, Commit: n.commit, LastIndex: n.LastIndex()}
}
