// Package audit is an append-only, hash-chained event log. Each event commits
// to the hash of the one before it, so deleting, reordering or editing any
// past event breaks verification from that point on.
package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
)

// Event is one audit record. At is a timestamp in milliseconds supplied by
// the caller, which keeps replays deterministic.
type Event struct {
	Seq     uint64            `json:"seq"`
	At      int64             `json:"at_ms"`
	Type    string            `json:"type"`
	Actor   string            `json:"actor"`
	Details map[string]string `json:"details,omitempty"`
	Prev    string            `json:"prev"`
	Hash    string            `json:"hash"`
}

const genesis = "0000000000000000000000000000000000000000000000000000000000000000"

func hashOf(e Event) string {
	b, _ := json.Marshal(struct {
		Seq     uint64            `json:"seq"`
		At      int64             `json:"at"`
		Type    string            `json:"type"`
		Actor   string            `json:"actor"`
		Details map[string]string `json:"details"`
		Prev    string            `json:"prev"`
	}{e.Seq, e.At, e.Type, e.Actor, e.Details, e.Prev}) // encoding/json sorts map keys
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// Log is safe for concurrent use.
type Log struct {
	mu     sync.Mutex
	events []Event
}

// Append records an event and returns it with its chain hash filled in.
func (l *Log) Append(at int64, typ, actor string, details map[string]string) Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	prev := genesis
	if n := len(l.events); n > 0 {
		prev = l.events[n-1].Hash
	}
	e := Event{Seq: uint64(len(l.events) + 1), At: at, Type: typ, Actor: actor, Details: details, Prev: prev}
	e.Hash = hashOf(e)
	l.events = append(l.events, e)
	return e
}

// Events returns a copy of the log.
func (l *Log) Events() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Event(nil), l.events...)
}

func (l *Log) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.events)
}

// Head is the hash of the latest event.
func (l *Log) Head() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.events) == 0 {
		return genesis
	}
	return l.events[len(l.events)-1].Hash
}

// Verify checks an exported chain end to end.
func Verify(events []Event) error {
	prev := genesis
	for i, e := range events {
		if e.Seq != uint64(i+1) {
			return fmt.Errorf("audit: sequence gap at position %d", i)
		}
		if e.Prev != prev {
			return fmt.Errorf("audit: chain broken at seq %d", e.Seq)
		}
		if hashOf(e) != e.Hash {
			return fmt.Errorf("audit: event %d was modified", e.Seq)
		}
		prev = e.Hash
	}
	return nil
}
