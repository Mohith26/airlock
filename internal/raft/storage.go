package raft

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// HardState is the part of Raft state that must survive a crash.
type HardState struct {
	Term uint64 `json:"term"`
	Vote int    `json:"vote"`
}

// Storage persists hard state and log entries. Every method must be durable
// before it returns, because Raft's safety argument assumes a node never
// forgets a vote or an acknowledged entry.
type Storage interface {
	Load() (HardState, []Entry, error)
	SaveHardState(HardState) error
	Append([]Entry) error
	TruncateFrom(index uint64) error
}

// walRecord is one line of the write-ahead log.
type walRecord struct {
	Kind  string     `json:"k"` // "hs", "ent", "trunc"
	HS    *HardState `json:"hs,omitempty"`
	Entry *Entry     `json:"e,omitempty"`
	Index uint64     `json:"i,omitempty"`
}

// FileStorage is an append-only, fsync'd JSON-lines write-ahead log.
// Replaying it in order reconstructs hard state and the log, including
// truncations caused by leader changes.
type FileStorage struct {
	mu   sync.Mutex
	path string
	f    *os.File
	sync bool
}

// OpenFileStorage opens or creates a WAL at path. If fsync is false, writes
// are buffered by the OS (useful for benchmarks, never for real deployments).
func OpenFileStorage(path string, fsync bool) (*FileStorage, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &FileStorage{path: path, f: f, sync: fsync}, nil
}

func (s *FileStorage) Load() (HardState, []Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.f.Seek(0, 0); err != nil {
		return HardState{}, nil, err
	}
	var hs HardState
	var ents []Entry
	sc := bufio.NewScanner(s.f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	line := 0
	for sc.Scan() {
		line++
		var r walRecord
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			// A torn final write from a crash mid-append is expected; anything
			// earlier means the file is damaged.
			if !sc.Scan() {
				break
			}
			return HardState{}, nil, fmt.Errorf("wal %s: corrupt record at line %d", s.path, line)
		}
		switch r.Kind {
		case "hs":
			hs = *r.HS
		case "ent":
			want := uint64(len(ents) + 1)
			if r.Entry.Index != want {
				return HardState{}, nil, fmt.Errorf("wal %s: entry index %d, expected %d", s.path, r.Entry.Index, want)
			}
			ents = append(ents, *r.Entry)
		case "trunc":
			if r.Index >= 1 && int(r.Index-1) <= len(ents) {
				ents = ents[:r.Index-1]
			}
		}
	}
	if err := sc.Err(); err != nil {
		return HardState{}, nil, err
	}
	if _, err := s.f.Seek(0, 2); err != nil {
		return HardState{}, nil, err
	}
	return hs, ents, nil
}

func (s *FileStorage) write(recs ...walRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := bufio.NewWriter(s.f)
	for _, r := range recs {
		b, err := json.Marshal(r)
		if err != nil {
			return err
		}
		w.Write(b)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if s.sync {
		return s.f.Sync()
	}
	return nil
}

func (s *FileStorage) SaveHardState(hs HardState) error {
	return s.write(walRecord{Kind: "hs", HS: &hs})
}

func (s *FileStorage) Append(ents []Entry) error {
	recs := make([]walRecord, len(ents))
	for i := range ents {
		e := ents[i]
		recs[i] = walRecord{Kind: "ent", Entry: &e}
	}
	return s.write(recs...)
}

func (s *FileStorage) TruncateFrom(index uint64) error {
	return s.write(walRecord{Kind: "trunc", Index: index})
}

func (s *FileStorage) Close() error { return s.f.Close() }
