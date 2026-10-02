package agent

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

// Record is the agent's durable memory of one job's outcome.
type Record struct {
	JobID      string `json:"job_id"`
	Status     string `json:"status"`
	Version    string `json:"version"`
	Generation uint64 `json:"generation"`
	Detail     string `json:"detail,omitempty"`
}

// Ledger is the agent's idempotency ledger. Put must be durable on return.
type Ledger interface {
	Get(jobID string) (Record, bool)
	Put(Record)
	Records() []Record
}

// MemLedger keeps records in memory. In the simulator it models a ledger
// that survives worker crashes, the way a file on local disk would.
type MemLedger struct {
	mu    sync.Mutex
	m     map[string]Record
	order []string
}

func NewMemLedger() *MemLedger { return &MemLedger{m: map[string]Record{}} }

func (l *MemLedger) Get(id string) (Record, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.m[id]
	return r, ok
}

func (l *MemLedger) Put(r Record) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.m[r.JobID]; !ok {
		l.order = append(l.order, r.JobID)
	}
	l.m[r.JobID] = r
}

func (l *MemLedger) Records() []Record {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Record, 0, len(l.order))
	for _, id := range l.order {
		out = append(out, l.m[id])
	}
	return out
}

// FileLedger is a fsync'd JSON-lines ledger on local disk.
type FileLedger struct {
	mem *MemLedger
	mu  sync.Mutex
	f   *os.File
}

// OpenFileLedger loads existing records from path and appends new ones.
func OpenFileLedger(path string) (*FileLedger, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	l := &FileLedger{mem: NewMemLedger(), f: f}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r Record
		if json.Unmarshal(sc.Bytes(), &r) == nil && r.JobID != "" {
			l.mem.Put(r)
		}
	}
	return l, sc.Err()
}

func (l *FileLedger) Get(id string) (Record, bool) { return l.mem.Get(id) }
func (l *FileLedger) Records() []Record            { return l.mem.Records() }

func (l *FileLedger) Put(r Record) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, _ := json.Marshal(r)
	b = append(b, '\n')
	if _, err := l.f.Write(b); err != nil {
		panic(err) // an agent that cannot record outcomes must not keep executing
	}
	if err := l.f.Sync(); err != nil {
		panic(err)
	}
	l.mem.Put(r)
}

func (l *FileLedger) Close() error { return l.f.Close() }
