package agent

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mohith26/airlock/internal/bundle"
	"github.com/Mohith26/airlock/internal/control"
)

// fakeCP applies commands straight to an FSM, with switchable reachability.
type fakeCP struct {
	f        *control.FSM
	idx      uint64
	down     bool
	blobs    map[string][]byte
	corrupt  bool
	acksSeen int
}

func (c *fakeCP) apply(cmd control.Command) control.Result {
	c.idx++
	return c.f.Apply(c.idx, cmd.Encode())
}

func (c *fakeCP) Pending(r string) ([]control.Job, error) {
	if c.down {
		return nil, ErrUnreachable
	}
	return c.f.Pending(r), nil
}
func (c *fakeCP) Release(s, v string) (bundle.Manifest, error) {
	if c.down {
		return bundle.Manifest{}, ErrUnreachable
	}
	m, _ := c.f.Release(s, v)
	return m, nil
}
func (c *fakeCP) Artifact(d string) ([]byte, error) {
	if c.down {
		return nil, ErrUnreachable
	}
	b := append([]byte(nil), c.blobs[d]...)
	if c.corrupt {
		b[0] ^= 1
	}
	return b, nil
}
func (c *fakeCP) Ack(id, status, detail string) error {
	if c.down {
		return ErrUnreachable
	}
	c.acksSeen++
	c.apply(control.Command{Op: "ack", JobID: id, Status: status, Detail: detail})
	return nil
}

func newEnv(t *testing.T) (*fakeCP, *bundle.Signer, *bundle.Signer) {
	rel, _ := bundle.NewSigner(bytes.Repeat([]byte{1}, 32))
	cpk, _ := bundle.NewSigner(bytes.Repeat([]byte{2}, 32))
	cp := &fakeCP{f: control.New(), blobs: map[string][]byte{}}
	for _, v := range []string{"1.0.0", "1.1.0", "1.2.0"} {
		art := []byte("svc build " + v)
		m := rel.Sign("svc", v, art, time.Unix(0, 0))
		cp.blobs[m.Digest] = art
		cp.apply(control.Command{Op: "release", Manifest: &m})
	}
	return cp, rel, cpk
}

func newAgent(rel, cpk *bundle.Signer, installs *int) *Agent {
	return New(Config{Region: "east", ReleaseKeys: bundle.NewKeyring(rel.Pub), PackageKeys: bundle.NewKeyring(cpk.Pub),
		Install: func(bundle.Manifest, []byte) error { *installs++; return nil }})
}

func TestSyncInstallsAndAcks(t *testing.T) {
	cp, rel, cpk := newEnv(t)
	n := 0
	a := newAgent(rel, cpk, &n)
	cp.apply(control.Command{Op: "deploy", JobID: "j1", Region: "east", Service: "svc", Version: "1.0.0"})
	if err := a.Sync(cp); err != nil {
		t.Fatal(err)
	}
	if j, _ := cp.f.Job("j1"); j.Status != control.Succeeded || n != 1 {
		t.Fatalf("status %s installs %d", j.Status, n)
	}
}

func TestCrashBetweenInstallAndAckDoesNotReinstall(t *testing.T) {
	cp, rel, cpk := newEnv(t)
	n := 0
	a := newAgent(rel, cpk, &n)
	cp.apply(control.Command{Op: "deploy", JobID: "j1", Region: "east", Service: "svc", Version: "1.0.0"})
	a.CrashBeforeNextAck()
	a.Sync(cp)
	if a.Alive() || n != 1 {
		t.Fatalf("expected crash after one install; alive=%v installs=%d", a.Alive(), n)
	}
	if j, _ := cp.f.Job("j1"); j.Status != control.Pending {
		t.Fatal("ack should have been lost in the crash")
	}
	a.Restart()
	a.Sync(cp)
	if n != 1 {
		t.Fatalf("reinstalled after restart: %d installs", n)
	}
	if j, _ := cp.f.Job("j1"); j.Status != control.Succeeded {
		t.Fatalf("status %s", j.Status)
	}
	if a.Stats().DuplicateDeliveries != 1 {
		t.Fatal("duplicate delivery not counted")
	}
}

func TestLedgerSurvivesProcessRestart(t *testing.T) {
	cp, rel, cpk := newEnv(t)
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	l1, _ := OpenFileLedger(path)
	n := 0
	a := New(Config{Region: "east", ReleaseKeys: bundle.NewKeyring(rel.Pub), PackageKeys: bundle.NewKeyring(cpk.Pub), Ledger: l1,
		Install: func(bundle.Manifest, []byte) error { n++; return nil }})
	cp.apply(control.Command{Op: "deploy", JobID: "j1", Region: "east", Service: "svc", Version: "1.0.0"})
	a.CrashBeforeNextAck()
	a.Sync(cp)
	l1.Close()

	// A brand-new process reads the same ledger file.
	l2, _ := OpenFileLedger(path)
	b := New(Config{Region: "east", ReleaseKeys: bundle.NewKeyring(rel.Pub), PackageKeys: bundle.NewKeyring(cpk.Pub), Ledger: l2,
		Install: func(bundle.Manifest, []byte) error { n++; return nil }})
	if v, g := b.Version(); v != "1.0.0" || g != 1 {
		t.Fatalf("recovered version %s gen %d", v, g)
	}
	b.Sync(cp)
	if n != 1 {
		t.Fatalf("new process reinstalled: %d installs", n)
	}
}

func TestPartitionBuffersAndBacklogSkipsToNewest(t *testing.T) {
	cp, rel, cpk := newEnv(t)
	n := 0
	a := newAgent(rel, cpk, &n)
	cp.down = true
	cp.apply(control.Command{Op: "deploy", JobID: "j1", Region: "east", Service: "svc", Version: "1.0.0"})
	cp.apply(control.Command{Op: "deploy", JobID: "j2", Region: "east", Service: "svc", Version: "1.1.0"})
	cp.apply(control.Command{Op: "deploy", JobID: "j3", Region: "east", Service: "svc", Version: "1.2.0"})
	if err := a.Sync(cp); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("got %v", err)
	}
	cp.down = false
	a.Sync(cp)
	if n != 1 {
		t.Fatalf("installed %d times, want only the newest", n)
	}
	if v, _ := a.Version(); v != "1.2.0" {
		t.Fatalf("running %s", v)
	}
	for _, id := range []string{"j1", "j2"} {
		if j, _ := cp.f.Job(id); j.Status != control.Superseded {
			t.Fatalf("%s status %s", id, j.Status)
		}
	}
}

func TestCorruptDownloadNeverInstalled(t *testing.T) {
	cp, rel, cpk := newEnv(t)
	n := 0
	a := newAgent(rel, cpk, &n)
	cp.apply(control.Command{Op: "deploy", JobID: "j1", Region: "east", Service: "svc", Version: "1.0.0"})
	cp.corrupt = true
	a.Sync(cp)
	if n != 0 {
		t.Fatal("installed corrupt bytes")
	}
	if j, _ := cp.f.Job("j1"); j.Status != control.Pending {
		t.Fatal("job should stay pending until clean bytes arrive")
	}
	cp.corrupt = false
	a.Sync(cp)
	if n != 1 {
		t.Fatal("clean retry did not install")
	}
}

func TestCorruptCacheDiscardedAndRefetched(t *testing.T) {
	cp, rel, cpk := newEnv(t)
	n := 0
	a := newAgent(rel, cpk, &n)
	cp.apply(control.Command{Op: "deploy", JobID: "j1", Region: "east", Service: "svc", Version: "1.0.0"})
	a.Sync(cp)
	// Deploy a different version, then roll back to 1.0.0 whose bytes are cached.
	cp.apply(control.Command{Op: "deploy", JobID: "j2", Region: "east", Service: "svc", Version: "1.1.0"})
	a.Sync(cp)
	m, _ := cp.f.Release("svc", "1.0.0")
	if !a.CorruptCached(m.Digest) {
		t.Fatal("nothing cached")
	}
	cp.apply(control.Command{Op: "deploy", JobID: "j3", Region: "east", Service: "svc", Version: "1.0.0"})
	a.Sync(cp)
	st := a.Stats()
	if st.ArtifactsRejected != 1 || st.Refetched != 1 || n != 3 {
		t.Fatalf("stats %+v installs %d", st, n)
	}
}

func TestOfflinePackageRules(t *testing.T) {
	cp, rel, cpk := newEnv(t)
	n := 0
	a := newAgent(rel, cpk, &n)
	cp.apply(control.Command{Op: "deploy", JobID: "j1", Region: "east", Service: "svc", Version: "1.0.0"})
	cp.apply(control.Command{Op: "deploy", JobID: "j2", Region: "east", Service: "svc", Version: "1.1.0"})
	build := func(seq uint64) bundle.Package {
		p := bundle.Package{Region: "east", Sequence: seq, Artifacts: map[string][]byte{}}
		for _, j := range cp.f.Pending("east") {
			m, _ := cp.f.Release(j.Service, j.Version)
			p.Jobs = append(p.Jobs, bundle.OfflineJob{JobID: j.ID, Generation: j.Generation, Manifest: m})
			p.Artifacts[m.Digest] = cp.blobs[m.Digest]
		}
		return cpk.SignPackage(p)
	}
	rogue, _ := bundle.NewSigner(bytes.Repeat([]byte{7}, 32))
	p := build(1)
	forged := rogue.SignPackage(p)
	if _, err := a.Import(forged); err == nil {
		t.Fatal("package from untrusted key accepted")
	}
	other := p
	other.Region = "west"
	if _, err := a.Import(cpk.SignPackage(other)); err == nil {
		t.Fatal("package for another region accepted")
	}
	acks, err := a.Import(p)
	if err != nil || len(acks) != 2 || n != 1 {
		t.Fatalf("import: acks=%v err=%v installs=%d", acks, err, n)
	}
	if _, err := a.Import(p); err == nil {
		t.Fatal("replayed package accepted")
	}
}
