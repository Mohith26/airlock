package control

import (
	"bytes"
	"testing"
	"time"

	"github.com/Mohith26/airlock/internal/bundle"
)

func setup(t *testing.T) (*FSM, *uint64) {
	t.Helper()
	f := New()
	s, _ := bundle.NewSigner(bytes.Repeat([]byte{9}, 32))
	idx := uint64(0)
	for _, v := range []string{"1.0.0", "1.1.0"} {
		art := []byte("svc " + v)
		m := s.Sign("svc", v, art, time.Unix(0, 0))
		idx++
		if r := f.Apply(idx, Command{Op: "release", Manifest: &m, Artifact: art}.Encode()); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	return f, &idx
}

func apply(f *FSM, idx *uint64, c Command) Result {
	*idx++
	return f.Apply(*idx, c.Encode())
}

func TestDeployAssignsGenerationsPerRegion(t *testing.T) {
	f, idx := setup(t)
	apply(f, idx, Command{Op: "deploy", JobID: "a", Region: "east", Service: "svc", Version: "1.0.0"})
	apply(f, idx, Command{Op: "deploy", JobID: "b", Region: "east", Service: "svc", Version: "1.1.0"})
	apply(f, idx, Command{Op: "deploy", JobID: "c", Region: "west", Service: "svc", Version: "1.0.0"})
	a, _ := f.Job("a")
	b, _ := f.Job("b")
	c, _ := f.Job("c")
	if a.Generation != 1 || b.Generation != 2 || c.Generation != 1 {
		t.Fatalf("generations a=%d b=%d c=%d", a.Generation, b.Generation, c.Generation)
	}
	if p := f.Pending("east"); len(p) != 2 || p[0].ID != "a" || p[1].ID != "b" {
		t.Fatalf("pending order wrong: %+v", p)
	}
}

func TestDuplicateSubmitAndAckAreNoOps(t *testing.T) {
	f, idx := setup(t)
	apply(f, idx, Command{Op: "deploy", JobID: "a", Region: "east", Service: "svc", Version: "1.0.0"})
	r := apply(f, idx, Command{Op: "deploy", JobID: "a", Region: "east", Service: "svc", Version: "1.1.0"})
	if !r.Duplicate || f.DuplicateSubmits != 1 {
		t.Fatalf("duplicate not suppressed: %+v", r)
	}
	if j, _ := f.Job("a"); j.Version != "1.0.0" {
		t.Fatal("duplicate submit changed the job")
	}
	apply(f, idx, Command{Op: "ack", JobID: "a", Status: Succeeded})
	r = apply(f, idx, Command{Op: "ack", JobID: "a", Status: Rejected})
	if !r.Duplicate || f.DuplicateAcks != 1 {
		t.Fatal("second ack not ignored")
	}
	if j, _ := f.Job("a"); j.Status != Succeeded {
		t.Fatalf("status flipped to %s", j.Status)
	}
	reg := f.Regions()[0]
	if reg.ConfirmedGen != 1 || reg.ConfirmedVersion != "1.0.0" {
		t.Fatalf("region not confirmed: %+v", reg)
	}
}

func TestUnknownReleaseIsInvalidNotPending(t *testing.T) {
	f, idx := setup(t)
	apply(f, idx, Command{Op: "deploy", JobID: "x", Region: "east", Service: "svc", Version: "9.9.9"})
	if j, _ := f.Job("x"); j.Status != Invalid {
		t.Fatalf("status %s", j.Status)
	}
	if len(f.Pending("east")) != 0 {
		t.Fatal("invalid job is pending")
	}
}

func TestReplayedEntryIndexIgnored(t *testing.T) {
	f, idx := setup(t)
	apply(f, idx, Command{Op: "deploy", JobID: "a", Region: "east", Service: "svc", Version: "1.0.0"})
	before := f.Digest()
	if r := f.Apply(*idx, Command{Op: "deploy", JobID: "z", Region: "east", Service: "svc", Version: "1.0.0"}.Encode()); !r.Duplicate {
		t.Fatal("re-applied index accepted")
	}
	if f.Digest() != before {
		t.Fatal("state changed")
	}
}

func TestArtifactMustMatchManifest(t *testing.T) {
	f := New()
	s, _ := bundle.NewSigner(bytes.Repeat([]byte{9}, 32))
	m := s.Sign("svc", "1.0.0", []byte("real"), time.Unix(0, 0))
	if r := f.Apply(1, Command{Op: "release", Manifest: &m, Artifact: []byte("fake")}.Encode()); r.Err == nil {
		t.Fatal("mismatched artifact stored")
	}
}

func TestReplicasApplyingSameLogAgree(t *testing.T) {
	a, ia := setup(t)
	b, ib := setup(t)
	cmds := []Command{
		{Op: "deploy", JobID: "1", Region: "e", Service: "svc", Version: "1.0.0"},
		{Op: "deploy", JobID: "2", Region: "e", Service: "svc", Version: "1.1.0"},
		{Op: "ack", JobID: "1", Status: Superseded},
		{Op: "ack", JobID: "2", Status: Succeeded},
		{Op: "deploy", JobID: "2", Region: "e", Service: "svc", Version: "1.1.0"},
	}
	for _, c := range cmds {
		apply(a, ia, c)
		apply(b, ib, c)
	}
	if a.Digest() != b.Digest() {
		t.Fatal("replicas diverged")
	}
}
