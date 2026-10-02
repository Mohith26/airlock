package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/Mohith26/airlock/internal/agent"
	"github.com/Mohith26/airlock/internal/bundle"
	"github.com/Mohith26/airlock/internal/control"
)

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func confirmed(cl *Client, region, version string) bool {
	st, err := cl.Status()
	if err != nil {
		return false
	}
	for _, r := range st.Regions {
		if r.Name == region {
			return r.ConfirmedVersion == version && r.ConfirmedGen == r.DesiredGen
		}
	}
	return false
}

// TestEndToEndOverHTTP runs three replicas and three region agents over real
// sockets: deploy, crash the leader, deploy again, carry a package across an
// air gap, restart the crashed replica from its WAL, and check convergence.
func TestEndToEndOverHTTP(t *testing.T) {
	if testing.Short() {
		t.Skip("networked test")
	}
	lc, err := StartLocalCluster(3, t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer lc.Close()
	if _, err := lc.WaitLeader(0, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	cl := lc.Client()

	agents := map[string]*agent.Agent{}
	for _, r := range []string{"us-east", "eu-west", "adc-1"} {
		agents[r] = agent.New(agent.Config{Region: r, ReleaseKeys: bundle.NewKeyring(lc.Release.Pub), PackageKeys: bundle.NewKeyring(lc.CPKey.Pub)})
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ac := lc.Client()
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
				agents["us-east"].Sync(ac)
				agents["eu-west"].Sync(ac)
			}
		}
	}()
	defer func() { close(stop); <-done }()

	for _, v := range []string{"1.0.0", "1.1.0"} {
		art := []byte("billing " + v)
		m := lc.Release.Sign("billing", v, art, time.Now())
		if _, err := cl.RegisterRelease(m, art); err != nil {
			t.Fatal(err)
		}
	}
	// A manifest signed by an unknown key is refused at the door.
	rogue, _ := bundle.NewSigner(nil)
	bad := []byte("rogue")
	if _, err := cl.RegisterRelease(rogue.Sign("billing", "6.6.6", bad, time.Now()), bad); err == nil {
		t.Fatal("control plane accepted a manifest from an untrusted key")
	}

	for i, r := range []string{"us-east", "eu-west", "adc-1"} {
		if _, err := cl.Submit(fmt.Sprintf("a-%d", i), r, "billing", "1.0.0"); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 5*time.Second, "1.0.0 in connected regions", func() bool {
		return confirmed(cl, "us-east", "1.0.0") && confirmed(cl, "eu-west", "1.0.0")
	})

	// Crash the leader and deploy through the new one.
	old := lc.Leader()
	lc.Kill(old)
	if _, err := lc.WaitLeader(old, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	for i, r := range []string{"us-east", "eu-west", "adc-1"} {
		if _, err := cl.Submit(fmt.Sprintf("b-%d", i), r, "billing", "1.1.0"); err != nil {
			t.Fatal(err)
		}
	}
	// Retried submission is a no-op.
	res, err := cl.Submit("b-0", "us-east", "billing", "1.1.0")
	if err != nil || !res.Duplicate {
		t.Fatalf("duplicate submit: %+v %v", res, err)
	}
	waitFor(t, 5*time.Second, "1.1.0 in connected regions", func() bool {
		return confirmed(cl, "us-east", "1.1.0") && confirmed(cl, "eu-west", "1.1.0")
	})

	// Air-gapped region: export, tamper, reject; export clean, import, carry acks back.
	pkg, err := cl.Export("adc-1")
	if err != nil || len(pkg.Jobs) != 2 {
		t.Fatalf("export: %d jobs, %v", len(pkg.Jobs), err)
	}
	tampered := pkg
	tampered.Artifacts = map[string][]byte{}
	for d, b := range pkg.Artifacts {
		c := append([]byte(nil), b...)
		c[0] ^= 1
		tampered.Artifacts[d] = c
	}
	if _, err := agents["adc-1"].Import(tampered); err == nil {
		t.Fatal("tampered package accepted")
	}
	acks, err := agents["adc-1"].Import(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agents["adc-1"].Import(pkg); err == nil {
		t.Fatal("replayed package accepted")
	}
	for _, a := range acks {
		if err := cl.Ack(a.JobID, a.Status, a.Detail); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 5*time.Second, "adc-1 at 1.1.0", func() bool { return confirmed(cl, "adc-1", "1.1.0") })
	if j, _ := lc.Servers[lc.Leader()].FSM().Job("a-2"); j.Status != control.Superseded {
		t.Fatalf("older adc-1 job should be superseded, got %q", j.Status)
	}

	// Restart the crashed replica from its WAL; it must catch up exactly.
	if err := lc.Restart(old); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "restarted replica to converge", func() bool {
		var d string
		for _, s := range lc.Servers {
			st := s.Status()
			if d == "" {
				d = st.Digest
			} else if st.Digest != d {
				return false
			}
		}
		return len(lc.Servers) == 3
	})
	for r, a := range agents {
		if st := a.Stats(); st.Executed > 2 {
			t.Fatalf("%s executed %d installs, want at most 2", r, st.Executed)
		}
	}
}

func TestFailoverMeasurementSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("networked test")
	}
	rep, err := MeasureFailover(3, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if rep.CommittedLost != 0 {
		t.Fatalf("lost %d committed jobs", rep.CommittedLost)
	}
	for _, tr := range rep.Trials {
		if tr.ElectionMS <= 0 || tr.ElectionMS > 2000 {
			t.Fatalf("implausible election time %v", tr.ElectionMS)
		}
	}
	t.Logf("%+v", rep.Trials)
}
