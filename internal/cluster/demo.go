package cluster

import (
	"fmt"

	"github.com/Mohith26/airlock/internal/audit"
	"github.com/Mohith26/airlock/internal/control"
)

// Frame is the environment right after a tick in which something happened.
type Frame struct {
	Snapshot
	Events []Event `json:"events"`
}

// Step is one chapter of the guided demo.
type Step struct {
	ID        string            `json:"id"`
	Title     string            `json:"title"`
	Narration string            `json:"narration"`
	StartMS   int               `json:"start_ms"`
	EndMS     int               `json:"end_ms"`
	Frames    []Frame           `json:"frames"`
	Facts     map[string]string `json:"facts,omitempty"`
}

// DemoResult is everything the guided demo produced.
type DemoResult struct {
	Seed          int64             `json:"seed"`
	TickMS        int               `json:"tick_ms"`
	Steps         []Step            `json:"steps"`
	Final         Snapshot          `json:"final"`
	Facts         map[string]string `json:"facts"`
	AuditVerified bool              `json:"audit_verified"`
	AuditEvents   int               `json:"audit_events"`
	ReplicasAgree bool              `json:"replicas_agree"`
	StateDigest   string            `json:"state_digest"`
	Checks        map[string]bool   `json:"checks"`
}

func artifact(service, version string) []byte {
	b := make([]byte, 4096)
	seed := []byte(service + "@" + version)
	for i := range b {
		b[i] = seed[i%len(seed)] ^ byte(i*31)
	}
	return b
}

// RunDemo plays the guided scenario on the real code and records it.
func RunDemo(seed int64) (*DemoResult, error) {
	s := New(Options{
		Seed:         seed,
		Regions:      []RegionSpec{{Name: "us-east"}, {Name: "eu-west"}, {Name: "adc-1", AirGapped: true}},
		PackageEvery: 60,
		RestartAfter: 30,
		Timeline:     true,
	})
	res := &DemoResult{Seed: seed, TickMS: s.Opt.TickMS, Facts: map[string]string{}, Checks: map[string]bool{}}
	var cur *Step
	seen := 0
	s.OnTick = func() {
		if cur == nil || len(s.Events) == seen {
			return
		}
		cur.Frames = append(cur.Frames, Frame{Snapshot: s.Snapshot(), Events: append([]Event(nil), s.Events[seen:]...)})
		seen = len(s.Events)
	}
	begin := func(id, title, narration string) *Step {
		res.Steps = append(res.Steps, Step{ID: id, Title: title, Narration: narration, StartMS: s.ms(s.now()), Facts: map[string]string{}})
		cur = &res.Steps[len(res.Steps)-1]
		return cur
	}
	end := func() {
		// Capture events recorded by actions taken between ticks.
		if len(s.Events) > seen {
			cur.Frames = append(cur.Frames, Frame{Snapshot: s.Snapshot(), Events: append([]Event(nil), s.Events[seen:]...)})
			seen = len(s.Events)
		}
		cur.EndMS = s.ms(s.now())
		if len(cur.Frames) == 0 {
			cur.Frames = append(cur.Frames, Frame{Snapshot: s.Snapshot()})
		}
	}
	must := func(ok bool, what string) error {
		if !ok {
			return fmt.Errorf("demo: %s did not happen", what)
		}
		return nil
	}
	confirmed := func(region, version string) func() bool {
		return func() bool {
			for _, r := range s.LeaderFSM().Regions() {
				if r.Name == region {
					return r.ConfirmedVersion == version && r.ConfirmedGen == r.DesiredGen
				}
			}
			return false
		}
	}
	retry := func(f func() error) error {
		var err error
		ok := s.RunUntil(300, func() bool { err = f(); return err == nil })
		if !ok {
			return err
		}
		return nil
	}

	// 1. Cluster forms.
	begin("form", "Three control-plane replicas elect a leader",
		"Each replica starts as a follower with a randomized election timer. The first to time out asks for votes and wins a majority.")
	if err := must(s.RunUntil(400, func() bool { return s.Leader() != 0 }), "initial election"); err != nil {
		return nil, err
	}
	cur.Facts["leader"] = fmt.Sprintf("cp-%d", s.Leader())
	cur.Facts["elected_after_ms"] = fmt.Sprint(s.ms(s.now()))
	s.RunFor(10)
	end()

	// 2. Signed release goes out to every region.
	begin("deploy", "Sign payments-api 1.4.2 and roll it out",
		"The release is signed with an Ed25519 key. Connected regions pull it; the air-gapped region receives it as a signed offline package. Every region verifies the signature and SHA-256 digest itself before installing.")
	if err := retry(func() error {
		_, err := s.RegisterRelease("payments-api", "1.4.2", artifact("payments-api", "1.4.2"))
		return err
	}); err != nil {
		return nil, err
	}
	for i, r := range []string{"us-east", "eu-west", "adc-1"} {
		id := fmt.Sprintf("deploy-1.4.2-%d", i+1)
		if err := retry(func() error { return s.Submit(id, r, "payments-api", "1.4.2") }); err != nil {
			return nil, err
		}
	}
	ok := s.RunUntil(800, func() bool {
		return confirmed("us-east", "1.4.2")() && confirmed("eu-west", "1.4.2")() && confirmed("adc-1", "1.4.2")()
	})
	if err := must(ok, "initial rollout"); err != nil {
		return nil, err
	}
	end()

	// 3. Region partition.
	begin("partition", "eu-west loses its network path",
		"The region keeps running 1.4.2. Nothing it cannot verify will be installed, and anything sent to it waits in the replicated log.")
	s.Partition("eu-west")
	s.RunFor(20)
	end()

	// 4. Leader failure.
	begin("failover", "The leader crashes mid-rollout",
		"With one of three replicas gone, the remaining two still form a majority. One times out, wins the election, commits a no-op in its new term, and starts accepting writes.")
	killed := s.KillLeader()
	ok = s.RunUntil(400, func() bool { return len(s.Failovers()) > 0 })
	if err := must(ok, "failover"); err != nil {
		return nil, err
	}
	fo := s.Failovers()[0]
	cur.Facts["killed"] = fmt.Sprintf("cp-%d", killed)
	cur.Facts["new_leader"] = fmt.Sprintf("cp-%d", fo.NewLeader)
	cur.Facts["election_ms"] = fmt.Sprint(fo.ElectionMS)
	cur.Facts["write_unavailable_ms"] = fmt.Sprint(fo.WriteGapMS)
	res.Facts["failover_election_ms"] = fmt.Sprint(fo.ElectionMS)
	res.Facts["failover_write_gap_ms"] = fmt.Sprint(fo.WriteGapMS)
	s.RunFor(10)
	end()

	// 5. Deploy while degraded.
	begin("degraded", "Roll out 1.4.3 with a dead replica and a dark region",
		"us-east confirms right away through the new leader. eu-west cannot be reached, so its job stays pending in the log. adc-1 gets it on the next courier trip.")
	if err := retry(func() error {
		_, err := s.RegisterRelease("payments-api", "1.4.3", artifact("payments-api", "1.4.3"))
		return err
	}); err != nil {
		return nil, err
	}
	for i, r := range []string{"us-east", "eu-west", "adc-1"} {
		id := fmt.Sprintf("deploy-1.4.3-%d", i+1)
		if err := retry(func() error { return s.Submit(id, r, "payments-api", "1.4.3") }); err != nil {
			return nil, err
		}
	}
	ok = s.RunUntil(300, confirmed("us-east", "1.4.3"))
	if err := must(ok, "us-east 1.4.3"); err != nil {
		return nil, err
	}
	end()

	// 6. Duplicate submission.
	begin("duplicate", "A client retries the same request",
		"The operator's CLI times out and resends deploy-1.4.3-1. The job id is the idempotency key, so the replicated state machine records it once and ignores the copy.")
	before := s.LeaderFSM().DuplicateSubmits
	if err := retry(func() error { return s.Submit("deploy-1.4.3-1", "us-east", "payments-api", "1.4.3") }); err != nil {
		return nil, err
	}
	ok = s.RunUntil(100, func() bool { return s.LeaderFSM().DuplicateSubmits > before })
	if err := must(ok, "duplicate suppression"); err != nil {
		return nil, err
	}
	s.RunFor(10)
	end()

	// 7. Worker crash at the worst moment.
	begin("crash", "us-east's worker dies after installing, before reporting back",
		"This is the moment that causes double deploys. The agent wrote the result to its local ledger before acknowledging, so when it restarts and sees the job still pending it reports the recorded result instead of running it again.")
	s.CrashWorker("us-east")
	if err := retry(func() error {
		_, err := s.RegisterRelease("payments-api", "1.4.4", artifact("payments-api", "1.4.4"))
		return err
	}); err != nil {
		return nil, err
	}
	if err := retry(func() error { return s.Submit("deploy-1.4.4-1", "us-east", "payments-api", "1.4.4") }); err != nil {
		return nil, err
	}
	ok = s.RunUntil(400, confirmed("us-east", "1.4.4"))
	if err := must(ok, "us-east recovery"); err != nil {
		return nil, err
	}
	cur.Facts["executions_of_deploy_1.4.4_1"] = fmt.Sprint(s.execCount["deploy-1.4.4-1"])
	end()

	// 8. Tampered package crossing the air gap.
	begin("tamper", "A package is altered on its way into adc-1",
		"One byte of the artifact changes in transit. The signed manifest still says what the bytes must hash to, so adc-1 rejects the whole package and installs nothing. The next clean package goes through.")
	s.TamperNextPackage("adc-1")
	ok = s.RunUntil(600, func() bool { return s.EventCount("package.rejected") > 0 })
	if err := must(ok, "package rejection"); err != nil {
		return nil, err
	}
	ok = s.RunUntil(600, confirmed("adc-1", "1.4.3"))
	if err := must(ok, "adc-1 1.4.3"); err != nil {
		return nil, err
	}
	end()

	// 9. Reconnection and reconciliation.
	begin("reconcile", "eu-west comes back with a backlog",
		"While it was dark, 1.4.4 was also queued for eu-west. On reconnect the agent compares desired and actual state, skips straight to the newest generation, marks 1.4.3 as superseded, and never installs an older version over a newer one.")
	if err := retry(func() error { return s.Submit("deploy-1.4.4-2", "eu-west", "payments-api", "1.4.4") }); err != nil {
		return nil, err
	}
	if err := retry(func() error { return s.Submit("deploy-1.4.4-3", "adc-1", "payments-api", "1.4.4") }); err != nil {
		return nil, err
	}
	s.RunFor(20)
	reconnectAt := s.now()
	s.Reconnect("eu-west")
	ok = s.RunUntil(400, confirmed("eu-west", "1.4.4"))
	if err := must(ok, "eu-west reconcile"); err != nil {
		return nil, err
	}
	cur.Facts["reconcile_ms"] = fmt.Sprint(s.ms(s.now() - reconnectAt))
	res.Facts["reconcile_ms"] = cur.Facts["reconcile_ms"]
	if j, ok := s.LeaderFSM().Job("deploy-1.4.3-2"); ok {
		cur.Facts["deploy_1.4.3_2_status"] = j.Status
	}
	ok = s.RunUntil(800, confirmed("adc-1", "1.4.4"))
	if err := must(ok, "adc-1 1.4.4"); err != nil {
		return nil, err
	}
	end()

	// 10. Old leader rejoins.
	begin("rejoin", "The crashed replica restarts and catches up",
		"It rejoins as a follower, receives the entries it missed, and applies them. All three replicas end with byte-identical state.")
	s.Revive(killed)
	ok = s.RunUntil(600, func() bool { agree, _ := s.ReplicasAgree(); return agree && s.Converged() })
	if err := must(ok, "replica convergence"); err != nil {
		return nil, err
	}
	end()

	res.Final = s.Snapshot()
	res.AuditEvents = s.Audit.Len()
	res.AuditVerified = audit.Verify(s.Audit.Events()) == nil
	res.ReplicasAgree, res.StateDigest = s.ReplicasAgree()
	counts := s.LeaderFSM().Counts()
	res.Checks["no_pending_jobs"] = counts[control.Pending] == 0
	res.Checks["no_double_execution"] = s.DoubleExecutions() == 0
	res.Checks["tampered_package_rejected"] = s.EventCount("package.rejected") >= 1
	res.Checks["duplicate_submit_suppressed"] = s.LeaderFSM().DuplicateSubmits >= 1
	res.Checks["superseded_not_installed"] = counts[control.Superseded] >= 1
	res.Checks["all_regions_converged"] = s.Converged()
	res.Checks["audit_chain_verified"] = res.AuditVerified
	res.Checks["replicas_identical"] = res.ReplicasAgree
	for k, v := range res.Checks {
		if !v {
			return res, fmt.Errorf("demo check failed: %s", k)
		}
	}
	return res, nil
}
