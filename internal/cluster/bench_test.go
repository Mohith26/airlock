package cluster

import (
	"encoding/json"
	"testing"
)

func TestBenchUnderFaults(t *testing.T) {
	cfg := DefaultBench(11)
	if testing.Short() {
		cfg.Jobs = 1500
	}
	res, err := RunBench(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.MarshalIndent(res, "", "  ")
	t.Log(string(b))
	if res.Jobs.Double != 0 || res.Jobs.Pending != 0 || res.Outcomes.CorruptInstalls != 0 || !res.ReplicasAgree || !res.AuditVerified {
		t.Fatalf("invariant violated")
	}
	if res.Jobs.Unique != cfg.Jobs {
		t.Fatalf("unique jobs %d, want %d", res.Jobs.Unique, cfg.Jobs)
	}
}
