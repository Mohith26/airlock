package cluster

import "testing"

func TestGuidedDemoPassesEveryCheck(t *testing.T) {
	res, err := RunDemo(7)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range res.Checks {
		if !v {
			t.Errorf("check %s failed", k)
		}
	}
	if len(res.Steps) != 10 {
		t.Fatalf("want 10 steps, got %d", len(res.Steps))
	}
	for _, st := range res.Steps {
		t.Logf("%-10s %5d-%5dms frames=%d facts=%v", st.ID, st.StartMS, st.EndMS, len(st.Frames), st.Facts)
	}
	t.Logf("facts=%v audit=%d", res.Facts, res.AuditEvents)
}

func TestDemoIsDeterministic(t *testing.T) {
	a, err := RunDemo(7)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := RunDemo(7)
	if a.StateDigest != b.StateDigest || a.Final.AuditHead != b.Final.AuditHead {
		t.Fatal("same seed produced different runs")
	}
}
