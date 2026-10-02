package audit

import "testing"

func TestChainVerifiesAndDetectsTampering(t *testing.T) {
	var l Log
	l.Append(1, "release.registered", "operator", map[string]string{"version": "1.4.2"})
	l.Append(2, "job.succeeded", "region/us-east", map[string]string{"job": "j1"})
	l.Append(3, "artifact.rejected", "region/adc-1", map[string]string{"reason": "digest mismatch"})
	ev := l.Events()
	if err := Verify(ev); err != nil {
		t.Fatal(err)
	}

	edited := append([]Event(nil), ev...)
	edited[1].Details = map[string]string{"job": "j2"}
	if Verify(edited) == nil {
		t.Fatal("edited event accepted")
	}

	dropped := []Event{ev[0], ev[2]}
	if Verify(dropped) == nil {
		t.Fatal("deleted event accepted")
	}

	swapped := []Event{ev[0], ev[2], ev[1]}
	if Verify(swapped) == nil {
		t.Fatal("reordered events accepted")
	}
}
