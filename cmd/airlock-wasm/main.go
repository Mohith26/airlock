//go:build js && wasm

// Command airlock-wasm exposes the fault harness to a web page. It is the same
// Raft, state machine and agent code the networked binary runs, compiled to
// WebAssembly and driven by the page's animation loop instead of a ticker.
package main

import (
	"encoding/json"
	"fmt"
	"syscall/js"

	"github.com/Mohith26/airlock/internal/cluster"
)

var (
	sim      *cluster.Sim
	seen     int
	minor    int
	lastJobs = map[string]string{}
)

func artifact(v string) []byte {
	b := make([]byte, 2048)
	for i := range b {
		b[i] = byte(i*131) ^ v[i%len(v)]
	}
	return b
}

func encode(v any) any {
	b, _ := json.Marshal(v)
	return string(b)
}

func deploy() string {
	minor++
	v := fmt.Sprintf("2.%d.0", minor)
	if _, err := sim.RegisterRelease("payments-api", v, artifact(v)); err != nil {
		minor--
		return "no leader to accept the release; try again in a moment"
	}
	for _, r := range []string{"us-east", "eu-west", "adc-1"} {
		id := fmt.Sprintf("deploy-%s-%s", v, r)
		sim.Submit(id, r, "payments-api", v)
		lastJobs[r] = id
	}
	return "submitted payments-api " + v + " to all three regions"
}

func reset(seed int64) {
	sim = cluster.New(cluster.Options{
		Seed:         seed,
		Regions:      []cluster.RegionSpec{{Name: "us-east"}, {Name: "eu-west"}, {Name: "adc-1", AirGapped: true}},
		PackageEvery: 80,
		RestartAfter: 40,
		Timeline:     true,
	})
	seen, minor = 0, 0
	lastJobs = map[string]string{}
	sim.RunUntil(500, func() bool { return sim.Leader() != 0 })
	sim.RunFor(5)
	deploy()
}

func action(name string) string {
	switch name {
	case "kill_leader":
		if id := sim.KillLeader(); id != 0 {
			return fmt.Sprintf("crashed cp-%d", id)
		}
		return "there is no leader right now"
	case "revive":
		n := 0
		for id := 1; id <= sim.Opt.Nodes; id++ {
			if sim.Net.IsDown(id) {
				sim.Revive(id)
				n++
			}
		}
		if n == 0 {
			return "every replica is already running"
		}
		return fmt.Sprintf("restarted %d replica(s) from their logs", n)
	case "kill_two":
		killed := 0
		for id := 1; id <= sim.Opt.Nodes && killed < 2; id++ {
			if !sim.Net.IsDown(id) {
				sim.Net.Stop(id)
				killed++
			}
		}
		return "crashed two replicas: no majority, so no writes can commit"
	case "partition":
		if sim.Partitioned("eu-west") {
			sim.Reconnect("eu-west")
			return "eu-west reconnected"
		}
		sim.Partition("eu-west")
		return "eu-west cut off from the control plane"
	case "crash":
		sim.CrashWorker("us-east")
		return "us-east's worker will die after its next install, before it reports back"
	case "tamper":
		sim.TamperNextPackage("adc-1")
		return "the next package into adc-1 will have one byte changed in transit"
	case "corrupt":
		if sim.CorruptCache("us-east") {
			return "flipped a byte in an artifact cached on us-east's disk"
		}
		return "us-east has nothing cached yet"
	case "deploy":
		return deploy()
	case "duplicate":
		id, ok := lastJobs["us-east"]
		if !ok {
			return "nothing to retry yet"
		}
		if j, ok := sim.LeaderFSM().Job(id); ok {
			if err := sim.Submit(id, "us-east", j.Service, j.Version); err != nil {
				return "no leader to send the retry to"
			}
			return "resent " + id
		}
		return "nothing to retry yet"
	}
	return "unknown action"
}

func main() {
	reset(7)
	js.Global().Set("airlockReset", js.FuncOf(func(this js.Value, a []js.Value) any {
		seed := int64(7)
		if len(a) > 0 {
			seed = int64(a[0].Int())
		}
		reset(seed)
		return nil
	}))
	js.Global().Set("airlockStep", js.FuncOf(func(this js.Value, a []js.Value) any {
		n := 1
		if len(a) > 0 {
			n = a[0].Int()
		}
		sim.RunFor(n)
		ev := sim.Events[seen:]
		seen = len(sim.Events)
		agree, _ := sim.ReplicasAgree()
		return encode(map[string]any{
			"snap": sim.Snapshot(), "events": ev, "agree": agree,
			"converged": sim.Converged(), "double": sim.DoubleExecutions(), "failovers": sim.Failovers(),
		})
	}))
	js.Global().Set("airlockAction", js.FuncOf(func(this js.Value, a []js.Value) any {
		return action(a[0].String())
	}))
	js.Global().Set("airlockReady", true)
	select {}
}
