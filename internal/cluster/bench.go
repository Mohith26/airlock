package cluster

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"time"

	"github.com/Mohith26/airlock/internal/audit"
	"github.com/Mohith26/airlock/internal/control"
)

// BenchConfig controls the fault-injection benchmark.
type BenchConfig struct {
	Seed           int64   `json:"seed"`
	Jobs           int     `json:"jobs"`
	Releases       int     `json:"releases"`
	DuplicateRate  float64 `json:"duplicate_submit_rate"`
	CrashRate      float64 `json:"worker_crash_rate"`
	CorruptRate    float64 `json:"cache_corruption_rate"`
	TamperRate     float64 `json:"package_tamper_rate"`
	KillEvery      int     `json:"leader_kill_every_ticks"`
	DownFor        int     `json:"node_down_ticks"`
	PartitionEvery int     `json:"partition_every_ticks"`
	PartitionFor   int     `json:"partition_ticks"`
	DropRate       float64 `json:"raft_message_drop_rate"`
}

// DefaultBench is the configuration whose results are published in README.
func DefaultBench(seed int64) BenchConfig {
	return BenchConfig{
		Seed: seed, Jobs: 10000, Releases: 40,
		DuplicateRate: 0.10, CrashRate: 0.01, CorruptRate: 0.01, TamperRate: 0.10,
		KillEvery: 1500, DownFor: 400, PartitionEvery: 1200, PartitionFor: 500,
		DropRate: 0.01,
	}
}

// Dist summarizes a latency distribution in milliseconds.
type Dist struct {
	N   int     `json:"n"`
	P50 float64 `json:"p50_ms"`
	P90 float64 `json:"p90_ms"`
	P99 float64 `json:"p99_ms"`
	Max float64 `json:"max_ms"`
}

func dist(ticks []int, tickMS int) Dist {
	if len(ticks) == 0 {
		return Dist{}
	}
	v := append([]int(nil), ticks...)
	sort.Ints(v)
	q := func(p float64) float64 {
		i := int(math.Ceil(p*float64(len(v)))) - 1
		if i < 0 {
			i = 0
		}
		return float64(v[i] * tickMS)
	}
	return Dist{N: len(v), P50: q(0.50), P90: q(0.90), P99: q(0.99), Max: float64(v[len(v)-1] * tickMS)}
}

// BenchResult is the published benchmark output.
type BenchResult struct {
	Config      BenchConfig `json:"config"`
	Environment struct {
		GoVersion string `json:"go_version"`
		GOOS      string `json:"goos"`
		GOARCH    string `json:"goarch"`
		CPUs      int    `json:"cpus"`
	} `json:"environment"`
	Topology struct {
		ControlPlaneReplicas int      `json:"control_plane_replicas"`
		ConnectedRegions     []string `json:"connected_regions"`
		AirGappedRegions     []string `json:"air_gapped_regions"`
		TickMS               int      `json:"tick_ms"`
	} `json:"topology"`

	SimulatedSeconds float64 `json:"simulated_seconds"`
	WallSeconds      float64 `json:"wall_seconds"`
	JobsPerWallSec   float64 `json:"jobs_per_wall_second"`

	Jobs struct {
		Unique     int            `json:"unique"`
		ByStatus   map[string]int `json:"by_status"`
		Pending    int            `json:"still_pending"`
		Executions int            `json:"installs_performed"`
		Double     int            `json:"double_executions"`
	} `json:"jobs"`

	Faults struct {
		LeaderKills         int    `json:"leader_kills"`
		Partitions          int    `json:"region_partitions"`
		WorkerCrashes       int    `json:"worker_crashes"`
		CacheCorruptions    int    `json:"cache_corruptions"`
		PackagesTampered    int    `json:"packages_tampered"`
		DuplicateSubmits    int    `json:"duplicate_submits_sent"`
		RaftMessagesSent    uint64 `json:"raft_messages_sent"`
		RaftMessagesDropped uint64 `json:"raft_messages_dropped"`
	} `json:"faults_injected"`

	Outcomes struct {
		DuplicateSubmitsSuppressed    uint64 `json:"duplicate_submits_suppressed"`
		DuplicateDeliveriesSuppressed uint64 `json:"duplicate_deliveries_suppressed"`
		DuplicateAcksIgnored          uint64 `json:"duplicate_acks_ignored"`
		CorruptArtifactsRejected      uint64 `json:"corrupt_cached_artifacts_rejected"`
		CleanRefetches                uint64 `json:"clean_refetches"`
		TamperedPackagesRejected      uint64 `json:"tampered_packages_rejected"`
		StaleGenerationsRejected      uint64 `json:"stale_generations_rejected"`
		CorruptInstalls               int    `json:"corrupt_artifacts_installed"`
	} `json:"outcomes"`

	Failover struct {
		Count    int  `json:"count"`
		Election Dist `json:"election"`
		WriteGap Dist `json:"write_unavailable"`
	} `json:"failover"`

	CommitLatency    Dist `json:"commit_latency"`
	ConfirmLatency   Dist `json:"submit_to_confirmed"`
	PartitionRecover Dist `json:"partition_reconnect_to_converged"`

	ReplicasAgree bool   `json:"replicas_identical"`
	StateDigest   string `json:"state_digest"`
	AuditEvents   int    `json:"audit_events"`
	AuditVerified bool   `json:"audit_chain_verified"`
	Converged     bool   `json:"all_regions_converged"`
}

// RunBench runs the fault-injection benchmark.
func RunBench(cfg BenchConfig) (*BenchResult, error) {
	regions := []RegionSpec{{Name: "us-east"}, {Name: "us-west"}, {Name: "eu-west"}, {Name: "ap-south"}, {Name: "adc-1", AirGapped: true}}
	s := New(Options{Seed: cfg.Seed, Regions: regions, DropRate: cfg.DropRate, PackageEvery: 100})
	rng := rand.New(rand.NewSource(cfg.Seed * 104729))
	res := &BenchResult{Config: cfg}
	res.Environment.GoVersion = runtime.Version()
	res.Environment.GOOS, res.Environment.GOARCH, res.Environment.CPUs = runtime.GOOS, runtime.GOARCH, runtime.NumCPU()
	res.Topology.ControlPlaneReplicas = s.Opt.Nodes
	res.Topology.TickMS = s.Opt.TickMS
	for _, r := range regions {
		if r.AirGapped {
			res.Topology.AirGappedRegions = append(res.Topology.AirGappedRegions, r.Name)
		} else {
			res.Topology.ConnectedRegions = append(res.Topology.ConnectedRegions, r.Name)
		}
	}

	start := time.Now()
	if !s.RunUntil(1000, func() bool { return s.Leader() != 0 }) {
		return nil, fmt.Errorf("bench: no initial leader")
	}
	versions := make([]string, cfg.Releases)
	for i := range versions {
		versions[i] = fmt.Sprintf("2.%d.0", i)
		v := versions[i]
		if !s.RunUntil(500, func() bool {
			_, err := s.RegisterRelease("ledger-svc", v, artifact("ledger-svc", v))
			return err == nil
		}) {
			return nil, fmt.Errorf("bench: could not register releases")
		}
	}
	s.RunFor(20)

	type sub struct {
		id, region, version string
		at                  int
	}
	var queue []sub // submissions waiting for a leader (client retry)
	var dupes []sub
	killedAt := map[int]int{}
	partitionedAt := map[string]int{}
	type watch struct {
		region string
		gen    uint64
		at     int
	}
	var watches []watch
	var recover []int

	submitted := 0
	connected := res.Topology.ConnectedRegions
	for submitted < cfg.Jobs || len(queue) > 0 || len(dupes) > 0 {
		t := s.now()
		// Scheduled faults.
		if cfg.KillEvery > 0 && t%cfg.KillEvery == cfg.KillEvery/2 {
			if id := s.KillLeader(); id != 0 {
				killedAt[id] = t
				res.Faults.LeaderKills++
			}
		}
		for id, at := range killedAt {
			if t-at >= cfg.DownFor {
				s.Revive(id)
				delete(killedAt, id)
			}
		}
		if cfg.PartitionEvery > 0 && t%cfg.PartitionEvery == 0 {
			r := connected[rng.Intn(len(connected))]
			if !s.Partitioned(r) {
				s.Partition(r)
				partitionedAt[r] = t
				res.Faults.Partitions++
			}
		}
		for r, at := range partitionedAt {
			if t-at >= cfg.PartitionFor {
				s.Reconnect(r)
				delete(partitionedAt, r)
				f := s.LeaderFSM()
				for _, reg := range f.Regions() {
					if reg.Name == r {
						watches = append(watches, watch{region: r, gen: reg.DesiredGen, at: t})
					}
				}
			}
		}
		if rng.Float64() < cfg.CrashRate {
			r := connected[rng.Intn(len(connected))]
			if s.Agents[r].Alive() {
				s.CrashWorker(r)
				res.Faults.WorkerCrashes++
			}
		}
		if rng.Float64() < cfg.CorruptRate {
			r := connected[rng.Intn(len(connected))]
			if s.CorruptCache(r) {
				res.Faults.CacheCorruptions++
			}
		}
		if t%s.Opt.PackageEvery == s.Opt.PackageEvery-1 && rng.Float64() < cfg.TamperRate {
			s.TamperNextPackage("adc-1")
			res.Faults.PackagesTampered++
		}

		// One new job per tick, round-robin across regions.
		if submitted < cfg.Jobs {
			r := regions[submitted%len(regions)].Name
			j := sub{id: fmt.Sprintf("job-%05d", submitted), region: r, version: versions[rng.Intn(len(versions))], at: t}
			queue = append(queue, j)
			if rng.Float64() < cfg.DuplicateRate {
				d := j
				d.at = t + 20 + rng.Intn(200)
				dupes = append(dupes, d)
			}
			submitted++
		}
		kept := queue[:0]
		for _, j := range queue {
			if err := s.Submit(j.id, j.region, "ledger-svc", j.version); err != nil {
				kept = append(kept, j)
			}
		}
		queue = kept
		keptD := dupes[:0]
		for _, d := range dupes {
			if d.at <= t {
				if s.Submit(d.id, d.region, "ledger-svc", d.version) == nil {
					res.Faults.DuplicateSubmits++
					continue
				}
			}
			keptD = append(keptD, d)
		}
		dupes = keptD

		s.Tick()
		keptW := watches[:0]
		for _, w := range watches {
			done := false
			for _, reg := range s.LeaderFSM().Regions() {
				if reg.Name == w.region && reg.ConfirmedGen >= w.gen {
					done = true
				}
			}
			if done && s.Leader() != 0 {
				recover = append(recover, s.now()-w.at)
			} else {
				keptW = append(keptW, w)
			}
		}
		watches = keptW
	}

	// Heal everything and drain.
	for id := range killedAt {
		s.Revive(id)
	}
	for r := range partitionedAt {
		s.Reconnect(r)
	}
	s.Net.DropRate = 0
	converged := s.RunUntil(20000, func() bool { agree, _ := s.ReplicasAgree(); return agree && s.Converged() })
	wall := time.Since(start).Seconds()

	f := s.LeaderFSM()
	counts := f.Counts()
	res.Jobs.ByStatus = counts
	res.Jobs.Pending = counts[control.Pending]
	res.Jobs.Unique = len(f.Jobs())
	res.Outcomes.CorruptInstalls = s.CorruptInstalls()
	res.Jobs.Double = s.DoubleExecutions()
	for _, c := range s.execCount {
		res.Jobs.Executions += c
	}
	for _, a := range s.Agents {
		st := a.Stats()
		res.Outcomes.DuplicateDeliveriesSuppressed += st.DuplicateDeliveries
		res.Outcomes.CleanRefetches += st.Refetched
		res.Outcomes.TamperedPackagesRejected += st.PackagesRejected
		res.Outcomes.StaleGenerationsRejected += st.StaleRejected
	}
	res.Outcomes.CorruptArtifactsRejected = uint64(s.EventCount("artifact.rejected"))
	res.Outcomes.DuplicateSubmitsSuppressed = f.DuplicateSubmits
	res.Outcomes.DuplicateAcksIgnored = f.DuplicateAcks
	res.Faults.RaftMessagesSent, res.Faults.RaftMessagesDropped = s.Net.Sent, s.Net.Dropped

	fos := s.Failovers()
	res.Failover.Count = len(fos)
	var el, gap []int
	for _, fo := range fos {
		el = append(el, (fo.ElectedAt - fo.KilledAt))
		gap = append(gap, (fo.FirstCommit - fo.KilledAt))
	}
	res.Failover.Election = dist(el, s.Opt.TickMS)
	res.Failover.WriteGap = dist(gap, s.Opt.TickMS)
	res.CommitLatency = dist(s.CommitLatency, s.Opt.TickMS)
	res.ConfirmLatency = dist(s.ConfirmLatency, s.Opt.TickMS)
	res.PartitionRecover = dist(recover, s.Opt.TickMS)

	res.SimulatedSeconds = float64(s.ms(s.now())) / 1000
	res.WallSeconds = math.Round(wall*1000) / 1000
	res.JobsPerWallSec = math.Round(float64(cfg.Jobs) / wall)
	res.ReplicasAgree, res.StateDigest = s.ReplicasAgree()
	res.AuditEvents = s.Audit.Len()
	res.AuditVerified = audit.Verify(s.Audit.Events()) == nil
	res.Converged = converged && s.Converged()
	if !res.Converged {
		return res, fmt.Errorf("bench: cluster did not converge (pending=%d)", res.Jobs.Pending)
	}
	return res, nil
}
