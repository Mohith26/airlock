# Airlock

[![ci](https://github.com/Mohith26/airlock/actions/workflows/ci.yml/badge.svg)](https://github.com/Mohith26/airlock/actions/workflows/ci.yml)

A deployment control plane for cloud regions that are unreliable, partitioned, or fully air-gapped. Written in Go using only the standard library.

An operator signs a release and asks for it in some set of regions. Airlock records that intent in a replicated log, delivers it to every region by whatever path exists (a network pull, or a signed package carried across an air gap), and makes sure each region ends up on the right version exactly once. That holds even when:

- the control-plane leader crashes mid-rollout,
- a region drops off the network and comes back with a backlog,
- a worker dies after installing but before reporting back,
- a client retries a request it already sent,
- the bytes are corrupted on disk or tampered with in transit.

**[Interactive walkthrough on my site](https://mohithgajjela.com/system-04-airlock)**: a replay of `airlock demo`, recorded from this code.

## Results

All numbers below come from the files in `results/`, produced by the commands next to them. They are from an Apple Silicon laptop (darwin/arm64, Go 1.26.3).

### Leader failover on a real networked cluster: `airlock failover -trials 30`

Each trial starts three replicas as HTTP servers on loopback, each with an fsync'd write-ahead log. It commits 20 jobs, kills the leader's server, and times two things with a wall clock: how long until another replica wins an election, and how long until a client write succeeds again.

| Metric | p50 | p99 | max |
|---|---:|---:|---:|
| Leader killed until new leader elected | 195.8 ms | 496.2 ms | 496.2 ms |
| Leader killed until writes succeed again | 207.4 ms | 508.4 ms | 508.4 ms |
| Committed jobs lost across 30 crashes | 0 | | |

Election timeouts are randomized between 150 and 300 ms, so most failovers land inside that window. The slow tail is split votes, where two followers time out together and a second round is needed.

### 10,000 jobs under continuous faults: `airlock bench`

Five regions (four connected, one air-gapped), three replicas, and every fault type firing throughout the run, with 1% of Raft messages dropped at random. This runs the real Raft, state machine and agent code on a seeded, deterministic network and clock. Times here are in simulated milliseconds (one tick is 10 ms; one network hop is one tick).

| Fault injected | Count | Outcome |
|---|---:|---|
| Leader crashes | 7 | 7 failovers, election p50 240 ms, max 520 ms (simulated) |
| Region partitions (5 s each) | 8 | reconnect to converged p50 80 ms, max 380 ms |
| Worker crashes between install and ack | 109 | **0 double executions** |
| Cached artifacts corrupted on disk | 113 | every one later read (100) rejected and re-fetched clean |
| Offline packages tampered in transit | 13 | 13 packages rejected, **0 corrupt installs** |
| Duplicate client submissions | 985 | 985 suppressed by job id |
| Raft messages dropped | 74,072 of 5.6M | no divergence |

At the end, all 10,000 jobs had reached a terminal state: 6,675 succeeded, and 3,325 were superseded because a newer version for the same region arrived first, so the agent skipped the older one rather than install and then overwrite it. Every replica had a byte-identical state digest, and the 30,252-event audit chain verified.

### Guided scenario: `airlock demo`

Ten steps on three replicas and three regions: a signed rollout, a region partition, a leader crash, a deploy while degraded, a duplicate retry, a worker crash at the worst moment, a tampered air-gap package, reconnection with a backlog, and the dead replica rejoining. Eight checks must hold at the end, and they do: no pending jobs, no double execution, the tampered package rejected, the duplicate suppressed, the superseded job never installed, all regions converged, the audit chain verified, and the replicas identical. The run is deterministic: the same seed produces the same audit head hash.

### Separate processes: `scripts/e2e.sh`

The same scenario with the real binary as six OS processes: `kill -9` on the leader process, a deploy through the new leader, a retried request flagged as a duplicate, a package exported, tampered with and rejected, then re-imported clean and acknowledged, and finally the killed replica restarted from its WAL until all three report the same state digest.

## How it works

```
operator ── airlock release / deploy ──▶ ┌──────────── control plane ────────────┐
                                         │ cp-1 ◀──▶ cp-2 ◀──▶ cp-3   (Raft/HTTP) │
                                         │ replicated log → state machine         │
                                         │ releases · jobs · per-region generation │
                                         └───────┬─────────────────┬──────────────┘
                              pull over HTTP     │                 │  signed offline package
                                                 ▼                 ▼  (file carried across the gap)
                                     ┌── region agent ──┐   ┌── region agent (air-gapped) ──┐
                                     │ verify signature │   │ verify package + manifests    │
                                     │ verify SHA-256   │   │ reject replayed sequence      │
                                     │ ledger, then ack │   │ install, write receipt        │
                                     └──────────────────┘   └───────────────────────────────┘
```

| Piece | Where | What it guarantees |
|---|---|---|
| Raft consensus | `internal/raft` | One leader per term. Committed entries are never lost or reordered. A leader that cannot reach a majority steps down (check-quorum). |
| Write-ahead log | `internal/raft/storage.go` | Term, vote and entries are fsync'd before they are acted on; a restarted replica replays them, including truncations. |
| Replicated state machine | `internal/control` | Every command is idempotent. The job id is the idempotency key, a second ack never flips a status, and each region has a monotonically increasing generation. |
| Signed releases | `internal/bundle` | Ed25519 over a canonical manifest with the artifact's SHA-256 and size. The control plane refuses to store a release it cannot verify, and agents verify again themselves. |
| Offline packages | `internal/bundle`, `internal/agent` | Signed by a separate control-plane key, scoped to one region, with a strictly increasing sequence number so an old package cannot be replayed. |
| Region agent | `internal/agent` | Writes each outcome to a local ledger before acknowledging, so a crash in between never re-runs the job. Buffers acks while disconnected. On a backlog, it installs only the newest generation and marks the rest superseded. Never installs a generation older than what it runs. |
| Audit log | `internal/audit` | Hash-chained events; editing, deleting or reordering any one breaks verification. |
| Fault harness | `internal/cluster` | The same code driven by a seeded clock and network, so failures are reproducible. |
| Network service | `internal/server` | Raft over HTTP with per-peer queues, the public API, leader redirects (421) followed by the client, and Prometheus metrics. |

More detail: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md), [docs/THREAT_MODEL.md](docs/THREAT_MODEL.md).

## Running it

Requires Go 1.23 or newer. There are no third-party dependencies.

```sh
make test          # go vet + 31 tests, then the same with -race
make e2e           # the multi-process scenario above
make results       # regenerate results/demo.json, bench.json, failover.json
```

A cluster by hand:

```sh
go build -o bin/airlock ./cmd/airlock
bin/airlock keygen -out keys
PEERS=1=http://127.0.0.1:7001,2=http://127.0.0.1:7002,3=http://127.0.0.1:7003
for i in 1 2 3; do bin/airlock node -id $i -peers $PEERS -listen 127.0.0.1:700$i -data data$i -keys keys & done
bin/airlock agent -region us-east -control http://127.0.0.1:7001,http://127.0.0.1:7002 -keys keys &

bin/airlock release -keys keys -service api -version 1.0.0 -artifact ./api.tar
bin/airlock deploy  -job rollout-1 -region us-east -service api -version 1.0.0
bin/airlock status
curl -s 127.0.0.1:7001/metrics | grep airlock_

# air-gapped region
bin/airlock export -region adc-1 -out pkg.json                 # on the connected side
bin/airlock agent  -region adc-1 -keys keys -import pkg.json -receipt receipt.json   # inside the gap
bin/airlock ack    -receipt receipt.json                       # back on the connected side
```

With Docker: `docker compose up --build` starts three replicas and two agents.

## API

| Method and path | Purpose |
|---|---|
| `POST /v1/releases` | Register a signed manifest and its artifact. Rejected with 422 if the signature or digest does not verify. |
| `POST /v1/jobs` | Deploy a release to a region. Returns after the job commits. Re-sending the same `job_id` returns `"duplicate": true`. |
| `GET /v1/regions/{r}/pending` | Jobs a region still has to do (leader only). |
| `POST /v1/acks` | A region reports an outcome. |
| `GET /v1/offline/{r}` | A signed package of a region's pending work. |
| `GET /v1/status`, `/v1/audit`, `/metrics` | Cluster state, the verified audit chain, Prometheus metrics. |

Non-leaders answer writes with `421` and the leader's URL; the client follows it and retries through elections.

## Limits

- **No log compaction.** The log and state machine grow without bound. A real deployment needs snapshots and log truncation.
- **No membership changes.** The replica set is fixed at startup.
- **No TLS between replicas or to agents.** Artifact integrity does not depend on the transport, because agents verify signatures themselves, but confidentiality and replica authentication would need mTLS.
- **Artifacts ride inside the replicated log.** That is fine for small artifacts. Large ones belong in a content-addressed blob store, with only digests in the log.
- **Reads go to the leader without a read-index check.** A deposed leader can serve a slightly stale pending list for one election timeout. That is safe here, because every delivery is idempotent, but it is not linearizable.
- **The benchmark runs on a simulated network.** Its latencies are in simulated time and show behavior under faults, not network performance. The wall-clock numbers are the failover table, measured on loopback, so there is no real cross-region latency.
- **Key management is files on disk.** Production would keep the release key in an HSM or KMS.
