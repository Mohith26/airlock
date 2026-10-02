# Architecture

## The problem

A deployment system usually assumes the network is there. A control plane tells regions what to run, regions report back, and a failure is retried until it works. That breaks in the environments where reliability matters most: a region that loses connectivity for hours, a classified region that never has a network path at all, or a control plane that loses a node in the middle of a rollout.

Airlock separates *what should be running* from *how the instruction gets there*. The intent lives in a replicated log. Delivery is whatever path exists. And every step on the region side is safe to repeat, because in a system where any message can be lost, repeating steps is the normal case.

## Control plane

Three (or five) replicas run Raft. I wrote the Raft core as a pure state machine with no clock or network of its own (`internal/raft/raft.go`):

- `Tick()` advances logical time.
- `Step(msg)` handles an inbound RPC.
- `Propose(data)` appends to the log on the leader.
- `Ready()` drains outbound messages and newly committed entries.

Because of that split, the same code runs over HTTP in `internal/server`, where a 10 ms ticker drives `Tick` and per-peer goroutines deliver batches of messages, and over the seeded in-memory network in `internal/raft/memnet.go`, where a test controls exactly which messages are lost.

Details that matter for correctness:

- **Election restriction.** A node only votes for a candidate whose log is at least as up to date as its own, so a new leader always holds every committed entry.
- **Commit rule.** A leader only advances the commit index to an entry from its *own* term that a majority stores. It appends a no-op on election so earlier entries can commit.
- **Conflict repair.** Followers truncate a divergent suffix when the leader's entry at the same index has a different term. The leader backs off using the follower's hint.
- **Check-quorum.** A leader that has not heard from a majority within two election timeouts steps down. Without this, an isolated leader keeps accepting writes it can never commit.
- **Durability.** `FileStorage` appends JSON lines for hard state, entries and truncations, and fsyncs each write. On restart it replays them in order. A torn final line from a crash mid-write is tolerated; corruption anywhere earlier is an error.

`TestSafetyUnderRandomFaults` runs 60 seeds of 3,000 ticks on five nodes, with random crashes, restarts, isolations and 5% message loss. After every tick it checks that no two nodes lead in the same term. At the end it checks that every node's committed sequence is a prefix of every other's.

## State machine

Every replica applies the same committed entries to `control.FSM`. There are three commands.

- `release` stores a signed manifest, and optionally the artifact bytes after checking them against its digest.
- `deploy` creates a job. If the job id already exists, nothing happens. Otherwise the region's generation increments and becomes the job's generation.
- `ack` moves a pending job to `succeeded`, `rejected` or `superseded`. A job that is already terminal ignores later acks.

Because every command is idempotent, retries are safe at every layer:

- The client can retry a submit whose response it never got.
- The agent can resend an ack after a crash.
- The leader can re-deliver a job after a leadership change.

`FSM.Digest()` hashes the full state, which is how tests and the benchmark prove that replicas converged to the same result, not just the same length.

## Region agents

An agent is deliberately suspicious. It trusts exactly two things it was provisioned with: the release public key and the control-plane public key. One sync pass:

1. Flushes buffered acks. If that fails, the region is treated as disconnected, and nothing new is attempted until the acks get through.
2. Fetches the region's pending jobs, ordered by generation.
3. Marks every job except the newest as `superseded`. Installing an old version only to replace it a moment later is wasted work, and if it ever ran after the newer one, it would be a rollback.
4. For the newest job:
   - If the ledger already has an outcome, re-acknowledges that outcome. This is the case where a crash or partition lost the original ack.
   - If its generation is not newer than what is running, rejects it.
   - Otherwise, verifies the manifest signature, then the artifact's size and SHA-256. A damaged cached copy is discarded and re-fetched. Unverifiable bytes are never installed, and the job stays pending.
5. Installs the artifact, records the outcome in the ledger with an fsync, and only then acknowledges.

Step 5 is what makes a crash between install and acknowledge harmless. In the benchmark, 109 workers crash at exactly that point and nothing runs twice.

## The air gap

A region with no network path gets a `bundle.Package`: the region's pending jobs with their signed manifests and artifacts, plus a region name and a sequence number, all signed by the control-plane key. The agent rejects the whole package if:

- the signature fails,
- it names another region,
- its sequence is not newer than the last package imported, or
- any artifact fails its manifest.

It never applies a package halfway. The agent writes a receipt of acknowledgements, which is carried back and replayed with `airlock ack`.

## Observability

- `/metrics` exports Prometheus counters and gauges:
  - Raft term, commit index and leadership
  - jobs by status
  - suppressed duplicates
  - messages sent and dropped
  - a commit-latency histogram
- `/v1/audit` returns the hash-chained audit log and whether it verifies.
- The simulation harness records the same events into a timeline, which is what the website walkthrough replays.
