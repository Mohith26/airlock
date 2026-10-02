# Threat model

## What is trusted

- The **release signing key**. Whoever holds it decides what software can run. Agents are provisioned with its public half.
- The **control-plane key**, which signs offline packages. Agents are provisioned with its public half.
- Each agent's **local disk**, for its ledger. If an attacker can write to that, they own the host anyway.

## What is not trusted

- **The network.** This covers the link between agents and the control plane, and the one between replicas.
- **The courier.** Offline packages pass through people, removable media and intermediate systems.
- **The control plane itself, for integrity of artifacts.** A compromised replica can lie about what is pending, but it cannot make an agent install bytes the release key did not sign.

## Attacks and what stops them

| Attack | Defense | Tested by |
|---|---|---|
| Modify artifact bytes in transit or on disk | SHA-256 and size are inside the signed manifest; the agent checks before install | `TestTamperedArtifactRejected`, `TestCorruptDownloadNeverInstalled`, `TestCorruptCacheDiscardedAndRefetched`, bench (113 corruptions, 0 corrupt installs) |
| Modify the manifest (say, change the version) | Ed25519 signature over the canonical manifest | `TestTamperedManifestRejected` |
| Sign with your own key | Agents only trust provisioned key ids; claiming a trusted id without its private key fails verification | `TestUntrustedKeyRejected` |
| Register a rogue release through the API | The control plane verifies before storing and returns 422 | `TestEndToEndOverHTTP` |
| Alter a job inside an offline package (say, its generation) | The package signature covers every job and manifest | `TestOfflinePackageSignature` |
| Tamper with an artifact inside a package | Each artifact is checked against its signed manifest; the whole package is rejected | bench (13 tampered, 13 rejected), `scripts/e2e.sh` |
| Replay an old package to roll a region back | Strictly increasing per-region sequence | `TestOfflinePackageRules`, `TestEndToEndOverHTTP` |
| Deliver a package meant for another region | The region name is signed and checked | `TestOfflinePackageRules` |
| Roll back by sending an older generation | The agent rejects any generation not newer than what it runs | `agent.handleLatest` stale check |
| Erase or edit history | Hash-chained audit log | `TestChainVerifiesAndDetectsTampering` |

## Known gaps

- **No transport encryption or replica authentication.** Anyone on the network can send Raft messages. This needs mTLS between replicas, plus client certificates for agents.
- **No key rotation or revocation protocol.** A keyring can hold several keys, but there is no signed rotation statement.
- **Freshness is relative only.** Sequence numbers stop replay of *older* packages. Nothing stops an attacker from withholding *new* ones, which delays a region without breaking it. A signed expiry on packages would bound that delay.
- **The release key on disk is the crown jewel.** In production it would live in an HSM or cloud KMS, with signing behind an approval step.
