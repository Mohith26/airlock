#!/usr/bin/env bash
# End-to-end check with the real binary as separate OS processes:
# 3 control-plane replicas, 2 connected agents, 1 air-gapped region.
# It kills the leader process mid-rollout, deploys through the new leader,
# carries a package across the air gap, and checks every region converged.
set -euo pipefail

BIN=${BIN:-./bin/airlock}
WORK=$(mktemp -d)
P1=${P1:-17101}; P2=${P2:-17102}; P3=${P3:-17103}
PEERS="1=http://127.0.0.1:$P1,2=http://127.0.0.1:$P2,3=http://127.0.0.1:$P3"
CTL="http://127.0.0.1:$P1,http://127.0.0.1:$P2,http://127.0.0.1:$P3"
PIDS=()

cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

say() { printf '\n== %s\n' "$*"; }
confirmed() { # region version
  "$BIN" status -control "$CTL" | python3 -c "
import json,sys
s=json.load(sys.stdin)
r=[x for x in s['regions'] if x['name']=='$1']
sys.exit(0 if r and r[0]['confirmed_version']=='$2' and r[0]['confirmed_generation']==r[0]['desired_generation'] else 1)"
}
wait_until() { # seconds, command...
  local t=$1; shift
  for _ in $(seq 1 $((t*10))); do "$@" && return 0; sleep 0.1; done
  echo "FAILED waiting for: $*"; return 1
}
leader_id() {
  "$BIN" status -control "$CTL" | python3 -c "import json,sys; print(json.load(sys.stdin)['leader'])"
}

say "keys"
"$BIN" keygen -out "$WORK/keys" >/dev/null

say "start 3 replicas"
for i in 1 2 3; do
  port_var="P$i"
  "$BIN" node -id $i -peers "$PEERS" -listen "127.0.0.1:${!port_var}" -data "$WORK/data$i" -keys "$WORK/keys" >"$WORK/node$i.log" 2>&1 &
  PIDS[$i]=$!
done
has_leader() { [ "$(leader_id 2>/dev/null || echo 0)" != "0" ]; }
wait_until 10 has_leader
L=$(leader_id); echo "leader is replica $L"

say "start agents for us-east and eu-west"
for r in us-east eu-west; do
  "$BIN" agent -region $r -control "$CTL" -keys "$WORK/keys" -ledger "$WORK/$r.ledger" -interval 200ms >"$WORK/agent-$r.log" 2>&1 &
  PIDS+=($!)
done

say "sign and register two releases"
head -c 65536 /dev/urandom >"$WORK/v1.bin"
head -c 65536 /dev/urandom >"$WORK/v2.bin"
"$BIN" release -control "$CTL" -keys "$WORK/keys" -service api -version 1.0.0 -artifact "$WORK/v1.bin"
"$BIN" release -control "$CTL" -keys "$WORK/keys" -service api -version 2.0.0 -artifact "$WORK/v2.bin"

say "deploy 1.0.0 everywhere"
for r in us-east eu-west adc-1; do "$BIN" deploy -control "$CTL" -job "v1-$r" -region $r -service api -version 1.0.0 >/dev/null; done
wait_until 10 confirmed us-east 1.0.0
wait_until 10 confirmed eu-west 1.0.0
echo "connected regions at 1.0.0"

say "kill -9 the leader process (replica $L)"
kill -9 "${PIDS[$L]}"
sleep 0.05
wait_until 10 bash -c "[ \"\$($BIN status -control '$CTL' | python3 -c 'import json,sys; print(json.load(sys.stdin)[\"leader\"])')\" != '$L' ]"
echo "new leader is replica $(leader_id)"

say "deploy 2.0.0 through the new leader, then retry the same request"
for r in us-east eu-west adc-1; do "$BIN" deploy -control "$CTL" -job "v2-$r" -region $r -service api -version 2.0.0 >/dev/null; done
"$BIN" deploy -control "$CTL" -job v2-us-east -region us-east -service api -version 2.0.0 | grep -q '"duplicate": true' && echo "retry recognised as duplicate"
wait_until 10 confirmed us-east 2.0.0
wait_until 10 confirmed eu-west 2.0.0
echo "connected regions at 2.0.0"

say "air gap: export, tamper, import, ack"
"$BIN" export -control "$CTL" -region adc-1 -out "$WORK/pkg.json"
python3 - "$WORK/pkg.json" "$WORK/tampered.json" <<'EOF'
import json,sys,base64
p=json.load(open(sys.argv[1]))
for d,b in p['artifacts'].items():
    raw=bytearray(base64.b64decode(b)); raw[100]^=1; p['artifacts'][d]=base64.b64encode(bytes(raw)).decode(); break
json.dump(p,open(sys.argv[2],'w'))
EOF
if "$BIN" agent -region adc-1 -keys "$WORK/keys" -ledger "$WORK/adc.ledger" -import "$WORK/tampered.json" -receipt "$WORK/r0.json" 2>"$WORK/tamper.err"; then
  echo "FAILED: tampered package accepted"; exit 1
fi
echo "tampered package rejected: $(tail -1 "$WORK/tamper.err")"
"$BIN" agent -region adc-1 -keys "$WORK/keys" -ledger "$WORK/adc.ledger" -import "$WORK/pkg.json" -receipt "$WORK/r1.json" 2>/dev/null
"$BIN" ack -control "$CTL" -receipt "$WORK/r1.json"
wait_until 10 confirmed adc-1 2.0.0
echo "adc-1 at 2.0.0"

say "restart the killed replica from its WAL and check all three agree"
port_var="P$L"
"$BIN" node -id $L -peers "$PEERS" -listen "127.0.0.1:${!port_var}" -data "$WORK/data$L" -keys "$WORK/keys" >>"$WORK/node$L.log" 2>&1 &
PIDS[$L]=$!
digests() {
  for p in $P1 $P2 $P3; do curl -s "http://127.0.0.1:$p/v1/status" | python3 -c "import json,sys; print(json.load(sys.stdin)['state_digest'])" 2>/dev/null || echo down; done | sort -u | wc -l | tr -d ' '
}
wait_until 10 bash -c "[ \"\$(for p in $P1 $P2 $P3; do curl -s http://127.0.0.1:\$p/v1/status | python3 -c 'import json,sys; print(json.load(sys.stdin)[\"state_digest\"])' 2>/dev/null || echo down; done | sort -u | wc -l | tr -d ' ')\" = 1 ]"
echo "all 3 replicas report the same state digest"

say "metrics"
curl -s "http://127.0.0.1:$P1/metrics" | grep -E '^airlock_(jobs|raft_term|leader_elections_total|duplicates)' || true

say "PASS"
