#!/usr/bin/env bash
#
# regtest-e2e.sh — the v0.2 gate: weir against a REAL bitcoind + redis on regtest, driving
# every lifecycle path and asserting the EXACT events examples/catch.js receives.
#
# Scenarios (each asserts events from the catcher's `[catch] <summary>` log lines):
#   1. happy path         seen → confirmed:1 → confirmed:3
#   2. RBF fee-bump       A seen → B seen; A dropped reason=replaced replacedBy=B IMMEDIATELY (no block)
#   3. redirect           A seen → bump pays nobody we watch → A dropped(replaced), no seen for B
#   4. reorg / demote     mine tx in b; invalidateblock b; mine an EMPTY block at the same height
#                         → demoted; next block re-mines it → confirmed:1 under a NEW block hash
#   5. reorg / conflict   mine A in b; invalidateblock b (A returns to the mempool); a higher-fee
#                         spend B of the same input (paying nobody we watch) replaces A (full-RBF);
#                         mine B → conflicted(A) PROVEN: reason=double-spend conflictingTxid=B
#   6. TTL expiry         watch with ZADD expiries (past) → expired on the next block, watch removed
#   7. webhook down       stop catcher, pay + mine, outbox holds; restart catcher → drains to empty
#   8. restart mid-flight seen → stop weir → mine 3 → start weir → confirmed:1 + confirmed:3 from
#                         boot reconciliation (gap walk); no duplicate seen
#   9. /ready             503 {reconciled:false} during boot catch-up, then 200 with chainLag 0
# Global: no [error] lines from weir after boot, every idempotencyKey delivered exactly once.
#
# Requires: docker compose v2 (stack from the repo root, profile 'regtest'), node ≥ 20 on the
# host (runs examples/catch.js), .env with WEBHOOK_URL=http://host.docker.internal:9090/webhook,
# WEBHOOK_SECRET and ADMIN_TOKEN set. The stack is recreated from scratch (down -v) at start
# and torn down at the end (KEEP_STACK=1 leaves it up). Runtime: ~2 minutes.
set -euo pipefail
cd "$(dirname "$0")/.."

CATCH_LOG="${CATCH_LOG:-/tmp/weir-e2e-catch.log}"
CATCH_PORT="${CATCH_PORT:-9090}"
SECRET="$(grep '^WEBHOOK_SECRET=' .env | cut -d= -f2-)"
TOKEN="$(grep '^ADMIN_TOKEN=' .env | cut -d= -f2-)"
PORT="$(grep '^ADMIN_PORT=' .env | cut -d= -f2-)"; PORT="${PORT:-8787}"
[ -n "$SECRET" ] || { echo "error: WEBHOOK_SECRET missing in .env" >&2; exit 1; }
[ -n "$TOKEN" ] || { echo "error: ADMIN_TOKEN missing in .env (scenario 9 needs the admin server)" >&2; exit 1; }

bcli() { docker compose exec -T bitcoind bitcoin-cli -conf=/etc/bitcoin/bitcoin.conf -rpcwallet=e2e "$@"; }
rcli() { docker compose exec -T redis redis-cli "$@"; }
jget() { node -e 'const j=JSON.parse(require("fs").readFileSync(0,"utf8"));console.log(eval(process.argv[1]))' "$1"; }
calc() { node -p "($1).toFixed(8)"; }
step() { printf '\n== %s\n' "$*"; }
ok()   { printf '   ✓ %s\n' "$*"; }
fail() { printf '   ✗ %s\n' "$*" >&2; printf '   --- last 40 weir log lines ---\n' >&2; docker compose logs --no-log-prefix --tail 40 weir >&2; exit 1; }

# wait until the catcher log has a summary line matching $1 (ERE), up to $2 seconds
expect_event() { local re="$1" secs="${2:-30}"; for _ in $(seq 1 "$secs"); do grep -Eq "\[catch\] $re" "$CATCH_LOG" && return 0; sleep 1; done; fail "expected event /$re/ within ${secs}s"; }
# assert the catcher log does NOT have a summary line matching $1 after waiting $2 seconds
expect_no_event() { local re="$1" secs="${2:-5}"; sleep "$secs"; grep -Eq "\[catch\] $re" "$CATCH_LOG" && fail "unexpected event /$re/"; return 0; }
count_events() { grep -Ec "\[catch\] $1" "$CATCH_LOG" || true; }

start_catcher() { (WEBHOOK_SECRET="$SECRET" PORT="$CATCH_PORT" nohup node examples/catch.js >> "$CATCH_LOG" 2>&1 &); sleep 1; }
stop_catcher()  { pkill -f 'node examples/catch.js' 2>/dev/null || true; sleep 1; }
# busybox wget -S prints the status line first ("  HTTP/1.1 503 ..."), then on non-2xx a "wget: server returned error: HTTP/1.1 503 ..." line
ready_status()  { docker compose exec -T weir wget -qO- --server-response "http://127.0.0.1:${PORT}/ready" 2>&1 | grep -o 'HTTP/[0-9.]* [0-9][0-9][0-9]' | head -1 | awk '{print $2}'; }
wait_ready()    { local s=""; for _ in $(seq 1 "${1:-90}"); do s="$(ready_status || true)"; [ "$s" = "200" ] && return 0; sleep 1; done; fail "/ready never returned 200 (last: '${s:-refused}')"; }
weir_errors()   { docker compose logs --no-log-prefix weir 2>&1 | grep -c '\[error\]' || true; }
watch_addr()    { rcli SADD weir:regtest:addresses "$1" >/dev/null; }
mine()          { bcli generatetoaddress "${1:-1}" "$MINER" >/dev/null; }
mine_empty()    { bcli generateblock "$MINER" '[]' >/dev/null; }
# a signed raw tx spending exactly one wallet UTXO ($1 txid, $2 vout) to the outputs JSON in $3
make_tx()       { local raw; raw="$(bcli createrawtransaction "[{\"txid\":\"$1\",\"vout\":$2}]" "$3")"; bcli signrawtransactionwithwallet "$raw" | jget 'j.complete ? j.hex : (()=>{throw new Error("signing incomplete")})()'; }

cleanup() {
  stop_catcher
  if [ "${KEEP_STACK:-0}" != "1" ]; then docker compose --profile regtest down -v >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

: > "$CATCH_LOG"
step "stack up (fresh)"
stop_catcher
docker compose --profile regtest down -v >/dev/null 2>&1 || true
docker compose --profile regtest up -d --build >/dev/null 2>&1
for _ in $(seq 1 60); do docker compose exec -T bitcoind bitcoin-cli -conf=/etc/bitcoin/bitcoin.conf getblockchaininfo >/dev/null 2>&1 && break; sleep 1; done
docker compose exec -T bitcoind bitcoin-cli -conf=/etc/bitcoin/bitcoin.conf createwallet e2e >/dev/null
start_catcher
MINER="$(bcli getnewaddress miner)"; bcli generatetoaddress 101 "$MINER" >/dev/null
wait_ready
ERR_BASE="$(weir_errors)"   # preflight failures while bitcoind was still booting are expected
ok "bitcoind + redis + weir up, wallet funded (height $(bcli getblockcount)), catcher on :$CATCH_PORT, weir /ready 200"

step "1. happy path"
A1="$(bcli getnewaddress)"; watch_addr "$A1"; T1="$(bcli sendtoaddress "$A1" 0.5)"
expect_event "seen txid=$T1 confs=0 sats=50000000 addr=$A1"
mine 1; expect_event "confirmed txid=$T1 confs=1 "
mine 2; expect_event "confirmed txid=$T1 confs=3 "
expect_no_event "confirmed txid=$T1 confs=2 " 0
ok "seen → confirmed:1 → confirmed:3"

step "2. RBF fee bump: replaced immediately, no block"
A2="$(bcli getnewaddress)"; watch_addr "$A2"
TA="$(bcli -named sendtoaddress address="$A2" amount=0.3 replaceable=true)"; expect_event "seen txid=$TA "
TB="$(bcli bumpfee "$TA" | jget j.txid)"
expect_event "seen txid=$TB "
expect_event "dropped txid=$TA confs=0 .* reason=replaced replacedBy=$TB" 10
[ "$(bcli getblockcount)" = "104" ] || fail "a block was mined during scenario 2"
ok "A dropped(replaced, replacedBy=B) and B seen, before any block"
mine 3; expect_event "confirmed txid=$TB confs=3 "

step "3. redirect (replacement pays nobody we watch)"
A3="$(bcli getnewaddress)"; watch_addr "$A3"
TC="$(bcli -named sendtoaddress address="$A3" amount=0.2 replaceable=true)"; expect_event "seen txid=$TC "
OTHER="$(bcli getnewaddress)"
TD="$(bcli -named bumpfee txid="$TC" options="{\"outputs\":[{\"$OTHER\":0.19}]}" | jget j.txid)"
expect_event "dropped txid=$TC confs=0 .* reason=replaced replacedBy=$TD" 10
expect_no_event "seen txid=$TD "
ok "A dropped(replaced, replacedBy=B); B never seen"
mine 3

step "4. reorg → demoted → re-confirmed under a new block hash"
A4="$(bcli getnewaddress)"; watch_addr "$A4"; T4="$(bcli sendtoaddress "$A4" 0.4)"; expect_event "seen txid=$T4 "
mine 1; H4="$(bcli getbestblockhash)"; expect_event "confirmed txid=$T4 confs=1 .* block=$H4"
bcli invalidateblock "$H4" >/dev/null
mine_empty   # replacement block at the same height WITHOUT the tx → it is back in the mempool
expect_event "demoted txid=$T4 confs=0 .* block=$H4" 30
H4B="$(bcli getbestblockhash)"; [ "$H4B" != "$H4" ] || fail "replacement block has the old hash"
expect_no_event "confirmed txid=$T4 confs=1 .* block=$H4B" 2
mine 1; H4C="$(bcli getbestblockhash)"
expect_event "confirmed txid=$T4 confs=1 .* block=$H4C" 30
[ "$(count_events "confirmed txid=$T4 confs=1 ")" = "2" ] || fail "expected exactly two confirmed:1 for T4 (old + new block)"
[ "$(count_events "demoted txid=$T4 ")" = "1" ] || fail "expected exactly one demoted for T4"
mine 2; expect_event "confirmed txid=$T4 confs=3 .* block=$H4C"
ok "confirmed:1@$H4 → demoted → confirmed:1@$H4C (new idempotency key) → confirmed:3"

step "5. reorg + double-spend → PROVEN conflicted"
A5="$(bcli getnewaddress)"; watch_addr "$A5"
UT="$(bcli listunspent | jget 'j.filter(u=>u.amount>=1&&u.confirmations>=101).map(u=>u.txid+" "+u.vout+" "+u.amount)[0]')"
UTXID="${UT%% *}"; REST="${UT#* }"; UVOUT="${REST%% *}"; UAMT="${REST#* }"
S5="$(make_tx "$UTXID" "$UVOUT" "{\"$A5\":0.5,\"$MINER\":$(calc "$UAMT-0.5-0.0002")}")"
T5="$(bcli sendrawtransaction "$S5")"; expect_event "seen txid=$T5 confs=0 sats=50000000 addr=$A5"
mine 1; H5="$(bcli getbestblockhash)"; expect_event "confirmed txid=$T5 confs=1 .* block=$H5"
bcli invalidateblock "$H5" >/dev/null
[ "$(bcli getmempoolentry "$T5" >/dev/null 2>&1 && echo yes)" = "yes" ] || fail "T5 did not return to the mempool after invalidateblock"
# competing spend of the SAME input, higher fee, paying nobody we watch: full-RBF replaces T5
S6="$(make_tx "$UTXID" "$UVOUT" "{\"$MINER\":$(calc "$UAMT-0.0004")}")"
T6="$(bcli sendrawtransaction "$S6")"
[ "$(bcli getmempoolentry "$T5" >/dev/null 2>&1 || echo gone)" = "gone" ] || fail "T6 did not replace T5 in the mempool"
mine 1; H6="$(bcli getbestblockhash)"
expect_event "conflicted txid=$T5 confs=1 .* reason=double-spend conflictingTxid=$T6 block=$H5" 30
expect_no_event "demoted txid=$T5 " 2
expect_no_event "dropped txid=$T5 " 0
[ "$(rcli EXISTS "weir:regtest:maturing:$T5" | tr -d '[:space:]')" = "0" ] || fail "conflicted record still in redis"
[ "$(rcli ZSCORE weir:regtest:tombstones "$T5" | tr -d '[:space:]')" != "" ] || fail "conflicted txid not tombstoned"
ok "conflicted, proven: conflictingTxid=$T6 mined in $H6"
mine 3

step "6. TTL expiry"
A6="$(bcli getnewaddress)"; watch_addr "$A6"; rcli ZADD weir:regtest:expiries "$(( $(date +%s) * 1000 - 1000 ))" "$A6" >/dev/null
mine 1; expect_event "expired address=$A6"
[ "$(rcli SISMEMBER weir:regtest:addresses "$A6" | tr -d '[:space:]')" = "0" ] || fail "expired watch still in the set"
[ "$(rcli ZSCORE weir:regtest:expiries "$A6" | tr -d '[:space:]')" = "" ] || fail "expired watch still in expiries"
ok "expired, watch removed from addresses + expiries"

step "7. webhook down: outbox holds, then drains"
A7="$(bcli getnewaddress)"; watch_addr "$A7"; stop_catcher
T7="$(bcli sendtoaddress "$A7" 0.1)"; mine 1; sleep 3
DEPTH="$(rcli ZCARD weir:regtest:outbox | tr -d '[:space:]')"
[ "$DEPTH" = "2" ] || fail "outbox should hold exactly the 2 undelivered events (seen + confirmed:1), has $DEPTH"
[ "$(rcli ZCARD weir:regtest:outbox:dead | tr -d '[:space:]')" = "0" ] || fail "dead-letter set not empty"
start_catcher; expect_event "seen txid=$T7 " 60; expect_event "confirmed txid=$T7 confs=1 " 60
for _ in $(seq 1 10); do [ "$(rcli ZCARD weir:regtest:outbox | tr -d '[:space:]')" = "0" ] && break; sleep 1; done
[ "$(rcli ZCARD weir:regtest:outbox | tr -d '[:space:]')" = "0" ] || fail "outbox not drained"
[ "$(rcli ZCARD weir:regtest:outbox:created | tr -d '[:space:]')" = "0" ] || fail "outbox:created not drained"
ok "2 events held while down (with backoff), delivered after restart, outbox empty"
mine 2; expect_event "confirmed txid=$T7 confs=3 "

step "8. restart mid-flight: boot reconciliation + gap walk"
A8="$(bcli getnewaddress)"; watch_addr "$A8"; T8="$(bcli sendtoaddress "$A8" 0.15)"; expect_event "seen txid=$T8 "
docker compose stop weir >/dev/null 2>&1; mine 3; H8="$(bcli getbestblockhash)"; docker compose start weir >/dev/null 2>&1
expect_event "confirmed txid=$T8 confs=1 " 90; expect_event "confirmed txid=$T8 confs=3 " 90
[ "$(count_events "seen txid=$T8 ")" = "1" ] || fail "seen duplicated across restart"
[ "$(count_events "confirmed txid=$T8 ")" = "2" ] || fail "expected exactly confirmed:1 + confirmed:3 for T8"
wait_ready
docker compose logs --no-log-prefix weir 2>&1 | grep "processed block $H8@" >/dev/null || fail "boot reconcile did not process the 3 missed blocks"
ok "caught up 3 blocks on boot, milestones fired from the gap walk, no duplicate seen"

step "9. /ready: 503 while reconciling, 200 with chainLag 0 after"
docker compose stop weir >/dev/null 2>&1; mine 150; docker compose start weir >/dev/null 2>&1
# poll from inside the container every 50ms: refused (process booting) → 503 (admin up, reconciling) → 200
SEQ="$(docker compose exec -T weir sh -c "for i in \$(seq 1 600); do s=\$(wget -qO- --server-response http://127.0.0.1:${PORT}/ready 2>&1 | grep -o 'HTTP/[0-9.]* [0-9][0-9][0-9]' | head -1 | awk '{print \$2}'); echo \"\${s:-refused}\"; [ \"\$s\" = 200 ] && exit 0; sleep 0.05; done; exit 1" | uniq | tr '\n' ' ')" || fail "/ready never returned 200 (observed: $SEQ)"
case "$SEQ" in *"503 "*"200"*) ;; *) fail "expected a 503 before the 200 (observed: $SEQ)";; esac
READY="$(docker compose exec -T weir wget -qO- "http://127.0.0.1:${PORT}/ready")"
[[ "$READY" == *'"ok":true'* ]] || fail "/ready ok != true: $READY"
[[ "$READY" == *'"chainLag":0'* ]] || fail "chainLag != 0 after catch-up: $READY"
[[ "$READY" == *"\"tipHeight\":$(bcli getblockcount)"* ]] || fail "tipHeight != node height: $READY"
ok "observed: $SEQ; chainLag 0 at height $(bcli getblockcount)"

step "global invariants"
DUP="$(grep -h '"idempotencyKey"' "$CATCH_LOG" | sort | uniq -d)"
[ -z "$DUP" ] || fail "idempotencyKey delivered more than once:"$'\n'"$DUP"
grep -q '\[catch\] BAD SIGNATURE' "$CATCH_LOG" && fail "catcher rejected a signature"
ERR_NOW="$(weir_errors)"
if [ "$ERR_NOW" != "$ERR_BASE" ]; then docker compose logs --no-log-prefix weir 2>&1 | grep '\[error\]' | tail -n "$((ERR_NOW - ERR_BASE))" >&2; fail "weir logged $((ERR_NOW - ERR_BASE)) error line(s) after boot"; fi
TOTALS="seen=$(count_events 'seen ') confirmed=$(count_events 'confirmed ') dropped=$(count_events 'dropped ') demoted=$(count_events 'demoted ') conflicted=$(count_events 'conflicted ') expired=$(count_events 'expired ')"
[ "$TOTALS" = "seen=8 confirmed=12 dropped=2 demoted=1 conflicted=1 expired=1" ] || fail "event totals differ from the nine scenarios' exact expectation: $TOTALS"
ok "every idempotencyKey delivered exactly once, all signatures verified, no weir [error] lines after boot, exact event totals"

step "ALL SCENARIOS PASSED"
printf '   events: %s\n' "$TOTALS"
printf '   weir error lines after boot: %s\n' "$((ERR_NOW - ERR_BASE))"
