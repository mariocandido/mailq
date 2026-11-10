#!/usr/bin/env bash
# Sends a burst of N messages to fake.test and reports how much work it cost
# Postgres: row updates on the messages table, measured with pg_stat.
set -euo pipefail

API=${API:-http://localhost:8080}
N=${N:-40}
RUN=$(date +%s)

psql() { docker compose exec -T postgres psql -U mailq -d mailq -tAc "$1"; }
updates() { psql "SELECT n_tup_upd FROM pg_stat_user_tables WHERE relname = 'messages'"; }
throttled() { curl -s "$API/metrics" | awk '/^mailq_throttled_total\{domain="fake.test"\}/ {print $2}' | grep . || echo 0; }

before=$(updates); thr_before=$(throttled)
TO=$(for i in $(seq 1 "$N"); do printf '"burst-%s-%s@fake.test",' "$RUN" "$i"; done)
curl -s -X POST "$API/send" -H 'Content-Type: application/json' \
  -d '{"from":"demo@mailq.local","to":['"${TO%,}"'],"subject":"burst","text":"burst"}' >/dev/null
start=$(date +%s)

# Wait until every message of this burst has a final status.
while :; do
  pending=$(psql "SELECT count(*) FROM messages WHERE recipient LIKE 'burst-$RUN-%' AND status NOT IN ('delivered','bounced')")
  [[ "$pending" == 0 ]] && break
  (( $(date +%s) - start > 120 )) && { echo "timed out with $pending pending"; break; }
  sleep 0.5
done
elapsed=$(( $(date +%s) - start ))
sleep 2.5 # pg_stat is flushed asynchronously

delivered=$(psql "SELECT count(*) FROM messages WHERE recipient LIKE 'burst-$RUN-%' AND status = 'delivered'")
echo "messages:            $N"
echo "delivered:           $delivered"
echo "seconds to finish:   $elapsed"
echo "row updates (msgs):  $(( $(updates) - before ))"
echo "throttle events:     $(awk "BEGIN {print $(throttled) - $thr_before}")"
