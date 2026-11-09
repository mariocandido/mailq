#!/usr/bin/env bash
# End-to-end demo against `docker compose up`.
set -euo pipefail

API=${API:-http://localhost:8080}
RUN=$(date +%s)

send() { # send <json> [idempotency-key]
  local args=(-s -X POST "$API/send" -H 'Content-Type: application/json' -d "$1")
  [[ -n "${2:-}" ]] && args+=(-H "Idempotency-Key: $2")
  curl "${args[@]}"
}

ids_of() { grep -o '"id":"[^"]*"' | cut -d'"' -f4; }

show() {
  for id in "$@"; do
    curl -s "$API/messages/$id" | python3 -c '
import json, sys
d = json.load(sys.stdin)
m = d["message"]
codes = " -> ".join(str(a["code"] or "ERR") for a in d["attempts"])
print("  %-28s %-10s attempts=%d  [%s]  %s" % (m["to"], m["status"], m["attempts"], codes, m.get("last_error", "")[:70]))'
  done
}

echo "== 1. One request, one recipient per scenario"
BODY='{"from":"demo@mailq.local","to":[
  "ok-'$RUN'@fake.test","bounce-'$RUN'@fake.test","block-'$RUN'@fake.test",
  "flaky-'$RUN'@fake.test","full-'$RUN'@fake.test","hello-'$RUN'@mailpit.test",
  "nobody@unrouted.test"],
  "subject":"Olá from mailq","text":"Hello! This is a mailq demo message."}'
RESP=$(send "$BODY" "demo-$RUN")
echo "$RESP"
IDS=$(echo "$RESP" | ids_of)

echo
echo "== 2. Same Idempotency-Key again: same ids, nothing new is queued"
RESP2=$(send "$BODY" "demo-$RUN")
if [[ "$(echo "$RESP2" | ids_of)" == "$IDS" ]]; then echo "  OK, identical ids"; else echo "  MISMATCH"; fi

echo
echo "== 3. Status after 1s, then after the retries (~16s)"
sleep 1; show $IDS
sleep 15; echo "  ---"; show $IDS

echo
echo "== 4. Suppression: bounce-$RUN@fake.test hard-bounced, so a new send is not even attempted"
send '{"from":"demo@mailq.local","to":["bounce-'$RUN'@fake.test"],"subject":"again","text":"again"}'; echo

echo
echo "== 5. Burst of 40 to fake.test (it accepts 5/s, then answers 421 4.7.28)"
TO=$(for i in $(seq 1 40); do printf '"load-%s-%s@fake.test",' "$RUN" "$i"; done)
LOAD=$(send '{"from":"demo@mailq.local","to":['"${TO%,}"'],"subject":"load","text":"load test"}' | ids_of)
sleep 20
echo "  status counts for the burst:"
show $LOAD | awk '{print $2}' | sort | uniq -c
echo
echo "  adaptive rate and throttling metrics:"
curl -s "$API/metrics" | grep -E '^mailq_(domain_rate|throttled_total|delivery_attempts_total)' | grep 'fake.test' | sed 's/^/  /'

echo
echo "Mailpit UI (message to mailpit.test): http://localhost:8025"
