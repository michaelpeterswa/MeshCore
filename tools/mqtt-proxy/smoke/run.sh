#!/bin/sh
# Publishes the test device's messages to the LAN broker and checks what the
# fake analyzer receives: status and packets, never room topics.
set -eu
cd "$(dirname "$0")/.."
D=6C265917E1D4BA04FB14A9C2C985B5143FD74C9DA89472C2D731A9A1413A9FAD
B=meshcore/SEA/$D
pub() { docker compose -f compose.smoke.yaml exec -T lan mosquitto_pub -t "$B/$1" -s < "proxy/testdata/device/$2"; }

for i in $(seq 1 50); do curl -fs http://127.0.0.1:18080/healthz >/dev/null && break; sleep 0.2; done
docker compose -f compose.smoke.yaml exec -T analyzer mosquitto_sub -t '#' -v -W 5 > /tmp/mqtt-proxy-smoke.out 2>/dev/null &
sleep 1
pub status status.json
pub packets packets.json
pub room/posts room_posts.json
pub link link.json
wait || true
cat /tmp/mqtt-proxy-smoke.out
grep -q "$B/status" /tmp/mqtt-proxy-smoke.out || { echo "FAIL: status not forwarded"; exit 1; }
grep -q "$B/packets" /tmp/mqtt-proxy-smoke.out || { echo "FAIL: packets not forwarded"; exit 1; }
if grep -qE "/room/|/link" /tmp/mqtt-proxy-smoke.out; then echo "FAIL: LAN-only topic forwarded"; exit 1; fi
echo "PASS"
