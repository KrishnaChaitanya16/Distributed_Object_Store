#!/usr/bin/env bash
# Builds and boots the full stack, then checks container-level behaviour; run from the repo root.
set -uo pipefail

COMPOSE="docker compose -f deploy/docker-compose.yml"
PASS=0
FAIL=0

check() {
	if [ "$2" = "$3" ]; then echo "  ok   $1"; PASS=$((PASS + 1)); else echo "  FAIL $1 (got '$2', want '$3')"; FAIL=$((FAIL + 1)); fi
}
state() { docker inspect -f '{{.State.Status}}' "$1" 2>/dev/null; }
restarts() { docker inspect -f '{{.RestartCount}}' "$1" 2>/dev/null; }

cleanup() { $COMPOSE down -v --remove-orphans >/dev/null 2>&1; }
trap cleanup EXIT

echo "== compose file is valid"
$COMPOSE config -q >/dev/null 2>&1; check "docker compose config" "$?" "0"
check "default controller host port is 8080" "$($COMPOSE config 2>/dev/null | grep -c 'published: "8080"')" "1"
check "default worker2 host port is 8002" "$($COMPOSE config 2>/dev/null | grep -c 'published: "8002"')" "1"

# Shifted host ports so the test never collides with services already running on this machine.
export CONTROLLER_HOST_PORT=18080 WEBUI_HOST_PORT=18081 PROMETHEUS_HOST_PORT=19090 GRAFANA_HOST_PORT=13000
export WORKER1_HOST_PORT=18001 WORKER2_HOST_PORT=18002 WORKER3_HOST_PORT=18003 WORKER4_HOST_PORT=18004
check "host port override applies" "$($COMPOSE config 2>/dev/null | grep -c 'published: "18080"')" "1"

echo "== images build"
$COMPOSE build -q >/dev/null 2>&1; check "build controller + workers" "$?" "0"

echo "== stack boots and stays up"
$COMPOSE up -d >/dev/null 2>&1; check "compose up" "$?" "0"
sleep 8
for c in controller worker1 worker2 worker3 worker4 prometheus grafana; do
	check "$c running" "$(state "$c")" "running"
	check "$c not restart-looping" "$(restarts "$c")" "0"
done
check "worker1 logged its id" "$(docker logs worker1 2>&1 | grep -c 'worker worker1 starting')" "1"
targets=$(curl -s "localhost:$PROMETHEUS_HOST_PORT/api/v1/targets")
for t in controller:8080 worker1:8001 worker2:8001 worker3:8001 worker4:8001; do
	check "prometheus scrapes $t" "$(echo "$targets" | grep -q "\"$t\"" && echo yes || echo no)" "yes"
done
ds=""
for _ in $(seq 1 15); do
	ds=$(curl -s -u admin:admin "localhost:$GRAFANA_HOST_PORT/api/datasources" 2>/dev/null) && echo "$ds" | grep -q Prometheus && break
	sleep 2
done
check "grafana has provisioned Prometheus datasource" "$(echo "$ds" | grep -c '"url":"http://prometheus:9090"')" "1"

echo "== invalid config exits non-zero with a clear error"
img=$(docker inspect -f '{{.Config.Image}}' worker1)
out=$(docker run --rm "$img" 2>&1); code=$?
check "missing WORKER_ID exit code" "$code" "1"
check "missing WORKER_ID message" "$(echo "$out" | grep -c 'WORKER_ID is required')" "1"
docker run --rm -e WORKER_ID=w -e WORKER_PORT=99999 "$img" >/dev/null 2>&1; check "bad WORKER_PORT exit code" "$?" "1"
docker run --rm -e WORKER_ID=w -e CONTROLLER_ADDRESS=controller "$img" >/dev/null 2>&1; check "bad CONTROLLER_ADDRESS exit code" "$?" "1"

echo "== controller"
check "controller answers list objects" "$(curl -s -o /dev/null -w '%{http_code}' "localhost:$CONTROLLER_HOST_PORT/api/v1/objects")" "200"
check "controller db is on the persistent volume" "$(docker exec controller test -f /data/meta.db && echo yes || echo no)" "yes"

echo "== end to end through the controller"
C="http://localhost:$CONTROLLER_HOST_PORT"
tmpe=$(mktemp -d)
for w in worker1 worker2 worker3 worker4; do
	check "$w registered with controller" "$(docker logs "$w" 2>&1 | grep -c "registered with controller as $w")" "1"
done
head -c 2621440 /dev/urandom > "$tmpe/obj"
up=$(curl -s -w '\n%{http_code}' -F "file=@$tmpe/obj" "$C/api/v1/objects")
check "upload 2.5 MiB object" "$(echo "$up" | tail -1)" "201"
oid=$(echo "$up" | head -1 | grep -o '"object_id":"[^"]*"' | cut -d'"' -f4)
check "upload split into 3 chunks" "$(echo "$up" | head -1 | grep -o '"chunks":[0-9]*' | cut -d: -f2)" "3"
curl -s "$C/api/v1/objects/$oid" -o "$tmpe/back"
check "download is byte-identical" "$(sha256sum < "$tmpe/back" | cut -d' ' -f1)" "$(sha256sum < "$tmpe/obj" | cut -d' ' -f1)"
for w in 1 2 3 4; do
	p=$(eval echo "\$WORKER${w}_HOST_PORT")
	curl -s "localhost:$p/internal/workers/worker$w/chunks" | grep -o '[0-9a-f]\{64\}'
done | sort | uniq -c > "$tmpe/replicas"
check "3 distinct chunks stored" "$(wc -l < "$tmpe/replicas" | tr -d ' ')" "3"
check "every chunk on exactly 2 workers" "$(awk '$1 != 2' "$tmpe/replicas" | wc -l | tr -d ' ')" "0"
check "delete object" "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$C/api/v1/objects/$oid")" "204"
check "deleted object is gone" "$(curl -s -o /dev/null -w '%{http_code}' "$C/api/v1/objects/$oid")" "404"
rm -rf "$tmpe"

echo "== worker data plane over real HTTP"
W1="http://localhost:$WORKER1_HOST_PORT"
W2="http://localhost:$WORKER2_HOST_PORT"
tmp=$(mktemp -d); trap 'cleanup; rm -rf "$tmp"' EXIT
head -c 1048576 /dev/urandom > "$tmp/chunk"
id=$(sha256sum "$tmp/chunk" | cut -d' ' -f1)
code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
check "PUT 1 MiB chunk to worker1" "$(code -X PUT --data-binary @"$tmp/chunk" "$W1/internal/chunks/$id")" "201"
curl -s "$W1/internal/chunks/$id" -o "$tmp/back"
check "GET returns identical bytes" "$(sha256sum < "$tmp/back" | cut -d' ' -f1)" "$id"
check "PUT with wrong hash rejected" "$(code -X PUT --data-binary 'x' "$W1/internal/chunks/$id")" "400"
check "worker1 inventory lists the chunk" "$(curl -s "$W1/internal/workers/worker1/chunks" | grep -c "$id")" "1"
check "worker2 does not have it yet" "$(code "$W2/internal/chunks/$id")" "404"
check "replicate-peer worker1 -> worker2" "$(code -X POST -d "{\"chunk_id\":\"$id\",\"target_addr\":\"worker2:8001\"}" "$W1/internal/workers/replicate-peer")" "200"
curl -s "$W2/internal/chunks/$id" -o "$tmp/peer"
check "worker2 copy is identical" "$(sha256sum < "$tmp/peer" | cut -d' ' -f1)" "$id"

echo "== graceful stop and restart keep data"
start=$(date +%s)
docker stop -t 10 worker2 >/dev/null 2>&1
took=$(( $(date +%s) - start ))
check "worker2 exits 0 on SIGTERM" "$(docker inspect -f '{{.State.ExitCode}}' worker2)" "0"
check "worker2 stops before kill timeout" "$([ "$took" -lt 5 ] && echo yes || echo no)" "yes"
check "worker2 logged shutdown" "$(docker logs worker2 2>&1 | grep -c 'worker worker2 shutting down')" "1"
check "worker2 unreachable while stopped" "$(code --max-time 2 "$W2/internal/chunks/$id")" "000"
docker start worker2 >/dev/null 2>&1; sleep 2
check "worker2 running after restart" "$(state worker2)" "running"
curl -s "$W2/internal/chunks/$id" -o "$tmp/after"
check "worker2 chunk survived restart" "$(sha256sum < "$tmp/after" | cut -d' ' -f1)" "$id"
check "worker2 delete" "$(code -X DELETE "$W2/internal/chunks/$id")" "204"
check "worker2 repeat delete is idempotent" "$(code -X DELETE "$W2/internal/chunks/$id")" "204"

echo "== result: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
