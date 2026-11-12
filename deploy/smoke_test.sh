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
cimg=$(docker inspect -f '{{.Config.Image}}' controller)
docker run --rm -e VNODE_COUNT=0 "$cimg" >/dev/null 2>&1; check "controller VNODE_COUNT=0 exit code" "$?" "1"

echo "== graceful stop and restart keep data"
docker exec worker2 sh -c 'echo persisted > /data/marker' >/dev/null 2>&1
start=$(date +%s)
docker stop -t 10 worker2 >/dev/null 2>&1
took=$(( $(date +%s) - start ))
check "worker2 exits 0 on SIGTERM" "$(docker inspect -f '{{.State.ExitCode}}' worker2)" "0"
check "worker2 stops before kill timeout" "$([ "$took" -lt 5 ] && echo yes || echo no)" "yes"
check "worker2 logged shutdown" "$(docker logs worker2 2>&1 | grep -c 'received terminated')" "1"
docker start worker2 >/dev/null 2>&1; sleep 2
check "worker2 running after restart" "$(state worker2)" "running"
check "worker2 volume survived restart" "$(docker exec worker2 cat /data/marker 2>/dev/null)" "persisted"

echo "== result: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
