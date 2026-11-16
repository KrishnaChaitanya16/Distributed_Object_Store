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
# GC passes run only on worker joins and explicit triggers, so every check below sees a predictable cluster.
export GC_INTERVAL_S=3600 ORPHAN_GRACE_S=30
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
docker run --rm -e WORKER_ID=w -e SCRUB_INTERVAL_S=0 "$img" >/dev/null 2>&1; check "zero SCRUB_INTERVAL_S exit code" "$?" "1"
docker run --rm -e WORKER_ID=w -e SCRUB_PAUSE_MS=-1 "$img" >/dev/null 2>&1; check "negative SCRUB_PAUSE_MS exit code" "$?" "1"

echo "== controller"
check "controller answers list objects" "$(curl -s -o /dev/null -w '%{http_code}' "localhost:$CONTROLLER_HOST_PORT/api/v1/objects")" "200"
check "controller db is on the persistent volume" "$(docker exec controller test -f /data/meta.db && echo yes || echo no)" "yes"

code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }

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
left=0
for c in $(awk '{print $2}' "$tmpe/replicas"); do
	for w in 1 2 3 4; do
		p=$(eval echo "\$WORKER${w}_HOST_PORT")
		curl -s "localhost:$p/internal/workers/worker$w/chunks" | grep -q "$c" && left=$((left+1))
	done
done
check "delete removed every copy of its chunks from the workers" "$left" "0"
check "second delete of the same object is 404" "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$C/api/v1/objects/$oid")" "404"
rm -rf "$tmpe"

echo "== data integrity: scrubber detects corruption, reads fall back, copy is restored"
wport() { eval echo "\$WORKER${1#worker}_HOST_PORT"; }
jsonnum() { grep -o "\"$1\":[0-9]*" | head -1 | cut -d: -f2; }
scrub() {
	before=$(curl -s "localhost:$(wport "$1")/internal/scrubber/status" | jsonnum passes)
	curl -s -o /dev/null -X POST "localhost:$(wport "$1")/internal/scrubber/run"
	for _ in $(seq 1 50); do
		st=$(curl -s "localhost:$(wport "$1")/internal/scrubber/status")
		[ "$(echo "$st" | jsonnum passes)" -gt "$before" ] && echo "$st" | grep -q '"running":false' && break
		sleep 0.2
	done
	echo "$st"
}
st=$(curl -s "localhost:$WORKER1_HOST_PORT/internal/scrubber/status")
check "scrubber status endpoint answers" "$(echo "$st" | grep -c '"interval_seconds":30')" "1"
tmpi=$(mktemp -d)
head -c 2621440 /dev/urandom > "$tmpi/obj"
want=$(sha256sum < "$tmpi/obj" | cut -d' ' -f1)
up=$(curl -s -w '\n%{http_code}' -F "file=@$tmpi/obj" "$C/api/v1/objects")
check "upload object for integrity test" "$(echo "$up" | tail -1)" "201"
ioid=$(echo "$up" | head -1 | grep -o '"object_id":"[^"]*"' | cut -d'"' -f4)
cid=""
for w in worker1 worker2 worker3 worker4; do
	curl -s "localhost:$(wport $w)/internal/workers/$w/chunks" | grep -o '[0-9a-f]\{64\}' | sed "s/$/ $w/"
done | sort > "$tmpi/placement"
cid=$(head -1 "$tmpi/placement" | cut -d' ' -f1)
holders=$(grep "^$cid " "$tmpi/placement" | cut -d' ' -f2 | tr '\n' ' ')
check "chunk under test has 2 holders" "$(echo $holders | wc -w | tr -d ' ')" "2"
for bad in $holders; do
	good=$(echo $holders | tr ' ' '\n' | grep -v "^$bad$")
	docker exec "$bad" sh -c "dd if=/dev/urandom of=/data/chunks/$cid.bin bs=16 count=1 seek=10 conv=notrunc 2>/dev/null"
	check "$bad: corruption injected (bytes no longer match id)" "$([ "$(curl -s "localhost:$(wport $bad)/internal/chunks/$cid" | sha256sum | cut -d' ' -f1)" != "$cid" ] && echo yes || echo no)" "yes"
	check "$good: other copy still intact" "$(curl -s "localhost:$(wport $good)/internal/chunks/$cid" | sha256sum | cut -d' ' -f1)" "$cid"
	st=$(scrub "$bad")
	check "$bad: scrubber quarantined 1 chunk this pass" "$(echo "$st" | jsonnum last_pass_corrupt)" "1"
	check "$bad: status names the chunk" "$(echo "$st" | grep -c "$cid")" "1"
	check "$bad: corrupt copy no longer served" "$(code "localhost:$(wport $bad)/internal/chunks/$cid")" "404"
	check "$bad: inventory drops it" "$(curl -s "localhost:$(wport $bad)/internal/workers/$bad/chunks" | grep -c "$cid")" "0"
	check "$bad: evidence kept in quarantine" "$(docker exec "$bad" sh -c "ls /data/quarantine | grep -c ^$cid")" "1"
	check "$bad: controller download still byte-identical" "$(curl -s "$C/api/v1/objects/$ioid" | sha256sum | cut -d' ' -f1)" "$want"
	st=$(scrub "$good")
	check "$good: healthy copy not quarantined" "$(echo "$st" | jsonnum last_pass_corrupt)" "0"
	check "$bad: restore via replicate-peer from $good" "$(code -X POST -d "{\"chunk_id\":\"$cid\",\"target_addr\":\"$bad:8001\"}" "localhost:$(wport $good)/internal/workers/replicate-peer")" "200"
	check "$bad: restored copy identical" "$(curl -s "localhost:$(wport $bad)/internal/chunks/$cid" | sha256sum | cut -d' ' -f1)" "$cid"
	st=$(scrub "$bad")
	check "$bad: restored copy passes next scrub" "$(echo "$st" | jsonnum last_pass_corrupt)" "0"
done
check "controller fell back to the other copy at least once" "$([ "$(docker logs controller 2>&1 | grep 'chunk fetch failed' | grep -c "$cid")" -ge 1 ] && echo yes || echo no)" "yes"

echo "== controller read path: SHA-256 check, read-repair, hedging, metadata locations"
for w in worker1 worker2 worker3 worker4; do scrub "$w" >/dev/null; done
tmpr=$(mktemp -d)
head -c 4718592 /dev/urandom > "$tmpr/obj"
rwant=$(sha256sum < "$tmpr/obj" | cut -d' ' -f1)
up=$(curl -s -w '\n%{http_code}' -F "file=@$tmpr/obj" "$C/api/v1/objects")
check "upload 4.5 MiB object for read-path test" "$(echo "$up" | tail -1)" "201"
roid=$(echo "$up" | head -1 | grep -o '"object_id":"[^"]*"' | cut -d'"' -f4)
md=$(curl -s "$C/api/v1/objects/$roid/metadata")
locs=$(echo "$md" | grep -o '"locations":\["worker[0-9]","worker[0-9]"\]')
check "metadata lists 5 chunks" "$(echo "$md" | grep -o '"chunk_id"' | wc -l | tr -d ' ')" "5"
check "every chunk has 2 locations" "$(echo "$locs" | wc -l | tr -d ' ')" "5"
check "locations are distinct workers" "$(echo "$locs" | awk -F'"' '$4==$6' | wc -l | tr -d ' ')" "0"
check "offsets add up" "$(echo "$md" | grep -o '"offset":[0-9]*' | cut -d: -f2 | tr '\n' ' ')" "0 1048576 2097152 3145728 4194304 "
check "metadata keeps system fields" "$(echo "$md" | grep -c '"object_id".*"size_bytes":4718592.*"chunk_count":5')" "1"
check "download Content-Length is set" "$(curl -sI -X GET -D - -o /dev/null "$C/api/v1/objects/$roid" | tr -d '\r' | grep -i '^content-length:' | cut -d' ' -f2)" "4718592"
et=$(curl -si "$C/api/v1/objects/$roid/metadata" | tr -d '\r' | grep -i '^etag:' | cut -d' ' -f2)
check "metadata response carries an ETag" "$([ -n "$et" ] && echo yes || echo no)" "yes"
check "PATCH with matching If-Match succeeds" "$(code -X PATCH -H "If-Match: $et" -d '{"k":"v"}' "$C/api/v1/objects/$roid/metadata")" "200"
check "PATCH with stale If-Match is a 409" "$(code -X PATCH -H "If-Match: $et" -d '{"k":"w"}' "$C/api/v1/objects/$roid/metadata")" "409"
check "custom metadata kept after rejected PATCH" "$(curl -s "$C/api/v1/objects/$roid/metadata" | grep -c '"k":"v"')" "1"

rcid=$(echo "$md" | grep -o '"chunk_id":"[0-9a-f]*"' | sed -n 3p | cut -d'"' -f4)
rpri=$(echo "$locs" | sed -n 3p | grep -o 'worker[0-9]' | sed -n 1p)
docker exec "$rpri" sh -c "dd if=/dev/urandom of=/data/chunks/$rcid.bin bs=16 count=1 seek=7 conv=notrunc 2>/dev/null"
check "$rpri copy is corrupt" "$([ "$(curl -s "localhost:$(wport $rpri)/internal/chunks/$rcid" | sha256sum | cut -d' ' -f1)" != "$rcid" ] && echo yes || echo no)" "yes"
check "download byte-identical despite corrupt primary (no scrub yet)" "$(curl -s "$C/api/v1/objects/$roid" | sha256sum | cut -d' ' -f1)" "$rwant"
for _ in $(seq 1 20); do docker logs controller 2>&1 | grep 'read-repair succeeded' | grep -q "$rcid" && break; sleep 0.5; done
check "controller logged the integrity failure" "$([ "$(docker logs controller 2>&1 | grep 'chunk integrity check failed' | grep -c "$rcid")" -ge 1 ] && echo yes || echo no)" "yes"
check "controller logged read-repair success" "$([ "$(docker logs controller 2>&1 | grep 'read-repair succeeded' | grep -c "$rcid")" -ge 1 ] && echo yes || echo no)" "yes"
check "$rpri copy repaired in place" "$(curl -s "localhost:$(wport $rpri)/internal/chunks/$rcid" | sha256sum | cut -d' ' -f1)" "$rcid"
st=$(scrub "$rpri")
check "repaired copy passes the scrubber" "$(echo "$st" | jsonnum last_pass_corrupt)" "0"

hp=$(echo "$locs" | sed -n 1p | grep -o 'worker[0-9]' | sed -n 1p)
docker pause "$hp" >/dev/null
t0=$(date +%s)
hgot=$(curl -s -m 20 "$C/api/v1/objects/$roid" | sha256sum | cut -d' ' -f1)
took=$(( $(date +%s) - t0 ))
docker unpause "$hp" >/dev/null
check "download identical while $hp is frozen (hedged read)" "$hgot" "$rwant"
check "frozen worker did not stall the download (<4s)" "$([ "$took" -lt 4 ] && echo yes || echo no)" "yes"
sleep 3

head -c 300000 /dev/urandom > "$tmpr/small"
suoid=$(curl -s -F "file=@$tmpr/small" "$C/api/v1/objects" | grep -o '"object_id":"[^"]*"' | cut -d'"' -f4)
sm=$(curl -s "$C/api/v1/objects/$suoid/metadata")
scid=$(echo "$sm" | grep -o '"chunk_id":"[0-9a-f]*"' | head -1 | cut -d'"' -f4)
for w in $(echo "$sm" | grep -o '"locations":\["worker[0-9]","worker[0-9]"\]' | grep -o 'worker[0-9]'); do
	docker exec "$w" sh -c "dd if=/dev/urandom of=/data/chunks/$scid.bin bs=16 count=1 seek=5 conv=notrunc 2>/dev/null"
done
check "both copies of chunk 0 corrupt -> 502" "$(curl -s -o "$tmpr/body" -w '%{http_code}' "$C/api/v1/objects/$suoid")" "502"
check "502 body is the JSON error, no object bytes" "$(grep -c 'no valid replica' "$tmpr/body")" "1"

lcid=$(echo "$md" | grep -o '"chunk_id":"[0-9a-f]*"' | sed -n 4p | cut -d'"' -f4)
for w in $(echo "$locs" | sed -n 4p | grep -o 'worker[0-9]'); do
	docker exec "$w" sh -c "dd if=/dev/urandom of=/data/chunks/$lcid.bin bs=16 count=1 seek=9 conv=notrunc 2>/dev/null"
done
curl -s -o "$tmpr/partial" "$C/api/v1/objects/$roid"; rc=$?
check "both copies of a later chunk corrupt -> client sees a failed transfer" "$([ "$rc" -ne 0 ] && echo yes || echo no)" "yes"
n=$(stat -c %s "$tmpr/partial")
check "client got only the 3 verified chunks before it" "$n" "3145728"
check "and those bytes are an exact prefix of the object" "$(head -c "$n" "$tmpr/obj" | sha256sum | cut -d' ' -f1)" "$(sha256sum < "$tmpr/partial" | cut -d' ' -f1)"
curl -s -o /dev/null -X DELETE "$C/api/v1/objects/$roid"
curl -s -o /dev/null -X DELETE "$C/api/v1/objects/$suoid"
rm -rf "$tmpr"
before=$(curl -s "localhost:$(wport "$bad")/internal/scrubber/status" | jsonnum corrupt_quarantined)
docker exec "$bad" sh -c "dd if=/dev/urandom of=/data/chunks/$cid.bin bs=16 count=1 seek=3 conv=notrunc 2>/dev/null"
found=no
for _ in $(seq 1 45); do
	[ "$(curl -s "localhost:$(wport "$bad")/internal/scrubber/status" | jsonnum corrupt_quarantined)" -gt "$before" ] && found=yes && break
	sleep 1
done
check "periodic scrub (30s timer, no trigger) finds corruption" "$found" "yes"
code -X POST -d "{\"chunk_id\":\"$cid\",\"target_addr\":\"$bad:8001\"}" "localhost:$(wport $good)/internal/workers/replicate-peer" >/dev/null
docker restart "$bad" >/dev/null 2>&1; sleep 3
check "quarantine survives worker restart" "$(docker exec "$bad" sh -c "ls /data/quarantine | grep -c ^$cid")" "2"
check "restored chunk still served after restart" "$(curl -s "localhost:$(wport $bad)/internal/chunks/$cid" | sha256sum | cut -d' ' -f1)" "$cid"
curl -s -o /dev/null -X DELETE "$C/api/v1/objects/$ioid"
rm -rf "$tmpi"

echo "== worker data plane over real HTTP"
W1="http://localhost:$WORKER1_HOST_PORT"
W2="http://localhost:$WORKER2_HOST_PORT"
tmp=$(mktemp -d); trap 'cleanup; rm -rf "$tmp"' EXIT
head -c 1048576 /dev/urandom > "$tmp/chunk"
id=$(sha256sum "$tmp/chunk" | cut -d' ' -f1)
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
shutdowns_before=$(docker logs worker2 2>&1 | grep -c 'worker worker2 shutting down')
start=$(date +%s)
docker stop -t 10 worker2 >/dev/null 2>&1
took=$(( $(date +%s) - start ))
check "worker2 exits 0 on SIGTERM" "$(docker inspect -f '{{.State.ExitCode}}' worker2)" "0"
check "worker2 stops before kill timeout" "$([ "$took" -lt 5 ] && echo yes || echo no)" "yes"
check "worker2 logged shutdown" "$(( $(docker logs worker2 2>&1 | grep -c 'worker worker2 shutting down') - shutdowns_before ))" "1"
check "worker2 unreachable while stopped" "$(code --max-time 2 "$W2/internal/chunks/$id")" "000"
docker start worker2 >/dev/null 2>&1; sleep 2
check "worker2 running after restart" "$(state worker2)" "running"
curl -s "$W2/internal/chunks/$id" -o "$tmp/after"
check "worker2 chunk survived restart" "$(sha256sum < "$tmp/after" | cut -d' ' -f1)" "$id"
check "worker2 delete" "$(code -X DELETE "$W2/internal/chunks/$id")" "204"
check "worker2 repeat delete is idempotent" "$(code -X DELETE "$W2/internal/chunks/$id")" "204"
code -X DELETE "$W1/internal/chunks/$id" >/dev/null

echo "== garbage collection and replica reconciliation"
gcstatus() { curl -s "$C/internal/gc/status"; }
gcrun() {
	gbefore=$(gcstatus | jsonnum passes)
	curl -s -o /dev/null -X POST "$C/internal/gc/run"
	for _ in $(seq 1 50); do
		gst=$(gcstatus)
		[ "$(echo "$gst" | jsonnum passes)" -gt "$gbefore" ] && echo "$gst" | grep -q '"running":false' && break
		sleep 0.2
	done
	echo "$gst"
}
holders() { for w in worker1 worker2 worker3 worker4; do curl -s "localhost:$(wport $w)/internal/workers/$w/chunks" | grep -q "$1" && echo "$w"; done | tr '\n' ' '; }
gst=$(gcstatus)
check "gc status answers" "$(echo "$gst" | grep -c '"interval_seconds":3600')" "1"
check "gc status shows the orphan grace" "$(echo "$gst" | grep -c '"orphan_grace_seconds":30')" "1"
check "gc run is accepted" "$(code -X POST "$C/internal/gc/run")" "202"
tmpg=$(mktemp -d)

head -c 1048576 /dev/urandom > "$tmpg/orphan"
orphan=$(sha256sum < "$tmpg/orphan" | cut -d' ' -f1)
check "plant an orphan chunk directly on worker3" "$(code -X PUT --data-binary @"$tmpg/orphan" "localhost:$(wport worker3)/internal/chunks/$orphan")" "201"
planted=$(date +%s)
gcrun >/dev/null
check "orphan survives a pass inside its grace period" "$(holders "$orphan")" "worker3 "

head -c 700000 /dev/urandom > "$tmpg/shared"
s1=$(curl -s -F "file=@$tmpg/shared" "$C/api/v1/objects" | grep -o '"object_id":"[^"]*"' | cut -d'"' -f4)
s2=$(curl -s -F "file=@$tmpg/shared" "$C/api/v1/objects" | grep -o '"object_id":"[^"]*"' | cut -d'"' -f4)
scid=$(sha256sum < "$tmpg/shared" | cut -d' ' -f1)
check "identical uploads share one chunk on 2 workers" "$(holders "$scid" | wc -w | tr -d ' ')" "2"
check "delete the first sharer" "$(code -X DELETE "$C/api/v1/objects/$s1")" "204"
check "shared chunk kept while still referenced" "$(holders "$scid" | wc -w | tr -d ' ')" "2"
check "second sharer still downloads identical" "$(curl -s "$C/api/v1/objects/$s2" | sha256sum | cut -d' ' -f1)" "$scid"
check "delete the last sharer" "$(code -X DELETE "$C/api/v1/objects/$s2")" "204"
check "chunk removed once nothing references it" "$(holders "$scid" | wc -w | tr -d ' ')" "0"
s3=$(curl -s -F "file=@$tmpg/shared" "$C/api/v1/objects" | grep -o '"object_id":"[^"]*"' | cut -d'"' -f4)
check "re-uploading deleted content stores it again" "$(holders "$scid" | wc -w | tr -d ' ')" "2"
check "and it downloads identical" "$(curl -s "$C/api/v1/objects/$s3" | sha256sum | cut -d' ' -f1)" "$scid"

hcid=$scid
hbad=$(holders "$hcid" | cut -d' ' -f1)
docker exec "$hbad" sh -c "dd if=/dev/urandom of=/data/chunks/$hcid.bin bs=16 count=1 seek=20 conv=notrunc 2>/dev/null"
st=$(scrub "$hbad")
check "scrubber quarantines the corrupt copy" "$(echo "$st" | jsonnum last_pass_corrupt)" "1"
check "$hbad no longer has the chunk" "$(code "localhost:$(wport $hbad)/internal/chunks/$hcid")" "404"
gst=$(gcrun)
check "reconciler restored the quarantined replica by itself" "$(curl -s "localhost:$(wport $hbad)/internal/chunks/$hcid" | sha256sum | cut -d' ' -f1)" "$hcid"
check "gc pass reports the restore" "$(echo "$gst" | jsonnum replicas_restored)" "1"
st=$(scrub "$hbad")
check "restored copy passes the scrubber" "$(echo "$st" | jsonnum last_pass_corrupt)" "0"

xsrc=$(holders "$hcid" | cut -d' ' -f1)
xtgt=""
for w in worker1 worker2 worker3 worker4; do
	echo " $(holders "$hcid") " | grep -q " $w " || { xtgt=$w; break; }
done
check "make an extra copy on $xtgt" "$(code -X POST -d "{\"chunk_id\":\"$hcid\",\"target_addr\":\"$xtgt:8001\"}" "localhost:$(wport $xsrc)/internal/workers/replicate-peer")" "200"
check "chunk now has 3 copies" "$(holders "$hcid" | wc -w | tr -d ' ')" "3"
gst=$(gcrun)
check "reconciler removed the unlisted copy" "$(holders "$hcid" | wc -w | tr -d ' ')" "2"
check "the removed copy was the extra one on $xtgt" "$(holders "$hcid" | grep -c "$xtgt")" "0"
check "gc pass reports the excess delete" "$(echo "$gst" | jsonnum excess_deleted)" "1"
locs=$(curl -s "$C/api/v1/objects/$s3/metadata" | grep -o '"locations":\[[^]]*\]' | grep -o 'worker[0-9]' | sort | tr '\n' ' ')
check "remaining copies are exactly the listed replicas" "$locs" "$(holders "$hcid" | tr ' ' '\n' | grep . | sort | tr '\n' ' ')"
check "download still identical" "$(curl -s "$C/api/v1/objects/$s3" | sha256sum | cut -d' ' -f1)" "$hcid"
gst=$(gcrun)
check "a clean cluster needs no work" "$(echo "$gst" | grep -o '"chunks_deleted":[0-9]*,"excess_deleted":[0-9]*,"replicas_restored":[0-9]*')" '"chunks_deleted":0,"excess_deleted":0,"replicas_restored":0'
check "no chunk reported lost" "$(echo "$gst" | jsonnum lost_chunks)" "0"

wait_s=$(( 31 - ($(date +%s) - planted) ))
[ "$wait_s" -gt 0 ] && sleep "$wait_s"
gst=$(gcrun)
check "orphan deleted after its grace period" "$(holders "$orphan")" ""
check "gc pass counted it" "$([ "$(echo "$gst" | jsonnum chunks_deleted)" -ge 1 ] && echo yes || echo no)" "yes"
m=$(curl -s "$C/metrics")
for k in minidfs_gc_chunks_deleted_total minidfs_reconcile_excess_deleted_total minidfs_reconcile_replicas_restored_total minidfs_chunks_lost; do
	check "metric $k exported" "$(echo "$m" | grep -c "^$k ")" "1"
done
check "chunks_lost gauge is 0" "$(echo "$m" | grep '^minidfs_chunks_lost ' | awk '{print $2}')" "0"
curl -s -o /dev/null -X DELETE "$C/api/v1/objects/$s3"
rm -rf "$tmpg"

echo "== idempotency keys"
tmpk=$(mktemp -d)
head -c 300000 /dev/urandom > "$tmpk/f"
kwant=$(sha256sum < "$tmpk/f" | cut -d' ' -f1)
kn() { curl -s "$C/api/v1/objects" | grep -o '"object_id"' | wc -l | tr -d ' '; }
kbefore=$(kn)
check "bad request with a key (no file) is rejected" "$(code -X POST -H 'Idempotency-Key: smoke-1' -F 'metadata={}' "$C/api/v1/objects")" "400"
k1=$(curl -s -D "$tmpk/h1" -H 'Idempotency-Key: smoke-1' -F "file=@$tmpk/f" "$C/api/v1/objects")
check "same key works right after the failed attempt (key was released)" "$(head -1 "$tmpk/h1" | cut -d' ' -f2)" "201"
k2=$(curl -s -D "$tmpk/h2" -H 'Idempotency-Key: smoke-1' -F "file=@$tmpk/f" "$C/api/v1/objects")
check "retry with the same key replays the first response" "$k2" "$k1"
check "replay is flagged" "$(grep -ci '^x-idempotency-replayed: true' "$tmpk/h2")" "1"
check "only one object was stored" "$(( $(kn) - kbefore ))" "1"
for i in 1 2 3 4 5 6; do (code -H 'Idempotency-Key: smoke-par' -F "file=@$tmpk/f" "$C/api/v1/objects" > "$tmpk/p$i") & done; wait
check "6 parallel uploads with one key: at least one created" "$([ "$(cat "$tmpk"/p* | grep -c 201)" -ge 1 ] && echo yes || echo no)" "yes"
check "6 parallel uploads with one key: every other one is a conflict or a replay" "$(cat "$tmpk"/p* | grep -vc '201\|409')" "0"
check "6 parallel uploads with one key stored exactly one more object" "$(( $(kn) - kbefore ))" "2"
kid=$(echo "$k1" | grep -o '"object_id":"[^"]*"' | cut -d'"' -f4)
check "keyed object downloads identical" "$(curl -s "$C/api/v1/objects/$kid" | sha256sum | cut -d' ' -f1)" "$kwant"
rm -rf "$tmpk"

echo "== rebalancer: RF=2 restored after worker failure"
# Upload a small object and find which two workers hold its chunk.
tmprb=$(mktemp -d)
head -c 524288 /dev/urandom > "$tmprb/obj"
rbup=$(curl -s -w '\n%{http_code}' -F "file=@$tmprb/obj" "$C/api/v1/objects")
check "rebalancer: upload object" "$(echo "$rbup" | tail -1)" "201"
rboid=$(echo "$rbup" | head -1 | grep -o '"object_id":"[^"]*"' | cut -d'"' -f4)
rbmd=$(curl -s "$C/api/v1/objects/$rboid/metadata")
rbcid=$(echo "$rbmd" | grep -o '"chunk_id":"[0-9a-f]*"' | head -1 | cut -d'"' -f4)
# Find the two workers that hold this chunk
rbholders=""
for w in worker1 worker2 worker3 worker4; do
	if curl -s "localhost:$(wport $w)/internal/workers/$w/chunks" | grep -q "$rbcid"; then
		rbholders="$rbholders $w"
	fi
done
rbholders=$(echo $rbholders | xargs)
check "rebalancer: chunk has 2 initial holders" "$(echo $rbholders | wc -w | tr -d ' ')" "2"

# Stop one holder to simulate failure
rbvictim=$(echo $rbholders | cut -d' ' -f1)
rbsurvivors=$(echo $rbholders | cut -d' ' -f2-)
docker stop -t 2 "$rbvictim" >/dev/null 2>&1
check "rebalancer: victim stopped" "$(state $rbvictim)" "exited"

# Wait up to 20 s for the rebalancer to restore RF=2 on a new worker
rbrestored=no
for _ in $(seq 1 40); do
	rbcount=0
	for w in worker1 worker2 worker3 worker4; do
		[ "$(state $w)" = "running" ] || continue
		curl -s "localhost:$(wport $w)/internal/workers/$w/chunks" | grep -q "$rbcid" && rbcount=$((rbcount+1))
	done
	[ "$rbcount" -ge 2 ] && rbrestored=yes && break
	sleep 0.5
done
check "rebalancer: RF=2 restored on surviving workers within 20s" "$rbrestored" "yes"

# Download must still work with victim down
rbwant=$(sha256sum < "$tmprb/obj" | cut -d' ' -f1)
check "rebalancer: download still byte-identical with victim down" \
	"$(curl -s "$C/api/v1/objects/$rboid" | sha256sum | cut -d' ' -f1)" "$rbwant"

# Bring victim back — it should NOT affect the now-restored RF=2
docker start "$rbvictim" >/dev/null 2>&1; sleep 3
check "rebalancer: victim restarted" "$(state $rbvictim)" "running"

# bbolt should show exactly 2 replica entries (no 3-replica accumulation)
rbreplicas=$(echo "$rbmd" | grep -o '"locations":\["worker[0-9]","worker[0-9]"\]' | head -1 | grep -o 'worker[0-9]' | wc -l | tr -d ' ')
# Re-fetch metadata to get current replica state after rebalancer ran
rbmd2=$(curl -s "$C/api/v1/objects/$rboid/metadata")
rbreplicas2=$(echo "$rbmd2" | grep -o '"chunk_id":"'"$rbcid"'"' | wc -l | tr -d ' ')
# The rejoined victim still has its old copy; the reconciler runs on the join and removes it.
for _ in $(seq 1 30); do
	rblive=0
	for w in worker1 worker2 worker3 worker4; do
		[ "$(state $w)" = "running" ] || continue
		curl -s "localhost:$(wport $w)/internal/workers/$w/chunks" | grep -q "$rbcid" && rblive=$((rblive+1))
	done
	[ "$rblive" -eq 2 ] && break
	sleep 1
done
check "rebalancer: no 3-replica accumulation (exactly 2 live holders)" "$rblive" "2"
check "rebalancer: the rejoined victim's stale copy is the one removed" "$(curl -s "localhost:$(wport $rbvictim)/internal/workers/$rbvictim/chunks" | grep -c "$rbcid")" "0"
check "rebalancer: download still works after victim rejoins" \
	"$(curl -s "$C/api/v1/objects/$rboid" | sha256sum | cut -d' ' -f1)" "$rbwant"

# Verify Prometheus counter was incremented
rbmetric=$(curl -s "localhost:$CONTROLLER_HOST_PORT/metrics" | grep '^minidfs_rebalancer_repairs_total' | awk '{print $2}')
check "rebalancer: prometheus counter > 0" "$([ "${rbmetric:-0}" -gt 0 ] 2>/dev/null && echo yes || echo no)" "yes"

curl -s -o /dev/null -X DELETE "$C/api/v1/objects/$rboid"
rm -rf "$tmprb"

echo "== result: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
