#!/usr/bin/env bash
# Extracts the layer tarballs of real Docker images into testdata/layers/<image>/ for the dedup benchmark.
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
out="$root/testdata/layers"
# default image set: benchmark 1 (Docker layers) and its variants; build_app_images.sh and build_spring_images.sh must have run first; pass image names as arguments to fetch others
images=(ubuntu:20.04 ubuntu:22.04 nginx:latest node:18-slim python:3.11-slim python:3.10-slim python:3.9-slim minidfs-bench-app-requests:1 minidfs-bench-app-flask:1 minidfs-bench-app-numpy:1 minidfs-bench-spring-user-service:1 minidfs-bench-spring-order-service:1 minidfs-bench-spring-payment-service:1)
if [ "$#" -gt 0 ]; then images=("$@"); fi

for img in "${images[@]}"; do
	name=${img//[:\/]/_}
	dir="$out/$name"
	rm -rf "$dir"
	mkdir -p "$dir"
	tmp=$(mktemp -d)
	docker image inspect "$img" > /dev/null 2>&1 || docker pull -q "$img" > /dev/null
	docker save "$img" | tar -x -C "$tmp"
	# docker save may store layers gzip-compressed; the benchmark wants the plain tar a layer really is
	python3 -I - "$tmp" "$dir" <<'PY'
import gzip, json, os, shutil, sys

src, dst = sys.argv[1], sys.argv[2]
with open(os.path.join(src, "manifest.json")) as f:
    layers = json.load(f)[0]["Layers"]
for i, rel in enumerate(layers, 1):
    path = os.path.realpath(os.path.join(src, rel))
    if not path.startswith(os.path.realpath(src) + os.sep):
        sys.exit("layer path escapes the archive: " + rel)
    name = "%02d-%s.tar" % (i, os.path.basename(path)[:12])
    with open(path, "rb") as f:
        magic = f.read(2)
        f.seek(0)
        with open(os.path.join(dst, name), "wb") as o:
            if magic == b"\x1f\x8b":
                with gzip.GzipFile(fileobj=f) as g:
                    shutil.copyfileobj(g, o)
            else:
                shutil.copyfileobj(f, o)
PY
	rm -rf "$tmp"
	echo "$img: $(ls "$dir" | wc -l) layers, $(du -sh "$dir" | cut -f1)"
done
