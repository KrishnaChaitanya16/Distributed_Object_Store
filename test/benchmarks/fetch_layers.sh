#!/usr/bin/env bash
# Extracts the layer tarballs of real Docker images into testdata/layers/<image>/ for the dedup benchmark.
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
out="$root/testdata/layers"
images=(ubuntu:20.04 ubuntu:22.04 nginx:latest node:18-slim python:3.11-slim)

for img in "${images[@]}"; do
	name=${img//[:\/]/_}
	dir="$out/$name"
	rm -rf "$dir"
	mkdir -p "$dir"
	tmp=$(mktemp -d)
	docker pull -q "$img" > /dev/null
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
