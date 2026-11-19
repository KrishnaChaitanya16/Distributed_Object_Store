#!/usr/bin/env bash
# Builds three small application images FROM the same base, the way many images in a registry share a base layer.
set -euo pipefail

base=python:3.11-slim
docker image inspect "$base" > /dev/null 2>&1 || docker pull -q "$base" > /dev/null

for spec in requests:requests flask:flask numpy:numpy; do
	name=${spec%%:*}
	pkg=${spec##*:}
	printf 'FROM %s\nRUN pip install --no-cache-dir %s\n' "$base" "$pkg" | docker build -q -t "minidfs-bench-app-$name:1" - > /dev/null
	echo "built minidfs-bench-app-$name:1"
done
