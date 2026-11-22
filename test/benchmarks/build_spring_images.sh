#!/usr/bin/env bash
# Builds three Spring Boot style service images FROM the same JRE base, each with its own small random "jar".
set -euo pipefail

base=eclipse-temurin:17-jre-jammy
docker image inspect "$base" > /dev/null 2>&1 || docker pull -q "$base" > /dev/null

for svc in user-service order-service payment-service; do
	# a random 2 MiB file stands in for the application jar, so the app layer differs per service and the JRE layers are shared
	printf 'FROM %s\nRUN mkdir -p /app && head -c 2097152 /dev/urandom > /app/%s.jar\nCMD ["java","-jar","/app/%s.jar"]\n' "$base" "$svc" "$svc" | docker build -q -t "minidfs-bench-spring-$svc:1" - > /dev/null
	echo "built minidfs-bench-spring-$svc:1"
done
