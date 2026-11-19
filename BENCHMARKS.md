# MiniDFS Benchmarks

Each result says what workload produced it. The method and the questions come from [docs/BENCHMARKS_SPEC.md](docs/BENCHMARKS_SPEC.md); the code is in [test/benchmarks](test/benchmarks).

**Environment for every number below:** AMD Ryzen AI 5 340 (12 threads), NVMe SSD, Go 1.27, Linux. The cluster is real but in one process on one machine: a controller with bbolt, 4 workers that write chunk files with fsync, replication factor 2, 1 MiB chunks, and chunks travelling over loopback HTTP. Absolute throughput is therefore bound by one disk and one CPU; the **ratios and counts** are what carry over to a real cluster.

---

## 1. Dedup storage savings: Docker layers vs random media

### Workloads

* **A, Docker layers.** The layer tarballs of five real images, uploaded in this order: `ubuntu:20.04`, `ubuntu:22.04`, `nginx:latest`, `node:18-slim`, `python:3.11-slim` (18 layer files, 620 MiB, 633 chunks). Extracted by `test/benchmarks/fetch_layers.sh` from `docker save`.
* **B, random media.** 5 files of 124 MiB from `crypto/rand` (620 MiB, 625 chunks), so no two chunks can be alike.
* **Baseline, dedup off.** The same uploads through the same code with `Server.DisableDedup()`: every chunk is written to its workers again and the Bloom filter is skipped.

"Storage ratio" is what a replicated store *without* dedup would write (`uploaded x 2`) divided by what is actually on the workers' disks.

### Results

| Metric | A: Docker layers, dedup on | A: dedup off | B: random media, dedup on | B: dedup off |
|---|---|---|---|---|
| Uploaded | 620 MiB | 620 MiB | 620 MiB | 620 MiB |
| Chunks / chunks deduplicated | 633 / **78** | 633 / 0 | 625 / 0 | 625 / 0 |
| Bytes sent to workers | **1085 MiB** | 1240 MiB | 1240 MiB | 1240 MiB |
| Bytes on worker disks | **1085 MiB** | 1085 MiB * | 1240 MiB | 1240 MiB |
| **Storage ratio** | **1.14x** | n/a (see below) | **1.00x** | 1.00x |
| Upload time per chunk | **21.3 ms** | 25.9 ms | 25.0 / 26.2 / 25.2 ms (3 runs) | 26.0 / 26.0 / 26.1 ms (3 runs) |
| Bloom filter false positives | 0 of 633 | n/a | 0 of 625 | n/a |

\* With content-addressed chunks, writing an identical chunk again replaces the same file, so "dedup off" does not use more disk here. What dedup saves is **network and worker writes** (row above), and the storage ratio is measured against the no-dedup size.

### Per image (Docker layers)

| Image | Size | Chunks written / deduplicated | Sent to workers, dedup on / off | Upload time, dedup on / off | Throughput on / off |
|---|---|---|---|---|---|
| ubuntu:20.04 | 71.7 MiB | 72 / 0 | 143 / 143 MiB | 1.71 s / 1.95 s | 41.8 / 36.7 MiB/s |
| ubuntu:22.04 | 77.2 MiB | 78 / 0 | 154 / 154 MiB | 1.82 s / 2.46 s | 42.5 / 31.4 MiB/s |
| nginx:latest | 159.2 MiB | 165 / 0 | 318 / 318 MiB | 4.00 s / 3.89 s | 39.8 / 41.0 MiB/s |
| node:18-slim | 188.2 MiB | 192 / 0 | 377 / 377 MiB | 4.42 s / 4.88 s | 42.6 / 38.6 MiB/s |
| **python:3.11-slim** | 123.7 MiB | **48 / 78** | **92 / 247 MiB (-63%)** | **1.53 s / 3.20 s (2.1x faster)** | **80.6 / 38.7 MiB/s** |

### What the data says

1. **The spec's ~3x expectation was not met on these images: the measured saving is 1.14x (78 of 633 chunks, 12.5% fewer bytes on disk).** The spec assumed an Ubuntu base layer shared by nginx, node and python. In reality `nginx:latest` and `python:3.11-slim` share one 77.5 MiB **Debian** layer (`37fb86c2a767`), `node:18-slim` has a different Debian layer, and the two Ubuntu images share nothing. Only the one overlap shows up, which is exactly the 78 deduplicated chunks. The finding is about the workload, not about the dedup code.
2. **Where there is overlap, the benefit is real and large.** The python image found its 78 shared chunks and sent 63% less data to the workers and uploaded 2.1x faster than the same upload with dedup off.
3. **Random media gets no savings (1.00x) and dedup costs nothing measurable.** Per-chunk upload time with dedup on (25.0 to 26.2 ms) overlaps dedup off (26.0 to 26.1 ms): the difference is inside run-to-run noise. The spec's expected "+2 ms per chunk" is **not reproduced**.
4. **Why the check is so cheap.** Measured in isolation (`BenchmarkDedupCheck_*`):

   | Step | Cost |
   |---|---|
   | Bloom filter lookup of a new chunk | 45 ns |
   | Bloom filter insert | 93 ns |
   | bbolt lookup that confirms a "maybe" | 2.0 us |
   | bbolt transaction that records the chunk reference (**both modes pay it**) | 1.67 ms |

   The ~2 ms the spec expected is the cost of recording the chunk reference, which an upload needs whether or not dedup is on, so it is not dedup overhead.
5. **Bloom false-positive rate: 0 of 633 questions** (0 of 625 for random media). The filter is sized for 1 M chunks at 1%, and these runs fill about 0.05% of it, so the expected rate at this fill is about 1e-24. "Below 1%" holds trivially; this run says nothing about the filter near capacity.

### Follow-up: do images that really share a base reach the expected savings?

A reviewer pointed out that the first result came from a poor workload and suggested three Python versions (`python:3.11-slim`, `3.10-slim`, `3.9-slim`) on the assumption that they "all share the same Debian base layer". That assumption was **checked before running anything, and it is false**: the three images have no layer in common (every layer ID and every layer SHA-256 differs), because they were built on different dates (2025-10-31, 2026-10-06, 2026-11-05) from different Debian snapshots. The measurement was run anyway, together with a workload that does share a base by construction.

**Workload C, Python versions** (`BenchmarkDedup_PythonVersions`): the layers of 3.11, 3.10 and 3.9 slim, in that order (365 MiB, 372 chunks).

**Workload D, shared base, constructed** (`BenchmarkDedup_SharedBase`): three application images built `FROM python:3.11-slim`, each adding one layer with `pip install` of `requests`, `flask` and `numpy` (481 MiB, 488 chunks). This is how many images in a real registry relate to each other; it is built by `build_app_images.sh`, not pulled.

| Metric | C: Python versions, dedup on | C: dedup off | D: shared base, dedup on | D: dedup off |
|---|---|---|---|---|
| Chunks deduplicated | **54 of 372 (14.5%)** | 0 | **252 of 488 (51.6%)** | 0 |
| Bytes sent to workers | 623 MiB | 731 MiB | **467 MiB** | 961 MiB |
| **Storage ratio** | **1.17x** | n/a | **2.06x** | n/a |
| Upload time per chunk | 22.2 ms | 25.7 ms | **13.6 ms** | 26.7 ms |
| Bloom false positives | 0 of 372 | n/a | 0 of 488 | n/a |

| Image (dedup on / off) | Chunks written / deduplicated | Sent to workers | Upload time |
|---|---|---|---|
| C: python:3.11-slim | 126 / 0 | 247 / 247 MiB | 3.04 s / 3.21 s |
| C: python:3.10-slim | 71 / **52** | **138** / 242 MiB (-43%) | **2.04 s** / 3.14 s (1.5x faster) |
| C: python:3.9-slim | 121 / 2 | 237 / 241 MiB | 3.18 s / 3.23 s |
| D: app-requests (first) | 140 / 0 | 275 / 275 MiB | 3.34 s / 3.62 s |
| D: app-flask | 16 / **126** | **31** / 279 MiB (-89%) | **0.92 s** / 3.77 s (**4.1x faster**) |
| D: app-numpy | 80 / **126** | **160** / 407 MiB (-61%) | **2.38 s** / 5.62 s (2.4x faster) |

What this shows:

1. **No whole layer is shared between the Python versions, yet 14.5% of their chunks still deduplicate.** The 3.10 and 3.11 base layers are different layers but come from two nearby Debian snapshots, so their tar archives contain mostly the same files in the same order. 52 chunks line up at the same positions in both and hash identically. The 3.9 base is an older Debian and only 2 chunks line up. This is a real effect of fixed 1 MiB chunking, but it is fragile: it works only while the content before a chunk is the same length in both files.
2. **The reviewer's expected 60 to 70% savings is not reached by real, independently built images (workload C: 14.5%).** It is reached in the sense that matters only when images truly share their base layers.
3. **With a truly shared base (workload D), dedup removes 51.6% of chunks and uploads of the second and third image are 2.4x to 4.1x faster.** The storage ratio of 2.06x is the **maximum possible for this data**: the three images contain about 233 MiB of unique bytes (the shared 124 MiB base plus three app layers, the largest being numpy's 80 MiB), and 481 MiB / 233 MiB = 2.06. Dedup found every byte that could be found. It cannot reach 3x here because the images are not that similar.
4. **Dedup savings depend on the workload, not only on the code.** Same system, same code: 1.00x on random data, 1.14x on five unrelated images, 1.17x on three Python versions, 2.06x on three images built from one base.

### Honest limits of this benchmark

* **Latency percentiles are weak here.** Only 18 files were uploaded, so P99 is just the largest file and per-file latency mostly measures file size. Per-image throughput and per-chunk time above are the comparable numbers. (`upload_p50_ms` and `upload_p99_ms` are in the JSON output.)
* **Everything runs on one machine.** Network cost is loopback and all four workers share one disk, so the "sent to workers" saving would be worth more on a real network.
* **MiniDFS is not recommended for raw media ingest.** For data with no repeated content it stores 2 copies like any replicated store and has the extra metadata work; it neither helps nor measurably hurts.
* **Fixed 1 MiB chunking only finds whole-chunk matches.** Two files that share content but shift it by a few bytes (an inserted header, for example) do not deduplicate.

### Reproduce

```bash
test/benchmarks/fetch_layers.sh                       # once: extracts the layers into testdata/layers (git-ignored)
go test ./test/benchmarks/ -run '^$' -bench 'Dedup_' -benchtime=1x          # the four workload runs
go test ./test/benchmarks/ -run '^$' -bench 'PythonVersions|SharedBase' -benchtime=1x   # the follow-up workloads
test/benchmarks/build_app_images.sh                   # once: builds the three FROM-one-base images used by SharedBase
test/benchmarks/fetch_layers.sh minidfs-bench-app-requests:1 minidfs-bench-app-flask:1 minidfs-bench-app-numpy:1
go test ./test/benchmarks/ -run '^$' -bench 'DedupCheck_' -benchmem         # cost of each step of the check
go test ./test/benchmarks/ -run '^$' -bench 'RawMedia' -benchtime=1x -count=3
BENCH_OUT=/some/dir go test ... -bench 'Dedup_'       # also writes the full per-file and per-image JSON
```

The workload benchmarks skip with a clear message when the layers have not been fetched.
