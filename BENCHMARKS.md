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

---

## 2. RF=2 availability during worker failure

### Workload
Small single-chunk objects of 64 KiB of random bytes on a 4-worker cluster, then workers are killed (the server is closed, so connections are refused) and **every object is downloaded again**. A download counts as *served* only if the body's SHA-256 equals what was uploaded; a 200 with the wrong bytes would be counted as *corrupt*. Two replication modes: RF=2 (normal) and RF=1 (`Server.UseSingleReplica()`, only the primary stores the chunk). Run at the spec's 200 objects and at 2000 for tighter statistics.

### Results

| Scenario | Objects | Served correctly | Unavailable | Corrupt bytes | Status of unavailable |
|---|---|---|---|---|---|
| **RF=2, 1 worker down** | 200 / 2000 | **100% / 100%** | 0 / 0 | **0** | n/a |
| RF=1, 1 worker down | 200 / 2000 | 77.5% / 77.3% | 45 / 455 | 0 | 503 |
| **RF=2, 2 workers down** | 200 / 2000 | 89.5% / 90.8% | 21 / 184 | **0** | 503 |
| RF=1, 2 workers down | 200 / 2000 | 55.5% / 55.3% | 89 / 895 | 0 | 503 |

| Latency (200 objects, RF=2, 1 worker down) | Before failure | After failure |
|---|---|---|
| P50 | 0.38 ms | 0.40 ms |
| P99 | 0.97 ms | 1.02 ms |
| Objects that had to be served from their secondary | | 48 |

**Detection time.** With real heartbeats (every 2 s) and the health monitor, a killed worker was declared OFFLINE after **5.3, 5.3 and 6.5 seconds** (lease 6 s, monitor tick 1 s).

### What the data says
1. **With RF=2 every object survives one worker failure: 100% served, 0 corrupt, no measurable latency penalty.** About a fifth of the objects (48 of 200, 476 of 2000) had their primary on the dead worker and were served from the secondary.
2. **RF=1 loses exactly the dead worker's share: 22.8% of 2000 objects** (the spec said ~25%). The share is not exactly 1/4 because placement follows the consistent-hash ring, where worker2 owns 22.4% of the hash space (see the ring view). The first 200-object run lost only 16.5%; that was sampling noise (it is 200 draws from a 22% chance), which is why the 2000-object run exists.
3. **RF=2 tolerates one failure, not two, and fails honestly.** With two workers down, the objects whose two copies were *both* on those workers (9.2% of 2000) are unavailable and answer **503**, never wrong bytes. In every run the number of unavailable objects equals exactly the number whose every copy was on a dead worker; the benchmark fails if it does not.
4. **The post-failure latency penalty is nil here because the failure is the cheap kind.** A refused connection fails in microseconds, so falling back to the secondary costs nothing visible. A worker that *hangs* instead of refusing is the case hedged reads exist for (benchmark 4).

### A defect this benchmark found and fixed
For the first ~6 seconds after a worker died (until its lease expires), a download of an object with both copies on dead workers answered **502 Bad Gateway** instead of the spec's **503 Service Unavailable**, because the controller chose the status from the health table, which had not noticed yet. It now classifies by what actually happened: if both replicas failed because the worker could not be reached (connection refused, timeout, or an open circuit breaker), the answer is 503 at once. Workers that answer but hold no valid data still give 502. Before the fix the RF=1 run returned 502 for 455 of 457 unavailable objects; after it, 503 for all.

### Honest limits
* Failure here means "connection refused", the fastest way to fail. Slow failures are measured in benchmark 4.
* A worker is killed once and not restored; recovery is benchmark 5.
* One machine, loopback network, so latencies are lower than a real network. The availability percentages do not depend on that.

### Reproduce
```bash
go test ./test/benchmarks/ -run '^$' -bench 'Availability_' -benchtime=1x
BENCH_OBJECTS=2000 go test ./test/benchmarks/ -run '^$' -bench 'Availability_RF' -benchtime=1x
```

---

## 3. Scrubber: corruption caught before a user reads it

### Workload
50 objects of 2 MiB (100 chunks, 200 copies on 4 workers). **10 distinct chunks** each lose one copy to corruption, chosen with a fixed seed so every run damages the same copies. Two kinds of damage:

* **Footer:** the 4-byte CRC32C footer of the chunk file is overwritten with `0xFF` (the spec's scenario). The data bytes are untouched.
* **Data (bit rot):** one byte inside the chunk's data is inverted.

Each is run without a scrubber and with one scrubber pass on every worker before reading. "Direct reads" fetch every chunk straight from the workers (what a store with no end-to-end check would deliver); "controller" downloads go through MiniDFS, which verifies the SHA-256 of every chunk.

### Results

| | Footer, no scrubber | Footer, scrubber | Data, no scrubber | Data, scrubber |
|---|---|---|---|---|
| Corrupt copies injected | 10 | 10 | 10 | 10 |
| Direct worker reads returning wrong bytes (of 200) | 0 | 0 | **10** | **10** (measured before the pass) |
| **Objects the controller served with wrong bytes** | **0** | **0** | **0** | **0** |
| Objects served correctly (of 50) | 50 | 50 | 50 | 50 |
| SHA-256 mismatches hit by readers (each a wasted fetch + fallback) | 0 | 0 | **6** | **0** |
| Copies quarantined by the scrubber | 0 | **10** | 0 | **10** |
| Corrupt copies still on disk after the run | 10 | **0** | 4 | **0** |
| Slowest scrubber pass | n/a | 34 ms | n/a | 37 ms |

**Scrubber pass duration** (one worker, 1000 chunks of 1 MiB, 10 corrupted, all 10 found):

| Pause between chunks | Time for 1000 chunks | Throughput |
|---|---|---|
| none | **0.57 s** | 1741 chunks/s (~1.7 GiB/s) |
| 10 ms (the default) | **13.6 s** | 73 chunks/s |

### What the data says
1. **The failure the spec worries about, wrong bytes delivered to a user, never happens in either configuration, and the scrubber is not what prevents it.** The controller checks the SHA-256 of every chunk it reads, so it served 0 wrong objects even with no scrubber and real bit rot. Without that check, 10 of the 200 chunk reads from workers would have returned corrupt data silently.
2. **What the scrubber adds is finding damage that no read would ever surface.**
   * **Footer damage is invisible to readers:** the data is fine, so every read succeeds and nothing complains; the corrupt footer stays forever. Only the scrubber finds it (10 of 10).
   * **Bit rot on a copy that is never read stays hidden.** Without a scrubber, 4 of the 10 damaged copies were still on disk after all 50 objects were downloaded: they were the secondary copies, which reads never touch. The other 6 were primaries, hit by a read and healed by read-repair. A damaged secondary is a latent single point of failure: lose the other copy and the data is gone.
   * **It removes the cost of discovering damage on the read path.** Without a scrubber 6 readers each fetched a corrupt 1 MiB chunk, failed the hash check and fell back; with the scrubber 0 did.
3. **The scrubber quarantines but does not restore.** After the pass, 10 chunks had fewer than 2 good copies. Bringing them back to 2 is the reconciler's job (benchmark 5); the two together close the window.
4. **A scrubber pass is cheap but deliberately paced.** CRC32C verification runs at ~1.7 GiB/s on this machine, so 1000 chunks take 0.57 s flat out. The default 10 ms pause between chunks (to avoid competing with reads) turns that into 13.6 s per 1000 chunks, about 73 chunks/s. It is a setting (`SCRUB_PAUSE_MS`), so the trade between background load and time-to-detect is explicit.

### Honest limits
* The spec's expected "silent corrupt data returned: possible" applies only to a system without end-to-end verification. MiniDFS has one, so that column reads 0 everywhere. The direct-read row shows what would have happened without it.
* The scrub passes ran on a quiet cluster with the OS page cache warm; a cold disk is slower.
* Corruption was injected into one copy per chunk. If both copies of a chunk are corrupted before anyone notices, the data is lost, and the system reports it as unavailable instead of returning bad bytes (benchmark 2).

### Reproduce
```bash
go test ./test/benchmarks/ -run '^$' -bench 'Scrubber_' -benchtime=1x
```

## 4. Hedged reads: tail latency when one worker is slow

### Workload
* Real in-process cluster, 4 workers, RF=2, loopback HTTP. `worker1` sits behind a proxy that holds back every chunk read by a chosen delay; writes are not delayed.
* 100 objects of 32 KiB (one chunk each), picked so that `worker1` is the primary. Every object is read 3 times per setting (300 reads), each verified against its SHA-256.
* Hedge threshold 25 ms (the default) versus hedging disabled (`HEDGE_THRESHOLD_MS=0`). Delay on the slow worker: 0, 10, 30, 50, 100 ms.

### Results (300 reads per row, every read of every object slowed)

| Slow-worker delay | Hedging off P50 | Hedging off P99 | Hedging on P50 | Hedging on P99 | Hedges fired | P99 improvement |
|---|---|---|---|---|---|---|
| 0 ms (all fast) | 0.4 ms | 1.0 ms | 0.4 ms | 1.1 ms | **0 of 300** | none needed |
| 10 ms | 11.4 ms | 12.3 ms | 11.4 ms | 12.2 ms | 0 of 300 | none (under threshold) |
| 30 ms | 31.5 ms | 32.4 ms | 26.9 ms | 27.5 ms | 300 of 300 | 1.2x |
| 50 ms | 51.7 ms | 52.4 ms | 27.0 ms | 27.5 ms | 300 of 300 | 1.9x |
| 100 ms | 101.7 ms | 102.5 ms | 27.0 ms | 27.7 ms | 300 of 300 | **3.7x** |

### Threshold sweep: worker slow (100 ms) on a random 10% of reads, the rest fast (500 reads per row)

| Threshold | P50 | P95 | P99 | Hedges fired |
|---|---|---|---|---|
| off | 0.5 ms | 101.4 ms | 101.7 ms | 0 |
| 2 ms | 0.4 ms | 2.7 ms | 3.0 ms | 46 (9.2%) |
| 5 ms | 0.4 ms | 5.9 ms | 6.2 ms | 46 (9.2%) |
| 10 ms | 0.4 ms | 11.0 ms | 11.5 ms | 46 (9.2%) |
| 25 ms (default) | 0.4 ms | 26.5 ms | 27.1 ms | 46 (9.2%) |
| 50 ms | 0.4 ms | 51.8 ms | 52.1 ms | 46 (9.2%) |
| 100 ms | 0.4 ms | 101.4 ms | 101.7 ms | 46 (9.2%) |

### What the data says
1. **A slow worker costs a read at most the threshold plus a fast read.** With hedging on, P99 is 27.5 ms whether the slow worker adds 30 ms or 100 ms; with it off, P99 follows the slow worker (102.5 ms at 100 ms). The benefit grows with how slow the worker is.
2. **Hedging never fires when nothing is slow.** All-fast: 0 hedges in 300 reads, so the unnecessary-hedge rate is 0%, and P50/P99 are the same as with hedging off (the 0.1 ms difference is noise).
3. **It does nothing below the threshold.** At 10 ms the primary answers before the 25 ms timer, no hedge starts, and the result matches hedging off. That is the design: the secondary is not asked unless the primary is late.
4. **When the worker is slow only some of the time, hedging is what removes the tail.** At 10% slow reads, P95 falls from 101 ms to 26.5 ms, and P50 does not move because fast reads never hedge.
5. **Cost: extra load only on the reads that were slow.** 46 of 500 reads (9.2%, matching the 10% injected) fired one extra chunk fetch each. The rest sent none.

### What this does not prove
* **The 25 ms threshold is a margin, not an optimum.** On loopback a normal read takes 0.4 ms, so even a 2 ms threshold hedged only the slow reads (the sweep shows the same 46 hedges at every threshold), and a lower threshold would have cut P99 further here. On a real network the normal P99 is larger and a threshold under it would hedge healthy reads and double the read load. 25 ms is chosen to sit well above a normal chunk read; it should be set from the P99 of real traffic.
* The slowness is injected as a fixed delay on one worker. A worker that is slow because it is overloaded would also slow the extra hedge requests sent to it for other chunks.
* Each object is one 32 KiB chunk, so the numbers are per-chunk. A multi-chunk object hedges each chunk separately.

### A defect this benchmark found and fixed
At 30 ms and above, the first run showed hedges stopping after 5 reads: from the sixth read on, the slow worker was never asked at all. Cause: when the secondary wins, the slow primary's request is cancelled, and the worker client counted that cancellation as a worker failure. Five hedged reads in a row opened the circuit breaker on a healthy but slow worker. A cancelled call is not a failure of the worker and is no longer counted (commit 3942b64), with tests for both cases: cancellations leave the breaker closed, and real errors still open it.

### Reproduce
```bash
go test ./test/benchmarks/ -run '^$' -bench 'Hedge_' -benchtime=1x
```

## 5. Reconciler: time to restore redundancy after a worker comes back empty

### Workload
* Real in-process cluster, 4 workers, RF=2, bbolt metadata, loopback HTTP, real files with fsync.
* Single-chunk objects are uploaded until `worker2` holds exactly N chunks (N = 10, 100, 500). Then `worker2` is "restarted with an empty disk": its chunk files are deleted while it stays online and registered. Every one of those N chunks now has one copy instead of two.
* Without a reconciler: wait 3 s and count the chunks that still have one copy. With it: time one `Reconciler.Pass`, which scans every worker, finds the missing copies and copies each back from the surviving holder (worker to worker). Afterwards every copy is counted on disk and every object is downloaded and checked against its SHA-256.
* 64 KiB chunks for 10/100/500; a fourth run uses 100 chunks of 1 MiB to get a byte rate.

### Results

| Chunks lost | Chunk size | One-copy chunks after 3 s, no reconciler | Pass time | Chunks/s | MiB/s | Two copies after | Objects unreadable |
|---|---|---|---|---|---|---|---|
| 10 | 64 KiB | 10 of 10 | 75 ms | 134 | 8.4 | 10 of 10 | 0 |
| 100 | 64 KiB | 100 of 100 | 405 ms | 247 | 15.4 | 100 of 100 | 0 |
| 500 | 64 KiB | 500 of 500 | 2.01 s | 248 | 15.5 | 500 of 500 | 0 |
| 100 | 1 MiB | 100 of 100 | 856 ms | 117 | 117 | 100 of 100 | 0 |

### What the data says
1. **Without the reconciler nothing heals.** After 3 s all lost chunks still have one copy, and they stay that way: a second failure of the one worker that holds them would lose the data. This is the exposure the reconciler closes.
2. **With it, redundancy is fully restored in one pass.** In every run each lost chunk was copied back (restored count equals lost count), two copies exist on disk, no object is unreadable, and the pass reports 0 lost chunks. A second pass restores nothing.
3. **Time grows linearly with the number of chunks.** 100 and 500 chunks both run at about 248 chunks/s (about 4 ms per chunk). The 10-chunk run looks slower per chunk (134/s) because the fixed cost of scanning all workers and metadata (about 35 ms) is a large share of a 75 ms pass.
4. **For large chunks the rate is bytes, not chunks.** 1 MiB chunks restore at 117 MiB/s, so the 4 ms per 64 KiB chunk is mostly per-chunk overhead (fsync, two HTTP calls, a metadata update), not copying. For a worker that lost 100 GiB of 1 MiB chunks that extrapolates to about 15 minutes (100 GiB at 117 MiB/s), and that is how long the data stays single-copy: the window of exposure is set by this rate.

### Honest limits
* The reconciler restores chunks one at a time inside a pass. The numbers above are that sequential rate; copying several chunks at once would be faster and is the obvious improvement if recovery time matters.
* Everything runs on one machine over loopback, so there is no network transfer cost between workers. A real network at 1 Gbit/s (about 119 MiB/s) would cap the 1 MiB rate at about the same figure; at 64 KiB the per-chunk overhead would be higher with real latency.
* The 3 s wait stands in for "no reconciler running"; the point is that the count does not change, not the length of the wait.
* The pass time includes the scan of every online worker's chunk list, so a cluster with many more chunks on healthy workers would add to the fixed cost.

### Reproduce
```bash
go test ./test/benchmarks/ -run '^$' -bench 'Reconciler_' -benchtime=1x
```
