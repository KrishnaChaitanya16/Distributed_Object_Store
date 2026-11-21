# MiniDFS Benchmarks

Every result says what workload produced it. The questions come from [docs/BENCHMARKS_SPEC.md](docs/BENCHMARKS_SPEC.md); the code is in [test/benchmarks](test/benchmarks) and the tables below are generated from its JSON output by [test/benchmarks/report.py](test/benchmarks/report.py), so nothing was copied by hand.

Machine: AMD Ryzen AI 5 340 w/ Radeon 840M (12 threads), kernel 7.0.0-43-generic, go1.27.4, commit 4c5725f, run on 2026-11-21; 1 warm-up and 7 measured repetitions per benchmark.

## How every number was produced

All eight benchmarks use the same protocol, on the same commit and the same machine, in one run.

* **System under test.** A real cluster in one process: a controller with bbolt, 4 workers that write chunk files with fsync, replication factor 2, 1 MiB chunks, chunks travelling over loopback HTTP. Absolute throughput is therefore bound by one disk and one CPU (other idle containers were also running on the machine); the **ratios and counts** carry over to a real cluster, the absolute milliseconds do not.
* **Repetitions.** One warm-up repetition, discarded, then 7 measured repetitions, each on a freshly built cluster. Where two configurations are compared (dedup on and off, hedging on and off) they alternate which goes first in each repetition.
* **Latency is reported as P50, P95, P99 and max, never as an average.** The samples from all 7 repetitions are pooled, and the sample count `n` is printed next to each table. With `n` below about 100, P99 is the largest or second-largest sample, so it is a tail indicator, not a stable estimate; the per-repetition P99 spread is shown where it matters (benchmark 4).
* **Every other measurement** (bytes, counts, durations that occur once per repetition) is the median of the 7 repetitions with the min to max range in brackets. A number without a range was identical in all 7.
* **Comparisons** are the ratio of medians with a bootstrap 95% confidence interval (2000 resamples, fixed seed). If the interval contains 1.00, the two configurations are **not distinguishable** in this data, and the text says so.
* **Closed loop.** One request is in flight at a time per client. These numbers show latency without queueing and do not include a throughput-versus-load curve; the effect of many concurrent clients was not measured.
* **Benchmark 8 is pure computation.** A repetition there is a different set of 10,000 chunk ids, not a re-run of the same one, and there is no warm-up.
* Benchmarks 2 and 3 enforce two invariants in every repetition and stop if either breaks: no object is ever served with wrong bytes, and (benchmark 2) exactly the objects whose copies were all on dead workers are the unavailable ones.

### Headline results

| # | Question | Result |
|---|---|---|
| 1 | Does dedup save storage and time? | Depends on content: 1.00x random media, 1.14x unrelated images, 2.06x images on a shared base. Upload time 1.56x faster on the shared base (CI 1.48-1.68), 1.10x on unrelated images (CI 1.05-1.21), not distinguishable on random media |
| 2 | Does RF=2 keep data available? | One worker down: 200 of 200 served in all 7 repetitions, 0 wrong, no read slowdown. Two down: 9.5% unavailable, all 503. RF=1 one down: 22.0% unavailable |
| 3 | What does the scrubber add? | 0 wrong bytes with or without it (the controller verifies SHA-256). It finds the 10 of 10 damaged copies that reads never see; without it 4 corrupt copies stay on disk |
| 4 | Do hedged reads cut tail latency? | 100 ms slow worker: P99 28.0 ms with hedging vs 103.5 ms without (3.7x). 0 hedges in 700 reads when nothing is slow |
| 5 | How fast does redundancy come back? | 521 chunks/s (500 x 64 KiB), 288 MiB/s (100 x 1 MiB); no reconciler: 100% of lost copies stay missing |
| 6 | Does dedup compound over repeated pushes? | 10 pushes: 74 MiB sent instead of 200 (2.70x), total time 1.55x shorter (CI 1.39-2.09) |
| 7 | What does dedup cost when it cannot help? | Nothing distinguishable: per-chunk time ratio 1.03 (CI 0.91-1.45) |
| 8 | How many chunks move when a worker is added? | 18.8% of chunks at 4 to 5 workers vs 79.9% with modulo; 2.7% vs 97.4% at 39 to 40 |

Defects found and fixed while benchmarking: the download returned 502 instead of 503 when no replica was reachable (benchmark 2); a cancelled read, which every winning hedge causes, was counted as a worker failure and opened the circuit breaker on a healthy worker (benchmark 4); and the upload handler left a temp file on disk for every upload over 32 MiB, which filled the disk during benchmark 7 (see the end).

---

## 1. Dedup: storage and upload time

### Workload
* **A, Docker layers.** The layer tarballs of five real images, uploaded in this order: `ubuntu:20.04`, `ubuntu:22.04`, `nginx:latest`, `node:18-slim`, `python:3.11-slim` (18 files, 620 MiB, 633 chunks), extracted by `test/benchmarks/fetch_layers.sh`.
* **C, Python versions.** `python:3.11-slim`, `3.10-slim`, `3.9-slim` (12 files, 365 MiB, 372 chunks). They share no whole layer, but some chunks are alike.
* **D, shared base.** Three application images (requests, flask, numpy) built `FROM python:3.11-slim` by `build_app_images.sh`, so they share the base layers exactly (15 files, 481 MiB, 488 chunks).
* **B, random media.** 5 files of 124 MiB from `crypto/rand` (620 MiB, 625 chunks); no two chunks can match.
* **Baseline, dedup off.** The same uploads through the same code with `Server.DisableDedup()`: every chunk is written to its workers again and the Bloom filter is skipped.
* "Upload ms per chunk" is one sample per uploaded file: the time of the whole request divided by its number of chunks (files differ in size, so raw per-file time would mostly measure file size). "Storage ratio" is what a replicated store without dedup would write (`uploaded x 2`) divided by what is on the workers' disks.

### Results
**A. Docker layers** (18 files, 620 MiB, 633 chunks)

| Metric (median over reps, min-max) | Dedup on | Dedup off |
|---|---|---|
| Chunks deduplicated | 78 | 0 |
| Bytes sent to workers (MiB) | 1,085 | 1,240 |
| Bytes on worker disks (MiB) | 1,085 | 1,085 |
| Storage ratio | 1.14 | n/a |
| Bloom false positives (% of checks) | 0.00 | n/a |
| Upload ms per chunk, P50 (n=126 files) | 19.4 | 19.3 |
| Upload ms per chunk, P95 (n=126 files) | 33.2 | 33.2 |
| Upload ms per chunk, P99 (n=126 files) | 63.8 | 58.2 |
| Upload ms per chunk, max (n=126 files) | 140.3 | 95.3 |
| Whole upload, seconds | 13.2 (12.0-13.9) | 14.6 (13.3-15.8) |
| Throughput, MiB/s | 46.8 (44.7-51.6) | 42.4 (39.2-46.6) |

Upload time off / on: 1.10x (95% CI 1.05-1.21)

**C. Python 3.11 / 3.10 / 3.9** (12 files, 365 MiB, 372 chunks)

| Metric (median over reps, min-max) | Dedup on | Dedup off |
|---|---|---|
| Chunks deduplicated | 54 | 0 |
| Bytes sent to workers (MiB) | 623 | 731 |
| Bytes on worker disks (MiB) | 623 | 623 |
| Storage ratio | 1.17 | n/a |
| Bloom false positives (% of checks) | 0.00 | n/a |
| Upload ms per chunk, P50 (n=84 files) | 21.3 | 21.9 |
| Upload ms per chunk, P95 (n=84 files) | 32.7 | 33.5 |
| Upload ms per chunk, P99 (n=84 files) | 48.2 | 66.7 |
| Upload ms per chunk, max (n=84 files) | 48.2 | 66.7 |
| Whole upload, seconds | 8.5 (7.2-9.6) | 9.4 (7.7-10.1) |
| Throughput, MiB/s | 43.2 (38.2-51.0) | 39.0 (36.1-47.7) |

Upload time off / on: 1.11x (95% CI 0.98-1.24)

**D. Three apps on one base image** (15 files, 481 MiB, 488 chunks)

| Metric (median over reps, min-max) | Dedup on | Dedup off |
|---|---|---|
| Chunks deduplicated | 252 | 0 |
| Bytes sent to workers (MiB) | 467 | 961 |
| Bytes on worker disks (MiB) | 467 | 467 |
| Storage ratio | 2.06 | n/a |
| Bloom false positives (% of checks) | 0.00 | n/a |
| Upload ms per chunk, P50 (n=105 files) | 6.8 | 19.1 |
| Upload ms per chunk, P95 (n=105 files) | 25.3 | 29.4 |
| Upload ms per chunk, P99 (n=105 files) | 33.0 | 43.7 |
| Upload ms per chunk, max (n=105 files) | 41.8 | 77.2 |
| Whole upload, seconds | 6.6 (6.3-7.2) | 10.3 (9.9-11.2) |
| Throughput, MiB/s | 73.1 (67.2-76.1) | 46.8 (43.0-48.7) |

Upload time off / on: 1.56x (95% CI 1.48-1.68)

**B. Random media** (5 files, 620 MiB, 625 chunks)

| Metric (median over reps, min-max) | Dedup on | Dedup off |
|---|---|---|
| Chunks deduplicated | 0 | 0 |
| Bytes sent to workers (MiB) | 1,240 | 1,240 |
| Bytes on worker disks (MiB) | 1,240 | 1,240 |
| Storage ratio | 1.00 | n/a |
| Bloom false positives (% of checks) | 0.00 | n/a |
| Upload ms per chunk, P50 (n=35 files) | 20.1 | 21.7 |
| Upload ms per chunk, P95 (n=35 files) | 27.2 | 27.3 |
| Upload ms per chunk, P99 (n=35 files) | 32.1 | 29.2 |
| Upload ms per chunk, max (n=35 files) | 32.1 | 29.2 |
| Whole upload, seconds | 13.1 (12.7-14.2) | 13.8 (12.5-14.4) |
| Throughput, MiB/s | 47.2 (43.6-48.8) | 45.0 (42.9-49.4) |

Upload time off / on: 1.05x (95% CI 0.95-1.08)

**The cost of the dedup check itself** (Go microbenchmark, `-count=7`, median and range over 7 runs of 1 s each):

| Operation | Per call |
|---|---|
| Bloom filter lookup, chunk never seen | 46.1 ns (45.0-46.9) |
| Bloom filter insert after storing a chunk | 93.2 ns (92.7-94.1) |
| bbolt chunk lookup, when the Bloom filter says "maybe" | 2.37 µs (2.30-2.44) |
| bbolt transaction recording a chunk reference (paid by both modes) | 1.64 ms (1.52-2.17) |

### What the data says
1. **Savings depend entirely on what is uploaded, and they are exact.** Storage ratio 1.14x for unrelated images, 1.17x for three Python versions, 2.06x for three images on one base, 1.00x for random media. The counts were identical in all 7 repetitions. 2.06x is the theoretical limit for the shared-base workload (two of the three images are almost entirely duplicates of the first).
2. **Sending less is faster only when a lot is saved.** Shared base: 1.56x less upload time (CI 1.48-1.68), per-chunk P50 6.8 ms against 19.1 ms, throughput 73 against 47 MiB/s. Unrelated images: 1.10x (CI 1.05-1.21), small but distinguishable. Python versions: 1.11x but the interval (0.98-1.24) contains 1.00, so not distinguishable.
3. **When nothing repeats, dedup costs nothing measurable.** Random media: 1.05x (CI 0.95-1.08), P50 per chunk 20.1 against 21.7 ms, 0 Bloom false positives. The per-chunk work dedup adds is a 46 ns Bloom lookup and a 93 ns insert against a 20 ms upload. The spec expected about 2 ms of overhead per chunk; the cost it had in mind is a metadata lookup, which the Bloom filter avoids for new chunks, and the one write a new chunk needs (1.64 ms) is paid with dedup on or off.
4. **Disk use is the same with dedup on or off.** Chunks are content-addressed, so writing an identical chunk again replaces the same file. What dedup saves is network transfer and worker writes (the "bytes sent" row), and the storage ratio is measured against the no-dedup size.

### Honest limits
* **The tail is not clearly better.** For Docker layers the slowest file with dedup on was slower than with it off (max 140.3 against 95.3 ms per chunk, P99 63.8 against 58.2). With 126 files per mode, P99 is the second-largest value, so this may be one slow file; the data cannot tell.
* **Dedup helped less than in the first write-up of this benchmark.** The baseline got faster when replica pushes became parallel (commit 12baf06): off-mode per-chunk P50 is now 19.3 ms, where it was about 26 ms, so the saved network transfer is a smaller share of the total.
* Chunks are cut at fixed 1 MiB boundaries, so one inserted byte shifts every later chunk and dedup finds nothing after it. A content-defined chunker might find more in the Python versions, which share content but no whole layer; that was not tested.
* Layers are uncompressed tar files extracted from `docker save`; compressed layers would likely dedupe much less; that was not tested.

### Reproduce
```bash
test/benchmarks/fetch_layers.sh && test/benchmarks/build_app_images.sh
go test ./test/benchmarks/ -run '^$' -bench 'Dedup_' -benchtime=1x -timeout 5h
go test ./test/benchmarks/ -run '^$' -bench 'DedupCheck_' -count=7
```

## 2. RF=2 availability during worker failure

### Workload
* 4 workers. 200 objects of 64 KiB (one chunk each) per repetition, random content, uploaded and read back once to confirm all are served. Then 1 or 2 workers are killed (their servers go away) and every object is read again, with every body checked against its SHA-256.
* Each configuration is run with RF=2 (the system) and RF=1 (a baseline with `Server.UseSingleReplica()`, one copy per chunk). Random content means different objects land on different workers in each repetition, so the unavailable counts vary and show their range.
* Failure detection: three workers are killed one after another on a cluster with heartbeats and the health monitor running as in production; each kill gives one sample (n=21 over 7 repetitions).

### Results
| Configuration | Objects per rep | Served correctly | Unavailable | Unavailable, all reps pooled | Served corrupt | Expected unavailable |
|---|---|---|---|---|---|---|
| RF=2, 1 worker down | 200 | 200 | 0 | 0.0% | 0 | 0 |
| RF=1, 1 worker down | 200 | 154 (149-166) | 46 (34-51) | 22.0% | 0 | 46 (34-51) |
| RF=2, 2 workers down | 200 | 181 (175-186) | 19 (14-25) | 9.5% | 0 | 19 (14-25) |
| RF=1, 2 workers down | 200 | 114 (102-120) | 86 (80-98) | 44.1% | 0 | 86 (80-98) |

| Configuration | Reads | P50 ms | P95 ms | P99 ms | max ms | n |
|---|---|---|---|---|---|---|
| RF=2, 1 down | before | 0.35 | 0.71 | 0.93 | 3.49 | 1400 |
| RF=2, 1 down | after | 0.34 | 0.72 | 0.94 | 2.03 | 1400 |
| RF=1, 1 down | before | 0.33 | 0.67 | 1.01 | 1.93 | 1400 |
| RF=1, 1 down | after | 0.30 | 0.61 | 0.84 | 1.05 | 1092 |
| RF=2, 2 down | before | 0.32 | 0.66 | 0.91 | 2.44 | 1400 |
| RF=2, 2 down | after | 0.32 | 0.65 | 0.86 | 1.16 | 1267 |
| RF=1, 2 down | before | 0.33 | 0.69 | 0.92 | 2.14 | 1400 |
| RF=1, 2 down | after | 0.30 | 0.61 | 0.79 | 1.13 | 782 |

Status codes of the unavailable objects, RF=2 / 2 down, all reps: {'503': 133}

Failure detection (seconds from kill to OFFLINE, n=21 kills): min 5.29, P50 5.30, P95 6.51, P99 6.51, max 6.51

### What the data says
1. **With RF=2, one worker down loses nothing.** 200 of 200 objects were served in every repetition, none with wrong bytes, and reads after the failure are as fast as before (P99 0.94 against 0.93 ms, n=1400 each).
2. **RF=1 shows what replication buys.** One worker down made 22.0% of objects unavailable (34 to 51 of 200 per repetition), the share of data that worker held.
3. **Two workers down hurts even RF=2, but only for objects that had both copies on them.** 9.5% (14 to 25 of 200). Each of those was reported as 503 (133 of 133 across the repetitions) rather than an error that suggests corruption.
4. **Never a wrong byte.** 0 objects were served with wrong content in any configuration, and in every repetition the unavailable set equalled the set of objects whose copies were all on dead workers. The benchmark stops if either is violated.
5. **Detection takes 5.3 to 6.5 seconds** (P50 5.30 s, n=21), set by the 2 s heartbeat and 6 s lease. Reads do not wait for it: the download path falls back to the secondary at once when a connection fails.

### Honest limits
* The killed server refuses connections immediately, so the fallback is fast. A worker that hangs instead of refusing is the case hedged reads address (benchmark 4).
* 64 KiB objects over loopback; large objects read from a secondary would add transfer time.
* Objects are single chunks; an object of many chunks is lost when any one chunk is.

### A defect this benchmark found and fixed
The download first returned 502 when no replica could be reached, where the spec asks for 503. It now returns 503 when both replicas are unreachable (including a tripped circuit breaker) and 502 only when a reachable worker does not have a valid copy (commit caafd09).

### Reproduce
```bash
go test ./test/benchmarks/ -run '^$' -bench 'Availability_' -benchtime=1x -timeout 5h
```

## 3. Scrubber: corruption caught before a user reads it

### Workload
* 4 workers, RF=2. 50 objects of 2 MiB (100 chunks, 200 copies on disk). 10 copies are damaged, in two ways: **footer damage** overwrites the stored CRC32C with 0xFF bytes and leaves the data intact; **bit rot** flips bytes inside the chunk data.
* Each damage type is run without a scrubber and with one full scrubber pass on every worker before reading. "Direct reads" fetch every copy straight from its worker, which is what a system without end-to-end verification would return.
* The 50 objects are then read through the controller; every body is checked against its SHA-256. The read-latency rows are the 50 reads per repetition (n=350).
* **Pass duration:** one worker, 1000 chunks of 1 MiB, 10 of them corrupted, with no pause between chunks and with the default 10 ms pause.

### Results
| Metric (median over reps, min-max) | footer, no scrubber | footer, scrubber | bit rot, no scrubber | bit rot, scrubber |
|---|---|---|---|---|
| Corrupt copies injected | 10 | 10 | 10 | 10 |
| Direct worker reads returning wrong bytes | 0 | 0 | 10 | 10 |
| Objects the controller served with wrong bytes | 0 | 0 | 0 | 0 |
| Objects unavailable | 0 | 0 | 0 | 0 |
| SHA-256 mismatches hit by readers | 0 | 0 | 6 | 0 |
| Copies quarantined by the scrubber | 0 | 10 | 0 | 10 |
| Corrupt copies still on disk afterwards | 10 | 0 | 4 | 0 |
| Slowest scrubber pass, ms | 0.0 | 82.0 (34.4-91.9) | 0.0 | 42.6 (38.7-169.8) |
| Object read ms, P50 (n=350) | 5.14 | 5.27 | 5.50 | 5.40 |
| Object read ms, P95 (n=350) | 6.62 | 6.77 | 7.83 | 6.91 |
| Object read ms, P99 (n=350) | 7.23 | 7.95 | 8.63 | 7.80 |
| Object read ms, max (n=350) | 7.81 | 9.08 | 9.80 | 10.27 |

| Pause between chunks | Seconds for 1000 x 1 MiB chunks | Chunks/s |
|---|---|---|
| none | 0.30 (0.24-0.56) | 3,340 (1,781-4,162) |
| 10 ms (default) | 11.47 (11.17-11.92) | 87 |

Corrupt chunks found per pass: 10 of 10

### What the data says
1. **The failure the spec worries about, wrong bytes delivered to a user, never happens, and the scrubber is not what prevents it.** The controller checks the SHA-256 of every chunk it reads, so it served 0 wrong objects even with no scrubber. Without that check, 10 direct reads under bit rot would have returned wrong bytes.
2. **What the scrubber adds is finding damage that no read surfaces.** Footer damage leaves the data correct, so every read succeeds and nothing complains: 10 damaged copies stay on disk without a scrubber, 0 with one. Under bit rot, 4 of the 10 damaged copies were still on disk after all reads without a scrubber: they were secondary copies, which reads never touch, so a second failure could have lost the data.
3. **It removes damage from the read path.** Without a scrubber, 6 reads fetched a corrupt chunk, failed the hash check and fell back to the other copy; with one, 0. The cost is visible in the tail but small: P95 7.83 against 6.91 ms and P99 8.63 against 7.80 ms; the maximum is not different (9.80 against 10.27 ms).
4. **The scrubber quarantines, it does not restore.** Ten chunks end with a single good copy; the reconciler (benchmark 5) brings them back to two.
5. **A pass is cheap, and deliberately paced.** Unpaced, 1000 chunks took 0.30 s with a wide range (0.24 to 0.56 s, 1,781 to 4,162 chunks/s); with the default 10 ms pause 11.47 s (87 chunks/s). The pause is a setting (`SCRUB_PAUSE_MS`) that trades time-to-detect against background load.

### Honest limits
* The scrubber ran on a quiet cluster with a warm page cache; a cold disk is slower, and the unpaced range shows how much the first pass can vary.
* Damage was injected into one copy per chunk. If both copies of a chunk are damaged before anyone notices, the data is lost and reported as unavailable instead of being returned wrong (benchmark 2).
* The spec's "silent corrupt data possible" column does not apply here because of the controller-side verification; the direct-read row is what that column would have shown.

### Reproduce
```bash
go test ./test/benchmarks/ -run '^$' -bench 'Scrubber_' -benchtime=1x -timeout 5h
```

## 4. Hedged reads: tail latency when one worker is slow

### Workload
* 4 workers, RF=2, `worker1` behind a proxy that holds back every chunk read by a chosen delay. 100 objects of 32 KiB (one chunk each) chosen so `worker1` is the primary; each is read once per repetition (n=700 reads per cell), every body verified.
* Hedge threshold 25 ms (the default) against hedging disabled (`HEDGE_THRESHOLD_MS=0`), with the slow worker's delay at 0, 10, 30, 50 and 100 ms.
* Threshold sweep: the delay is 100 ms on a random 10% of reads and zero on the rest, 100 objects read twice per repetition (n=1400 per row).

### Results (all in ms)
| Slow-worker delay | Off P50 | Off P95 | Off P99 | Off max | On P50 | On P95 | On P99 | On max | Hedges fired | P99 off / on |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 ms | 0.3 | 0.6 | 1.1 | 1.8 | 0.3 | 0.6 | 1.1 | 2.0 | 0 of 700 | 1.0x |
| 10 ms | 11.9 | 12.7 | 13.2 | 13.7 | 11.9 | 12.7 | 13.1 | 13.6 | 0 of 700 | 1.0x |
| 30 ms | 32.3 | 33.2 | 33.4 | 33.8 | 26.8 | 27.5 | 27.9 | 28.4 | 700 of 700 | 1.2x |
| 50 ms | 52.4 | 53.2 | 53.4 | 53.7 | 26.5 | 27.2 | 27.7 | 28.3 | 700 of 700 | 1.9x |
| 100 ms | 102.4 | 103.2 | 103.5 | 103.7 | 26.9 | 27.6 | 28.0 | 28.2 | 700 of 700 | 3.7x |

(all in ms; n=700 reads per cell; per-repetition P99 spread, hedging on: 0 ms: 1.0 (0.8-1.3), 10 ms: 12.9 (12.7-13.3), 30 ms: 27.8 (27.5-28.0), 50 ms: 27.6 (27.3-28.0), 100 ms: 27.9 (27.5-28.1); hedging off: 0 ms: 0.9 (0.6-1.2), 10 ms: 13.1 (12.7-13.5), 30 ms: 33.4 (33.3-33.5), 50 ms: 53.3 (53.2-53.4), 100 ms: 103.4 (103.3-103.5))

| Threshold | P50 | P95 | P99 | max | Hedges fired | P99 per rep (median, min-max) |
|---|---|---|---|---|---|---|
| off | 0.4 | 102.1 | 102.5 | 102.9 | 0 of 1400 (0.0%) | 102.4 (102.3-102.8) |
| 2 ms | 0.3 | 2.5 | 2.7 | 3.4 | 119 of 1400 (8.5%) | 2.7 (2.6-2.8) |
| 5 ms | 0.4 | 5.7 | 6.0 | 6.3 | 119 of 1400 (8.5%) | 5.9 (5.8-6.1) |
| 10 ms | 0.4 | 11.0 | 12.0 | 12.5 | 119 of 1400 (8.5%) | 11.9 (11.6-12.1) |
| 25 ms | 0.4 | 26.7 | 27.2 | 27.8 | 119 of 1400 (8.5%) | 27.2 (27.0-27.3) |
| 50 ms | 0.4 | 51.5 | 52.2 | 53.0 | 119 of 1400 (8.5%) | 52.1 (51.9-52.3) |
| 100 ms | 0.4 | 101.6 | 102.1 | 102.8 | 119 of 1400 (8.5%) | 102.1 (101.9-102.2) |

### What the data says
1. **A slow worker costs a read at most the threshold plus a fast read.** With hedging, P99 is about 28 ms whether the slow worker adds 30, 50 or 100 ms; without it, P99 follows the delay (103.5 ms at 100 ms). The per-repetition P99 range is tight (27.3 to 28.1 ms with hedging at 30 to 100 ms), so this is not an artifact of pooling.
2. **Hedging never fires when nothing is slow.** All fast: 0 hedges in 700 reads (unnecessary-hedge rate 0%), and P50 to P99 are identical with hedging on and off.
3. **It does nothing below the threshold.** At 10 ms the primary answers before the 25 ms timer and the result matches hedging off. At 30 ms the gain is only 1.2x, because the saving is just the difference between the delay and the threshold.
4. **When the worker is slow only some of the time, hedging removes the tail.** At 10% slow reads P95 falls from 102.1 to 26.7 ms; P50 does not move because fast reads never hedge.
5. **The tail tracks the threshold.** P99 is the threshold plus about 1 to 2 ms at every setting (2.7 ms at 2 ms, 27.2 at 25 ms, 52.2 at 50 ms). A hedge costs one extra chunk fetch, and only for reads that were slow: 119 of 1400 reads (8.5%) at every threshold in the sweep.

### Honest limits
* **The 25 ms threshold is a margin, not an optimum.** On loopback a normal read takes 0.4 ms, so even a 2 ms threshold hedged only the slow reads, and lower is better here. On a real network the normal P99 is higher, a threshold under it would hedge healthy reads and double the read load, and 25 ms should be re-chosen from the P99 of real traffic.
* **The 10% slowness follows a fixed pseudo-random sequence,** the same in every repetition, so those 7 repetitions confirm repeatability, not independent samples of the slow pattern.
* The slowness is a fixed delay on one worker. A worker that is slow because it is overloaded would also slow the extra hedge requests sent to it.
* Each object is one chunk; a multi-chunk object hedges each chunk separately.

### A defect this benchmark found and fixed
In the first run hedges stopped after 5 reads. When the secondary wins, the slow primary's request is cancelled, and the worker client counted that cancellation as a worker failure; five hedged reads in a row opened the circuit breaker on a healthy but slow worker. A cancelled call is no longer counted (commit 3942b64), with tests for both cases.

### Reproduce
```bash
go test ./test/benchmarks/ -run '^$' -bench 'Hedge_' -benchtime=1x -timeout 5h
```

## 5. Reconciler: time to restore redundancy

### Workload
* 4 workers, RF=2. Single-chunk objects are uploaded until `worker2` holds exactly N chunks; then `worker2` is "restarted with an empty disk": its chunk files are deleted while it stays registered and online. Every one of those chunks now has one copy.
* Without a reconciler: wait 1 s and count the chunks that still have one copy. With it: time one `Reconciler.Pass`, which scans every worker and copies each missing chunk back from the surviving holder (worker to worker, 4 copies in flight at once). Then every copy is counted on disk and every object is downloaded and checked.
* N = 10, 100 and 500 chunks of 64 KiB, and 100 chunks of 1 MiB. One pass is one sample per repetition, so the table gives the median and range of 7 pass times; there are too few samples per cell for a percentile.

### Results
| Chunks lost | One copy before | One copy after a 1 s wait, no reconciler | Pass ms | Chunks/s | MiB/s | Two copies after the pass | Objects unreadable |
|---|---|---|---|---|---|---|---|
| 10 x 64 KiB | 10 | 10 | 46 (37-164) | 219 (61-267) | 13.7 (3.8-16.7) | 10 | 0 |
| 100 x 64 KiB | 100 | 100 | 204 (188-263) | 489 (380-533) | 30.6 (23.8-33.3) | 100 | 0 |
| 500 x 64 KiB | 500 | 500 | 960 (750-1,493) | 521 (335-667) | 32.6 (20.9-41.7) | 500 | 0 |
| 100 x 1024 KiB | 100 | 100 | 347 (333-452) | 288 (221-300) | 288.3 (221.0-300.5) | 100 | 0 |

### What the data says
1. **Without the reconciler nothing heals.** After a 1 s wait every lost chunk still has one copy, in every repetition; a second failure of the one worker holding them would lose the data. This is the exposure the reconciler closes.
2. **With it, redundancy is fully restored in one pass.** Every lost chunk was copied back, each has two copies on disk, no object is unreadable, and the pass reports 0 lost chunks, in all 7 repetitions of every row.
3. **Time grows with the number of chunks, roughly linearly.** 100 chunks take 204 ms and 500 take 960 ms (489 and 521 chunks/s). The 10-chunk run is slower per chunk (219/s) and very variable (37 to 164 ms), because the fixed cost of scanning every worker is a large share of a 46 ms pass.
4. **For large chunks the rate is bytes, not chunks.** 1 MiB chunks restore at 288 MiB/s (221 to 300). At 64 KiB the per-chunk overhead (fsync, two HTTP calls, a metadata update) dominates and the rate is 31 to 33 MiB/s. A worker that lost 100 GiB of 1 MiB chunks would take about 6 minutes at 288 MiB/s, and that is how long the data stays at one copy.

### Honest limits
* **This benchmark ran after the reconciler was made parallel (commit afc0e27).** An earlier sequential version measured 248 chunks/s at 500 x 64 KiB and 117 MiB/s at 100 x 1 MiB, once each on the same machine; this version is 2.1x and 2.5x faster. That earlier measurement was a single run and is context, not a controlled comparison.
* Everything runs over loopback on one machine, so there is no network transfer cost between workers; a real network at 1 Gbit/s (about 119 MiB/s) would cap the large-chunk rate well below 288 MiB/s.
* The pass time includes the scan of every online worker's chunk list, so healthy workers holding many chunks add to the fixed cost.
* The 1 s wait stands in for "no reconciler running"; the point is that the count does not change.

### Reproduce
```bash
go test ./test/benchmarks/ -run '^$' -bench 'Reconciler_' -benchtime=1x -timeout 5h
```

## 6. Upload latency: how dedup compounds over repeated image pushes

### Workload
* 4 workers, RF=2. 10 images pushed one after another. Each image is 10 chunks of 1 MiB: the same 7 chunks (a shared base) then 3 chunks unique to that image, all pseudo-random so nothing else repeats.
* Dedup on against dedup off, alternating order, 7 repetitions each. Push 1 has n=7 samples (one per repetition); pushes 2 to 10 pool n=63.

### Results
| Metric | Dedup on | Dedup off |
|---|---|---|
| Push 1 ms (n=7) | P50 236, max 272 | P50 271, max 694 |
| Pushes 2-10 ms, P50 (n=63) | 142 | 249 |
| Pushes 2-10 ms, P95 (n=63) | 207 | 365 |
| Pushes 2-10 ms, P99 (n=63) | 386 | 455 |
| Pushes 2-10 ms, max (n=63) | 386 | 455 |
| MiB sent, first push | 20 | 20 |
| MiB sent, each later push | 6 | 20 |
| MiB sent, all 10 pushes | 74 | 200 |
| Seconds, all 10 pushes | 1.54 (1.17-1.97) | 2.39 (2.30-3.11) |

Total time off / on: 1.55x (95% CI 1.39-2.09); median later push off / on: 1.75x (95% CI 1.58-2.00); bytes off / on: 2.70x

### What the data says
1. **The first push gains nothing.** Both send 20 MiB. The 236 against 271 ms P50 for push 1 comes from only 7 samples and one 694 ms outlier in dedup-off, so it is noise, not a gain.
2. **Every later push sends 6 MiB instead of 20 MiB** (3 unique chunks x 2 replicas) and is faster: P50 142 against 249 ms, P95 207 against 365 ms, and the median later push is 1.75x faster (CI 1.58-2.00).
3. **The gain compounds with the number of pushes.** After 10 pushes 74 MiB was sent instead of 200 (2.70x) and the total time was 1.55x shorter (CI 1.39-2.09). Three of ten chunks are always new, so the bytes ratio tends to 3.33x for longer series.
4. **Time falls less than bytes.** A push with 70% of its chunks already stored still takes 142 ms, not 30% of 249 ms: for every deduplicated chunk the controller still checks the Bloom filter, looks the chunk up in bbolt and records the reference, and the object's metadata is committed regardless.

### Honest limits
* Constructed workload with an assumed 70% shared share, matching the spec's 7 shared and 3 unique. Real images share what benchmark 1 measured.
* P99 over 63 samples is the maximum, so it is only a tail indicator (386 against 455 ms).
* Disk use is the same with dedup on or off (chunks are content-addressed); dedup saves transfer, worker writes and time.
* Loopback transfers are fast; on a real network the 14 MiB saved per push would count for more.

### Reproduce
```bash
go test ./test/benchmarks/ -run '^$' -bench 'Compounding_' -benchtime=1x -timeout 5h
```

## 7. Random media: what dedup costs when it cannot help

### Workload
* 4 workers, RF=2. 10 files of 5 MiB from `crypto/rand` (50 chunks, 100 MiB), uploaded back to back; no two chunks can match.
* Dedup on against off, alternating order, 7 repetitions each. n=70 file uploads per mode.

### Results
| Metric | Dedup on | Dedup off |
|---|---|---|
| Upload ms per 5 MiB file, P50 (n=70) | 105.8 | 103.9 |
| Upload ms per 5 MiB file, P95 (n=70) | 152.0 | 202.5 |
| Upload ms per 5 MiB file, P99 (n=70) | 295.3 | 584.7 |
| Upload ms per 5 MiB file, max (n=70) | 295.3 | 584.7 |
| Upload ms per chunk (median over reps, min-max) | 21.5 (20.4-25.9) | 22.2 (20.4-31.5) |
| Stored (MiB) | 100 | 100 |
| Storage ratio | 1.00 | 1.00 |

Per-chunk time off / on: 1.03x (95% CI 0.91-1.45)

### What the data says
1. **No storage is saved and none was promised: the ratio is 1.00x** with both modes.
2. **The dedup path costs nothing distinguishable.** Median per-chunk time 21.5 ms against 22.2 ms, ratio 1.03 with a 95% interval of 0.91 to 1.45 that contains 1.00; P50 per file 105.8 against 103.9 ms. The extra work is a 46 ns Bloom lookup and a 93 ns insert per chunk (benchmark 1).
3. **The spec's expected overhead of about 2 ms per chunk is not what this system shows.** Bloom filter answers "no" for new chunks, so no metadata lookup happens; the reference write a new chunk needs is paid in both modes.

### Honest limits
* The interval is wide at the top (up to 1.45) because a few uploads were slow outliers in both modes; a real difference below about 10% would not show in 7 repetitions.
* P99 per file (295 against 585 ms) is the second-largest of 70 samples and is dominated by those outliers; it does not show a systematic difference.
* Both modes write each chunk's reference to bbolt, so that cost (1.64 ms) is in both columns and is not part of the difference.

### Reproduce
```bash
go test ./test/benchmarks/ -run '^$' -bench 'RandomMedia_' -benchtime=1x -timeout 5h
```

## 8. Consistent hashing: how many chunks move when a worker is added

### Workload
* 7 sets of 10,000 chunk ids each (SHA-256 of a salted counter, as lowercase hex), placed with the real `placement.Ring` (50 virtual nodes per worker). Pure computation, no cluster; the 7 repetitions are the 7 id sets.
* Baseline: modulo placement, `owner = first 8 bytes of the chunk hash mod worker count`.
* For each cluster size, the share of chunks whose **primary** worker changes when one worker is added (4 to 5, 9 to 10, 19 to 20, 39 to 40), and how evenly the primaries spread over the workers.

### Results
| Workers | Ideal | Consistent hashing (primary moved) | Modulo (primary moved) | Ring: primary or secondary changed |
|---|---|---|---|---|
| 4 to 5 | 20.0% | 18.8 (18.5-19.0)% | 79.9 (79.7-80.9)% | 44.4 (44.0-45.0)% |
| 9 to 10 | 10.0% | 10.7 (10.4-11.3)% | 89.9 (89.4-90.2)% | 19.9 (19.5-20.4)% |
| 19 to 20 | 5.0% | 5.9 (5.8-6.0)% | 94.9 (94.7-95.4)% | 10.7 (10.5-11.3)% |
| 39 to 40 | 2.5% | 2.7 (2.5-3.0)% | 97.4 (97.3-97.6)% | 5.6 (5.3-5.8)% |

| Workers | Fair share | Smallest share | Largest share | Spread (CV) |
|---|---|---|---|---|
| 4 | 25.0% | 22.4 (21.9-22.8)% | 29.1 (28.6-29.3)% | 10.4 (10.2-12.0)% |
| 10 | 10.0% | 7.8 (7.6-8.0)% | 12.6 (12.4-12.8)% | 17.2 (15.9-18.6)% |
| 20 | 5.0% | 3.7 (3.6-4.0)% | 6.3 (6.2-6.5)% | 14.3 (13.3-15.6)% |

### What the data says
1. **Adding a worker moves close to the minimum possible.** Consistent hashing moves within 1.2 points of the ideal 1/(N+1) at every size (18.8% against 20.0%, 10.7% against 10.0%, 5.9% against 5.0%, 2.7% against 2.5%), and only onto the new worker (checked by a test). Modulo moves 80% to 97%, so adding one machine to a 20-worker cluster would mean re-copying nearly every chunk.
2. **The gap widens with scale.** The moved share falls as the cluster grows while modulo's rises towards 100%.
3. **The result is stable.** The range over 7 different id sets is under 1.5 points at every size.

### Where it is less good than it looks
* **Balance is uneven.** With 50 virtual nodes, a worker's share of primaries ranges from 22.4% to 29.1% at 4 workers (fair share 25%), 7.8% to 12.6% at 10 and 3.7% to 6.3% at 20; the busiest worker holds up to 1.26x its fair share. More virtual nodes would even this out; the number is fixed at 50 in the code and was not varied here.
* **RF=2 moves more than the primary alone.** Counting chunks whose primary or secondary changed, 44.4% at 4 to 5, 19.9% at 9 to 10, 10.7% at 19 to 20 and 5.6% at 39 to 40, roughly double the primary-only figure.
* Chunk ids here are hashes of generated names, so they are uniformly distributed by construction; real chunk ids are hashes of content and are too.

### Reproduce
```bash
go test ./test/benchmarks/ -run '^$' -bench 'Ring_' -benchtime=1x
```

---

## Running everything

```bash
BENCH_OUT=out BENCH_COMMIT=$(git rev-parse --short HEAD) go test ./test/benchmarks/ -run '^$' \
  -bench 'Dedup_|Availability_|Scrubber_|Hedge_|Reconciler_|Compounding_|RandomMedia_|Ring_' -benchtime=1x -timeout 5h
python3 test/benchmarks/report.py out
```

The full run takes about 36 minutes and needs about 3 GB of scratch disk. `BENCH_REPS` changes the number of measured repetitions (default 7), and `BENCH_OBJECTS` the number of objects in the availability runs (default 200).

## A defect that nearly stopped the benchmarks

The first complete run filled the disk. The upload handler works on a copy of the request (`r.WithContext`), so net/http's own cleanup never saw the parsed multipart form, and every uploaded file over 32 MiB left a temp file in the controller's temp directory for good. Uploading the 124 MiB media files and the Docker layers used up the roughly 21 GB of free space in one run, and the random-media benchmark then failed with a worker write error. The handler now removes the form's temp files when the request ends (commit 405d8f0), with tests for both the success and the rejected-upload path, and the full run above shows no disk growth.
