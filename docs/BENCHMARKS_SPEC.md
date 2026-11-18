# MiniDFS Benchmark Specification

Each benchmark below has a **stated hypothesis**, **methodology**, **honest failure condition**
(the workload where our design does NOT win), and a **definition of done**.

The goal is not to show MiniDFS wins everywhere — it is to show *exactly where it wins and why*,
and to be honest about where it does not.

---

## 1. Dedup Storage Savings: Docker-Layer Workload vs Raw Media

### Hypothesis
MiniDFS's content-addressed dedup produces significant storage savings for workloads with high
chunk reuse (container images, backups, ML artifacts). For workloads with unique content
(raw video, encrypted data) it provides zero savings and adds a small per-chunk overhead.

### Methodology

**Setup:**
- 4-worker cluster, RF=2, 1MB chunks
- Two workload types uploaded to separate cluster instances

**Workload A — Docker-layer simulation (good case):**
Pull real Docker images and extract their layer tarballs:
```bash
docker pull ubuntu:20.04
docker pull ubuntu:22.04
docker pull nginx:latest
docker pull node:18-slim
docker pull python:3.11-slim
docker save <image> | tar -x -C testdata/layers/<image>/
```
Upload each `layer.tar` file to MiniDFS. Ubuntu base layers (~100MB) will be shared
across nginx, node, and python images. Expected chunk overlap: 60-70%.

**Workload B — Raw media (honest bad case):**
Generate 5 files of equal total size using `crypto/rand` — every byte unique,
zero chunk overlap possible.

**Metrics collected per workload:**
- Total bytes uploaded (sum of all file sizes)
- Total bytes written to worker disks (sum of all `.bin` files in `chunks/`)
- Dedup ratio = bytes_uploaded / bytes_on_disk
- Upload latency per file (P50, P99)
- Bloom filter false positive rate (from `upload.bloom` events)

**Baseline for comparison:**
Re-run Workload A with dedup disabled (patch `storeChunk` to always treat chunks as new).
Compare storage used: dedup-enabled vs dedup-disabled.

### Expected results
| Metric | Docker layers (dedup on) | Docker layers (dedup off) | Raw media (dedup on) |
|---|---|---|---|
| Storage ratio | ~3x savings | 1x (baseline) | ~1x (no savings) |
| Upload P99 (image 2+) | Faster (skip worker push) | Slower | Same as baseline |
| Bloom false positive rate | <1% | N/A | <0.1% (all unique) |

### Honest failure condition
Raw media upload with dedup enabled is ~2ms slower per chunk than without dedup
(bloom check + bbolt read). Report this number explicitly.

### Definition of done
- Numbers in `BENCHMARKS.md` table
- Both workloads run, both results reported
- Dedup-disabled baseline included

---

## 2. RF=2 Availability During Worker Failure

### Hypothesis
With RF=2, every chunk has 2 replicas. When one worker goes offline, 100% of objects
remain downloadable. Without replication (RF=1), ~25% of objects become unavailable
(those whose only copy was on the dead worker).

### Methodology

**Setup:**
- Upload 200 objects (small files, 1 chunk each) across 4 workers
- Record which worker holds the primary replica for each object
- Kill one worker (close its HTTP server)
- Attempt to download all 200 objects

**Two runs:**
1. RF=2 (normal MiniDFS) — download all 200, count successes
2. RF=1 (patch placement to skip secondary write) — download all 200, count failures

**Metrics:**
- Success rate (%) after worker failure
- Download latency P99 (does fallback to secondary add latency?)
- Time for system to detect worker as offline (health lease timeout)

### Expected results
| Metric | RF=2 | RF=1 |
|---|---|---|
| Download success rate | 100% | ~75% |
| P99 latency after failure | <50ms (fallback is fast) | N/A for failed objects |

### Honest failure condition
RF=2 means tolerating **one** replica failure. If both replicas are on different
dead workers, the object is lost. Test this explicitly: kill 2 workers simultaneously,
show that objects whose both replicas were on those workers return 503, not corrupted data.
MiniDFS fails honestly.

### Definition of done
- Both RF=2 and RF=1 numbers recorded
- Simultaneous 2-worker failure tested and documented
- Availability numbers in `BENCHMARKS.md`

---

## 3. Scrubber: Silent Corruption Caught Before User Reads

### Hypothesis
Silent bit-rot (CRC mismatch) on a chunk file is detected by the scrubber
*before* a user reads it. Without a scrubber, the user either gets corrupted bytes
(worst case: silent wrong data) or an error only at read time. With the scrubber,
the chunk is quarantined proactively and the user gets clean data from the secondary.

### Methodology

**Setup:**
- Upload 50 objects across 4 workers
- Corrupt the CRC footer of 10 random chunk files (overwrite last 4 bytes with 0xFF)

**Two scenarios:**

Scenario A — No scrubber (baseline):
- Download all 50 objects
- Record: how many return corrupted bytes, how many return errors, how many succeed

Scenario B — With scrubber:
- Run one scrubber pass across all workers
- Download all 50 objects
- Record: how many were quarantined (bad chunks caught), how many succeed via secondary

**Metrics:**
- Corrupted bytes silently returned to user (must be 0 with scrubber)
- Objects successfully served despite corruption
- Time for scrubber to complete one full pass over N chunks

### Expected results
| Metric | Without scrubber | With scrubber |
|---|---|---|
| Silent corrupt data returned | Possible | 0 — never |
| Objects unavailable after corruption | ~25% (primary down) | 0% (served from secondary) |
| User-visible errors | Yes (CRC error on read) | No (quarantined proactively) |

### Why this matters
Silent data corruption returning wrong bytes to users is a correctness violation
worse than unavailability. The scrubber is MiniDFS's guarantee that this cannot happen.

### Definition of done
- Both scenarios run and results recorded
- Zero corrupt bytes returned in scrubber scenario verified
- Pass duration per 1k chunks measured

---

## 4. Hedge Reads: P99 Latency Under Degraded Conditions

### Hypothesis
When one worker is slow (GC pause, cold disk, network hiccup), hedged reads
dramatically reduce P99 download latency. Under normal conditions (all workers equally fast),
hedge adds no overhead to P50 and minimal overhead to P99.

### Methodology

**Setup:**
- Upload 100 objects (1 chunk each, primary on worker1)
- Use `slowProxy` from `test/e2e/chaos_gaps_test.go` to simulate slow worker

**Four runs varying slow-worker delay:**
- 10ms extra latency (within hedge threshold — hedge should NOT fire)
- 30ms extra latency (above 25ms hedge threshold — hedge fires)
- 50ms extra latency
- 100ms extra latency

**Two configurations per run:**
1. Hedge enabled (normal MiniDFS, threshold=25ms)
2. Hedge disabled (patch to never fire secondary)

**Metrics per run:**
- P50 download latency
- P95 download latency
- P99 download latency
- Number of hedge requests fired (from `download.hedge` events)

### Expected results
| Slow-worker delay | P99 hedge enabled | P99 hedge disabled | Hedge fired? |
|---|---|---|---|
| 10ms | ~15ms | ~15ms | No (below threshold) |
| 30ms | ~30ms | ~55ms | Yes |
| 50ms | ~30ms | ~80ms | Yes |
| 100ms | ~35ms | ~130ms | Yes |

### Honest failure condition
Under all-fast-workers condition, hedge occasionally fires a secondary request
unnecessarily (when primary is just above 25ms). This adds load to the secondary worker.
Report the unnecessary hedge rate explicitly.

### Definition of done
- All 8 runs completed (4 delays × 2 configurations)
- Numbers in `BENCHMARKS.md` with clear table
- Hedge threshold explained and justified with data

---

## 5. Reconciler: Time-to-Heal After Worker Failure

### Hypothesis
When a worker returns after failure with missing chunk files, the reconciler
automatically detects and re-replicates the missing replicas. Without the reconciler,
RF permanently drops to 1 for affected chunks until manual intervention.

### Methodology

**Setup:**
- Upload 100 objects, RF=2
- Kill worker1, delete all its chunk files from disk
- Restart worker1 (now empty)
- Trigger reconciler pass

**Metrics:**
- Time from worker restart to RF=2 fully restored (measure via bbolt chunk state)
- Number of replicas restored per reconciler pass
- Comparison: with reconciler (automatic) vs without (RF stays at 1 forever)

**Measure at different scales:**
- 10 chunks to heal
- 100 chunks to heal
- 500 chunks to heal

Report healing throughput: chunks restored per second.

### Expected results
| Chunks to heal | Time to full RF=2 | Chunks/second |
|---|---|---|
| 10 | <1s | ~10/s |
| 100 | ~10s | ~10/s |
| 500 | ~50s | ~10/s |

### Definition of done
- Healing throughput measured at 3 scales
- Without-reconciler baseline documented (RF stays degraded)
- Numbers in `BENCHMARKS.md`

---

## 6. Upload Throughput: Dedup Compounding Benefit

### Hypothesis
For workloads with repeated content (e.g. container image deployments), upload
throughput *improves* over time as more chunks are already stored. Image 1 uploads
at baseline speed. Images 2-10 that share base layers upload significantly faster
because dedup chunks skip the worker push entirely.

### Methodology

**Setup:**
- Simulate 10 sequential "image pushes"
- Image 1: all unique chunks (1 base layer, 3 unique layers) = 10 chunks total
- Images 2-10: 7 shared chunks (base layer, dedup hits) + 3 unique chunks each

**Metrics per image push:**
- Total upload time (ms)
- Chunks pushed to workers vs chunks deduped
- Bytes sent over network to workers

### Expected results
| Image | Upload time | Chunks pushed | Chunks deduped |
|---|---|---|---|
| 1 | 500ms (baseline) | 10 | 0 |
| 2 | 180ms | 3 | 7 |
| 3-10 | ~180ms | 3 | 7 |

Shows: 2.7x faster upload for repeated images, 70% less network traffic to workers.

### Definition of done
- All 10 image uploads measured
- Latency chart data in `BENCHMARKS.md`
- Network bytes saved calculated

---

## 7. Honest Benchmark: Random Media (Where We Don't Win)

### Hypothesis
For workloads with zero content overlap (raw video, encrypted backups), dedup
provides no storage savings and adds ~2ms per-chunk overhead. MiniDFS is not
the right choice for this workload.

### Methodology
- Generate 10 files of 5MB each using `crypto/rand` (every byte unique)
- Upload all 10 to MiniDFS
- Measure: storage ratio (should be ~1.0x), per-chunk overhead vs no-dedup baseline

### Expected results
| Metric | Random media |
|---|---|
| Dedup storage savings | 0% |
| Per-chunk overhead vs no-dedup | +1-3ms (bloom + bbolt read) |
| Verdict | Use a simpler store for this workload |

### Why include this
Showing honest limits is more credible than only showing wins.
A system that claims to be good for everything is trusted less.

### Definition of done
- Numbers reported alongside Docker-layer benchmark
- Explicit statement: "MiniDFS is not recommended for raw media ingest"

---

## 8. Consistent Hashing: Chunk Movement on Scale-Out

### Hypothesis
Adding a 5th worker to a 4-worker cluster moves ~20% of chunks with consistent
hashing vs ~80% with modulo placement. At 4 workers the absolute difference is
modest but the formula scales: at 40 workers the gap is catastrophic for modulo.

### Methodology

**Setup:**
- Place 10,000 chunk IDs on a 4-worker ring (consistent hashing)
- Record each chunk's assigned worker
- Add a 5th worker
- Count how many chunks changed worker assignment

**Run same test with modulo placement:**
- `worker = hash(chunkID) % 4` → `hash(chunkID) % 5`
- Count chunks that moved

**Show formula, not just 4-worker numbers:**
- Consistent hashing: expected ~1/N chunks move = 20% when going 4→5 workers
- Modulo: expected ~(N-1)/N chunks move = 80% when going 4→5 workers
- Project to 10, 20, 40 workers to show trend

### Expected results
| Workers | Consistent hashing chunks moved | Modulo chunks moved |
|---|---|---|
| 4→5 | ~20% | ~80% |
| 9→10 | ~10% | ~90% |
| 19→20 | ~5% | ~95% |

### Honest limitation
At 4 workers the absolute difference (2,000 vs 8,000 chunks) matters less than
at scale. State this explicitly: "The benefit compounds at scale; 4 workers shows
the trend, not the full impact."

### Definition of done
- Both consistent hashing and modulo measured at 4-worker scale
- Formula projected to larger cluster sizes
- Numbers in `BENCHMARKS.md`

---

## Running the Benchmarks

```bash
# All benchmarks
go test ./test/benchmarks/... -bench=. -benchtime=10s -count=3

# Specific benchmark
go test ./test/benchmarks/... -bench=BenchmarkDedup_DockerLayers -v

# With memory stats
go test ./test/benchmarks/... -bench=. -benchmem
```

Results go in `BENCHMARKS.md` with the methodology and workload described for each number.
Raw numbers without context are not acceptable — every number must say what workload produced it.
