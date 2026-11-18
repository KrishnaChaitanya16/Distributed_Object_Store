# The live dashboard

The controller serves a dashboard on **http://localhost:8081** that shows, as it happens, how an upload is chunked, hashed, placed on the hash ring and stored, which workers are alive, and how the system heals itself.

```
docker compose -f deploy/docker-compose.yml up -d --build
open http://localhost:8081
```

For the failure demos add the demo override, which lets the page stop containers and damage chunks:

```
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.demo.yml up -d --build
```

The override mounts the Docker socket into the controller, so use it only on a machine you trust. Without it the controls are hidden and every `/ui/api/demo/*` call is refused.

## The six views

| Tab | What it shows |
|---|---|
| Upload and flow | Drop a file (or press *Generate and upload a test file*). Eight stages light up as real events arrive, with a plain-English caption per step. *Narrated* slows steps to about half a second for presenting; *Live* shows real speed. *Upload the same file again* demonstrates deduplication. |
| Hash ring | The ring with one lane per worker and each of the 50 virtual nodes at its real position. Type text or click a chunk tick to see the clockwise walk that picks the primary and secondary worker. |
| Workers | Topology with heartbeat pulses, a card per worker (heartbeat age against the 6 s lease, scrubber status, the chunks really on its disk). A dead worker shows a broken link. |
| Objects and chunks | Object status (STAGED, COMMITTED, FAILED, DELETED), each chunk's state (PENDING, DURABLE, PENDING_DELETE) and where every copy is. Download, delete and add custom metadata. |
| Bloom filter | The real bit array, how full it is, the counters (definitely new, true and false positives) and a lookup that shows the exact bits a chunk id maps to. |
| Failure and healing | Replication health per chunk, the step-by-step story of a worker failure and of a damaged copy rebuilt from the real event log, and the demo controls. |

The rail on the right is the live event log: every step the controller takes, filterable by category.

## Suggested demo

1. Upload a 5 MiB test file in *Narrated* mode and follow the eight stages.
2. Upload the same file again: every chunk is a duplicate and nothing is sent to the workers.
3. Open *Hash ring*, click a chunk tick, and read the walk.
4. Open *Failure and healing*, stop a worker: watch the dead link, the ring lane turning hollow, the replicas being rebuilt, and the replication grid returning to green.
5. Start the worker again: its stale copies are removed by the reconciler.
6. Damage a copy, press *Scrub now*, then download the object: the scrubber quarantines it, the download is served from the other copy, and the damaged copy is restored.

## How it works

* `GET /ui/api/state` and `/ui/api/objects` return the cluster as the controller sees it, with the contents of every worker's disk read from the workers themselves, so a stale or missing copy is visible even when the records look fine.
* `GET /ui/api/events` and `/ui/api/events/stream` (Server-Sent Events) carry the structured events the upload, download, rebalancer and reconciler emit.
* The page uploads and downloads through the normal REST API (`/api/v1/...` on the same port), so what you watch is the real code path.
* `WEBUI_PORT` changes the port (0 turns the UI off), `DEMO_MODE=true` enables the controls, and `WEBUI_DIR` serves the page from a directory for UI development.
