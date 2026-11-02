# MiniDFS — Self-Healing (P2P Replication + Worker Restart Reconciliation)

```mermaid
sequenceDiagram
  participant W2 as Worker 2 failing
  participant LT as Lease Table
  participant CTL as Controller
  participant DB as bbolt
  participant WS as Surviving Worker
  participant WN as Replacement Worker

  loop Every 2 seconds
    W2--xLT: Heartbeat stops — docker stop worker2
  end

  LT->>LT: 3 missed heartbeats — 6 seconds elapsed
  LT->>CTL: Declare worker2 OFFLINE
  CTL->>CTL: Remove worker2 vnodes from hash ring

  CTL->>DB: Scan all chunks — replica_count less than 2
  DB-->>CTL: List of DEGRADED chunks

  loop For each degraded chunk
    CTL->>CTL: Select replacement via ring — next healthy vnode
    CTL->>WS: POST /replicate-peer {chunk_id, target: WN.addr}
    Note over WS,WN: P2P direct stream — controller not in data path
    WS->>WN: PUT /internal/chunks/{chunk_id}
    WN-->>WS: 201 Created
    WS-->>CTL: 202 Accepted
    CTL->>DB: Update replica list — replica_count back to 2
  end

  Note over CTL: Target under 3s per chunk per NFR-3

  Note over W2,CTL: Later — docker start worker2
  W2->>CTL: POST /internal/workers/register
  CTL->>CTL: Re-add worker2 vnodes to hash ring
  CTL->>W2: GET /internal/workers/worker2/chunks
  W2-->>CTL: Current chunk inventory
  CTL->>CTL: Reconcile — skip already-healed, repair still-degraded only
```
