# KubeRoute (MVP)

KubeRoute is a Go gRPC gateway that exposes a **subset** of etcd KV API and routes requests by key policy with explicit consistency modes.

## What this MVP implements

- etcd KV gRPC methods:
  - `Put`
  - `Range`
  - `DeleteRange`
- Prefix policy modes:
  - `/linearizable/*` -> direct primary etcd
  - `/bounded/*` -> cache-first point reads with max staleness
  - `/eventual/*` -> sync primary writes + durable async replication to secondary
- Durable replication queue (BoltDB), at-least-once worker retry
- Cache staleness metadata on reads:
  - `x-kuberoute-mode`
  - `x-kuberoute-source` (`cache` or `primary`)
  - `x-kuberoute-staleness-ms`
  - `x-kuberoute-replication` (`n/a`, `queued`, `degraded`)

## Explicit limitations (MVP honesty)

- Not a drop-in etcd replacement.
- Not full etcd API compatibility.
- `Txn` and `Compact` return `Unimplemented`.
- etcd maintenance RPCs (for example `etcdctl endpoint health`) are not implemented by this MVP gateway.
- No exactly-once replication guarantee; replication is at-least-once.
- Revisions across primary/secondary are not guaranteed to match.
- In `/eventual/*`, primary write success is returned even if replication enqueue is degraded; check `x-kuberoute-replication`.

## Consistency behavior matrix

| Key prefix | Write path | Read path | Consistency |
|---|---|---|---|
| `/linearizable/` | Primary only | Primary | Linearizable from primary |
| `/bounded/` | Primary | Cache first if fresh, else primary | Bounded-stale (max staleness) |
| `/eventual/` | Primary + enqueue replication | Cache first if fresh, else primary | Eventual on secondary |

## Quickstart

1. Start two local etcd instances (different **client ports**, **peer ports**, and **data dirs**):

```bash
brew install etcd
etcd \
  --name etcd1 \
  --data-dir /tmp/etcd1 \
  --listen-client-urls=http://127.0.0.1:2379 \
  --advertise-client-urls=http://127.0.0.1:2379 \
  --listen-peer-urls=http://127.0.0.1:2380 \
  --initial-advertise-peer-urls=http://127.0.0.1:2380 \
  --initial-cluster=etcd1=http://127.0.0.1:2380
```

```bash
etcd \
  --name etcd2 \
  --data-dir /tmp/etcd2 \
  --listen-client-urls=http://127.0.0.1:3379 \
  --advertise-client-urls=http://127.0.0.1:3379 \
  --listen-peer-urls=http://127.0.0.1:3380 \
  --initial-advertise-peer-urls=http://127.0.0.1:3380 \
  --initial-cluster=etcd2=http://127.0.0.1:3380
```

2. Run KubeRoute:

```bash
go run ./cmd/kuberoute \
  --listen=:22379 \
  --primary-endpoint=127.0.0.1:2379 \
  --secondary-endpoint=127.0.0.1:3379 \
  --replication-artificial-delay=200ms
```

3. Issue etcdctl requests against KubeRoute:

```bash
etcdctl --endpoints=http://127.0.0.1:22379 --dial-timeout=3s --command-timeout=5s put /eventual/demo hello
etcdctl --endpoints=http://127.0.0.1:22379 --dial-timeout=3s --command-timeout=5s get /eventual/demo
etcdctl --endpoints=http://127.0.0.1:22379 --dial-timeout=3s --command-timeout=5s put /bounded/demo world
etcdctl --endpoints=http://127.0.0.1:22379 --dial-timeout=3s --command-timeout=5s get /bounded/demo
```

4. Verify placement behavior:

```bash
# /bounded/* writes go to primary (2379)
etcdctl --endpoints=http://127.0.0.1:2379 get /bounded/demo

# /eventual/* writes are replicated to secondary (3379)
etcdctl --endpoints=http://127.0.0.1:3379 get /eventual/demo
```

Expected:

- `/bounded/demo` is present on `2379`.
- `/eventual/demo` eventually appears on `3379` (after replication delay, if configured).

Notes:

- `etcdctl endpoint health` is expected to fail against KubeRoute in this MVP.
- KubeRoute emits per-request logs for `Put`, `Range`, and `DeleteRange`; if a request reaches KubeRoute you should see a corresponding log line.

## Development

```bash
go test ./...
go build ./cmd/kuberoute
```
