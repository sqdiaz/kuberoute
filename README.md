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

1. Start two local etcd instances:

```bash
brew install etcd
etcd --listen-client-urls=http://127.0.0.1:2379 --advertise-client-urls=http://127.0.0.1:2379
```

```bash
etcd --listen-client-urls=http://127.0.0.1:3379 --advertise-client-urls=http://127.0.0.1:3379
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
ETCDCTL_API=3 etcdctl --endpoints=127.0.0.1:22379 put /eventual/demo hello
ETCDCTL_API=3 etcdctl --endpoints=127.0.0.1:22379 get /eventual/demo
ETCDCTL_API=3 etcdctl --endpoints=127.0.0.1:22379 put /bounded/demo world
ETCDCTL_API=3 etcdctl --endpoints=127.0.0.1:22379 get /bounded/demo
```

## Development

```bash
go test ./...
go build ./cmd/kuberoute
```
