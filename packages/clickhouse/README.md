# @ponder/clickhouse

ClickHouse analytics sink for Ponder. It writes finalized events by default;
live delivery is opt-in.

## Install

```bash
pnpm add @ponder/clickhouse
```

## Configure

```ts
import { createClickHouseSink } from "@ponder/clickhouse";

const sink = createClickHouseSink({
  url: "https://clickhouse.example.com",
  projectId: "mainnet-analytics",
  schema: {
    database: "default",
    table: "ponder_events",
    autoCreate: true,
  },
});
```

Add `sink` to `createConfig({ sinks: [sink] })` in a Ponder project that uses
Postgres. Sinks are rejected with PGlite.

The ClickHouse database must already exist. `autoCreate` creates only the event
table. Use `username`, `password`, and an HTTPS URL when required by your
ClickHouse deployment.

## Delivery

Each batch is persisted in Postgres before delivery, then acknowledged after a
successful ClickHouse response. Delivery is at least once: a batch can be
replayed after restart.

Finalized delivery uses the v1 `ponder_events` table by default. It is
duplicate-safe when queried with `FINAL`:

```sql
SELECT *
FROM default.ponder_events FINAL
WHERE project_id = 'mainnet-analytics';
```

## Live delivery

Enable live event and reorg delivery explicitly:

```ts
const sink = createClickHouseSink({
  url: "https://clickhouse.example.com",
  projectId: "mainnet-analytics",
  live: true,
  schema: { autoCreate: true },
});
```

Live mode writes to a separate `ponder_events_v2` table by default and never
changes the v1 table. It appends event and revocation rows with a stable
`event_id` and causal `row_version`. Query active events with `FINAL`:

```sql
SELECT *
FROM default.ponder_events_v2 FINAL
WHERE project_id = 'mainnet-analytics'
  AND row_kind = 'event';
```

Live events are provisional. A reorg becomes invisible to this query after its
durable revocation batch is delivered. Without `FINAL`, ClickHouse merge delay
can expose superseded rows temporarily.

See the [ClickHouse guide](https://ponder.sh/docs/advanced/clickhouse) and the
[runnable example](https://github.com/ponder-sh/ponder/tree/main/examples/with-clickhouse)
for setup, operations, and the event schema.
