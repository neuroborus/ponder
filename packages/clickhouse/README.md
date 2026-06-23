# @ponder/clickhouse

Finalized ClickHouse analytics sink for Ponder.

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

Ponder writes only finalized batches. Each batch is persisted in Postgres before
delivery, then acknowledged after a successful ClickHouse response. Delivery is
at least once: a batch can be replayed after restart.

The table is duplicate-safe when queried with `FINAL`:

```sql
SELECT *
FROM default.ponder_events FINAL
WHERE project_id = 'mainnet-analytics';
```

See the [ClickHouse guide](https://ponder.sh/docs/advanced/clickhouse) and the
[runnable example](https://github.com/ponder-sh/ponder/tree/main/examples/with-clickhouse)
for setup, operations, and the event schema.
