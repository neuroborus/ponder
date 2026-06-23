# Ponder with ClickHouse

This project indexes Mainnet blocks into Postgres and writes finalized events
to ClickHouse for analytics. Set `PONDER_CLICKHOUSE_LIVE=true` in `.env.local`
to opt into provisional events and reorg revocations in a separate v2 table.

## Run locally

From the repository root:

```bash
pnpm install
pnpm build
cd examples/with-clickhouse
cp .env.example .env.local
docker compose up -d --wait
pnpm dev
```

Set `PONDER_RPC_URL_1` in `.env.local` before starting Ponder. The sink writes
only after a block is finalized.

By default, query the finalized projection after a block is finalized:

```bash
curl --data-binary "
  SELECT event_id, block_number, event_name
  FROM default.ponder_events FINAL
  WHERE project_id = 'with-clickhouse'
  ORDER BY block_number DESC
  LIMIT 10
" http://127.0.0.1:8123/
```

With `PONDER_CLICKHOUSE_LIVE=true`, query active provisional events instead:

```bash
curl --data-binary "
  SELECT event_id, block_number, event_name
  FROM default.ponder_events_v2 FINAL
  WHERE project_id = 'with-clickhouse'
    AND row_kind = 'event'
  ORDER BY block_number DESC
  LIMIT 10
" http://127.0.0.1:8123/
```

The v2 table is append-only. A reorg appends a revocation row; `FINAL` filters
superseded rows deterministically.

Stop local services with:

```bash
docker compose down -v
```
