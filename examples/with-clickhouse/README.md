# Ponder with ClickHouse

This project indexes finalized Mainnet blocks into Postgres and writes the same
finalized events to ClickHouse for analytics.

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

Query the projection after a finalized block is indexed:

```bash
curl --data-binary "
  SELECT event_id, block_number, event_name
  FROM default.ponder_events FINAL
  WHERE project_id = 'with-clickhouse'
  ORDER BY block_number DESC
  LIMIT 10
" http://127.0.0.1:8123/
```

Stop local services with:

```bash
docker compose down -v
```
