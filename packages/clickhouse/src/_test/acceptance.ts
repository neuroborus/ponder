import { test } from "vitest";

export const isAcceptanceRun = process.env.PONDER_CLICKHOUSE_ACCEPTANCE === "1";

if (isAcceptanceRun && process.env.CLICKHOUSE_URL === undefined) {
  throw new Error(
    "CLICKHOUSE_URL is required for acceptance tests. Export it in the shell, pass it inline, or source packages/clickhouse/.env.local — see .env.example.",
  );
}

export const clickhouseUrl = process.env.CLICKHOUSE_URL;

export const acceptanceTest =
  isAcceptanceRun && clickhouseUrl !== undefined ? test : test.skip;
