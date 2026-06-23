import type { IndexingSink } from "ponder";
import { assertType, test } from "vitest";
import { createClickHouseSink } from "./index.js";

test("createClickHouseSink() config", () => {
  const sink = createClickHouseSink({
    url: "https://clickhouse.example.com",
    projectId: "my-indexer",
    requestTimeout: 30_000,
    maxRetries: 3,
    retryDelayMs: 250,
    schema: {
      autoCreate: true,
      database: "analytics",
      table: "ponder_events",
    },
  });

  assertType<IndexingSink>(sink);
});

test("createClickHouseSink() rejects invalid config types", () => {
  createClickHouseSink({
    url: "https://clickhouse.example.com",
    // @ts-expect-error projectId must be a string
    projectId: 1,
  });

  createClickHouseSink({
    url: "https://clickhouse.example.com",
    projectId: "my-indexer",
    schema: {
      // @ts-expect-error autoCreate must be a boolean
      autoCreate: "true",
    },
  });
});
