import type { IndexingSink, SinkSetupContext } from "ponder";
import { afterAll, afterEach, beforeEach, expect, vi } from "vitest";
import { acceptanceTest, clickhouseUrl } from "./_test/acceptance.js";
import { createClickHouseServer } from "./_test/clickhouseServer.js";
import { createBatch } from "./_test/fixtures.js";
import { createClickHouseSink } from "./index.js";

const table = "ponder_clickhouse_acceptance";
const qualifiedTable = `default.${table}`;
const requireClickhouse = () => {
  if (clickhouseUrl === undefined) {
    throw new Error("CLICKHOUSE_URL is required");
  }

  return createClickHouseServer(clickhouseUrl);
};

const context = {
  logger: {
    error: vi.fn(),
    warn: vi.fn(),
    info: vi.fn(),
    debug: vi.fn(),
  },
  metrics: { recordRetry: vi.fn() },
} satisfies SinkSetupContext;

const sinks: IndexingSink[] = [];

const trackSink = (sink: IndexingSink) => {
  sinks.push(sink);
  return sink;
};

const createSink = ({
  url = clickhouseUrl,
  autoCreate = true,
  maxRetries = 0,
}: {
  url?: string;
  autoCreate?: boolean;
  maxRetries?: number;
} = {}) => {
  if (url === undefined) throw new Error("CLICKHOUSE_URL is required");

  return createClickHouseSink({
    url,
    projectId: "acceptance",
    maxRetries,
    retryDelayMs: 0,
    schema: { table, autoCreate },
  });
};

const openSink = (
  options: {
    url?: string;
    autoCreate?: boolean;
    maxRetries?: number;
  } = {},
) => trackSink(createSink(options));

beforeEach(async () => {
  vi.clearAllMocks();
  if (clickhouseUrl === undefined) return;
  await requireClickhouse().execute(`DROP TABLE IF EXISTS ${qualifiedTable}`);
});

afterEach(async () => {
  await Promise.all(sinks.splice(0).map((sink) => sink.shutdown?.()));
});

afterAll(async () => {
  if (clickhouseUrl === undefined) return;
  await requireClickhouse().execute(`DROP TABLE IF EXISTS ${qualifiedTable}`);
});

acceptanceTest(
  "createClickHouseSink() creates a table and writes a historical finalized batch",
  async () => {
    const sink = openSink();

    await sink.setup?.(context);
    await sink.writeFinalizedBatch(
      createBatch({ batchId: "batch-historical", eventId: "event-historical" }),
    );

    const rows = await requireClickhouse().query<{
      contract_address: string;
      event_name: string;
      event_type: string;
      payload: string;
      project_id: string;
    }>(`
    SELECT project_id, event_name, event_type, contract_address, payload
    FROM ${qualifiedTable} FINAL
  `);

    expect(rows).toStrictEqual([
      {
        project_id: "acceptance",
        event_name: "Transfer",
        event_type: "log",
        contract_address: "0x0000000000000000000000000000000000000001",
        payload: expect.stringContaining('"amount":"1"'),
      },
    ]);
  },
);

acceptanceTest(
  "createClickHouseSink() writes a realtime finalized batch",
  async () => {
    const sink = openSink();

    await sink.setup?.(context);
    await sink.writeFinalizedBatch(
      createBatch({
        batchId: "batch-realtime",
        checkpoint: "0x0000000000000002",
        eventId: "event-realtime",
      }),
    );

    expect(await requireClickhouse().getEventCount(qualifiedTable)).toBe(1);
  },
);

acceptanceTest(
  "createClickHouseSink() replays failed delivery duplicate-safe after restart",
  async () => {
    const batch = createBatch({
      batchId: "batch-replay",
      eventId: "event-replay",
    });
    const failedSink = openSink({
      url: "http://127.0.0.1:1",
      autoCreate: false,
    });

    await failedSink.setup?.(context);
    await expect(failedSink.writeFinalizedBatch(batch)).rejects.toThrow();

    const firstSink = openSink();

    await firstSink.setup?.(context);
    await firstSink.writeFinalizedBatch(batch);

    const restartedSink = openSink();
    await restartedSink.setup?.(context);
    await restartedSink.writeFinalizedBatch(batch);

    expect(await requireClickhouse().getEventCount(qualifiedTable)).toBe(1);
  },
);

acceptanceTest(
  "createClickHouseSink() retains written rows through shutdown",
  async () => {
    const sink = openSink();

    await sink.setup?.(context);
    await sink.writeFinalizedBatch(
      createBatch({ batchId: "batch-shutdown", eventId: "event-shutdown" }),
    );

    expect(await requireClickhouse().getEventCount(qualifiedTable)).toBe(1);
  },
);

acceptanceTest(
  "createClickHouseSink() rejects invalid configuration and exhausted retries",
  async () => {
    expect(() =>
      createClickHouseSink({ url: "invalid", projectId: "acceptance" }),
    ).toThrow("url must be a valid HTTP or HTTPS URL");

    const sink = openSink({ url: "http://127.0.0.1:1", maxRetries: 1 });
    await sink.setup?.(context);
    await expect(sink.writeFinalizedBatch(createBatch())).rejects.toThrow();

    expect(context.metrics.recordRetry).toHaveBeenCalledTimes(1);
    expect(context.logger.error).toHaveBeenCalledWith(
      expect.objectContaining({ operation: "write", retry_count: 1 }),
    );
  },
);
