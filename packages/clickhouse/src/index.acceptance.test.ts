import type { IndexingSink, SinkSetupContext } from "ponder";
import { afterAll, afterEach, beforeEach, expect, vi } from "vitest";
import { acceptanceTest, clickhouseUrl } from "./_test/acceptance.js";
import { createClickHouseServer } from "./_test/clickhouseServer.js";
import {
  createBatch,
  createLiveBatch,
  createReorgBatch,
} from "./_test/fixtures.js";
import { createClickHouseSink } from "./index.js";

const table = "ponder_clickhouse_acceptance";
const qualifiedTable = `default.${table}`;
const liveTable = "ponder_clickhouse_live_acceptance";
const qualifiedLiveTable = `default.${liveTable}`;
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
  live = false,
  maxRetries = 0,
}: {
  url?: string;
  autoCreate?: boolean;
  live?: boolean;
  maxRetries?: number;
} = {}) => {
  if (url === undefined) throw new Error("CLICKHOUSE_URL is required");

  return createClickHouseSink({
    url,
    projectId: "acceptance",
    live,
    maxRetries,
    retryDelayMs: 0,
    schema: { table: live ? liveTable : table, autoCreate },
  });
};

const openSink = (
  options: {
    url?: string;
    autoCreate?: boolean;
    live?: boolean;
    maxRetries?: number;
  } = {},
) => trackSink(createSink(options));

beforeEach(async () => {
  vi.clearAllMocks();
  if (clickhouseUrl === undefined) return;
  await requireClickhouse().execute(`DROP TABLE IF EXISTS ${qualifiedTable}`);
  await requireClickhouse().execute(
    `DROP TABLE IF EXISTS ${qualifiedLiveTable}`,
  );
});

afterEach(async () => {
  await Promise.all(sinks.splice(0).map((sink) => sink.shutdown?.()));
});

afterAll(async () => {
  if (clickhouseUrl === undefined) return;
  await requireClickhouse().execute(`DROP TABLE IF EXISTS ${qualifiedTable}`);
  await requireClickhouse().execute(
    `DROP TABLE IF EXISTS ${qualifiedLiveTable}`,
  );
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
  "createClickHouseSink() writes live events and suppresses reorged events",
  async () => {
    const sink = openSink({ live: true });
    if (
      sink.writeLiveBatch === undefined ||
      sink.writeReorgBatch === undefined
    ) {
      throw new Error("Expected a live ClickHouse sink");
    }

    await sink.setup?.(context);
    await sink.writeLiveBatch(createLiveBatch());

    expect(
      await requireClickhouse().query<{ event_id: string }>(`
        SELECT event_id
        FROM ${qualifiedLiveTable} FINAL
        WHERE project_id = 'acceptance' AND row_kind = 'event'
      `),
    ).toHaveLength(1);

    await sink.writeLiveBatch(createLiveBatch());
    await sink.writeReorgBatch(createReorgBatch());
    await sink.writeReorgBatch(createReorgBatch());

    expect(
      await requireClickhouse().query<{ event_id: string }>(`
        SELECT event_id
        FROM ${qualifiedLiveTable} FINAL
        WHERE project_id = 'acceptance' AND row_kind = 'event'
      `),
    ).toStrictEqual([]);
    const rows = await requireClickhouse().query<{
      row_kind: string;
      row_version: string | number;
    }>(`
      SELECT row_kind, row_version
      FROM ${qualifiedLiveTable} FINAL
      WHERE project_id = 'acceptance'
    `);

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ row_kind: "revocation" });
    expect(Number(rows[0]!.row_version)).toBe(2);
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
      expect.objectContaining({ operation: "write_finalized", retry_count: 1 }),
    );
  },
);
