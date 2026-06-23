import { createHash } from "node:crypto";
import type { FinalizedSinkBatch, SinkSetupContext } from "ponder";
import { beforeEach, expect, test, vi } from "vitest";

vi.mock("@clickhouse/client", () => ({
  createClient: vi.fn(),
}));

import { createClient } from "@clickhouse/client";

type MockFunction = {
  mockReturnValue: (value: unknown) => void;
};

const mocks = {
  close: vi.fn(),
  command: vi.fn(),
  createClient: createClient as typeof createClient & MockFunction,
  insert: vi.fn(),
};

import { createClickHouseSink } from "./index.js";

const context = {
  logger: {
    error: vi.fn(),
    warn: vi.fn(),
    info: vi.fn(),
    debug: vi.fn(),
  },
  metrics: { recordRetry: vi.fn() },
} satisfies SinkSetupContext;

const batch = {
  version: 1,
  id: "batch-1",
  checkpoint: "0x0000000000000001",
  events: [
    {
      id: "event-1",
      checkpoint: "0x0000000000000001",
      chain: { id: 1, name: "mainnet" },
      name: "Token:Transfer",
      type: "log",
      event: {
        id: "event-1",
        args: { amount: 1n },
        log: {
          address: "0x0000000000000000000000000000000000000001",
          data: "0x",
          logIndex: 3,
          removed: false,
          topics: [],
        },
        block: {
          baseFeePerGas: null,
          difficulty: 0n,
          extraData: "0x",
          gasLimit: 0n,
          gasUsed: 0n,
          hash: "0x",
          logsBloom: "0x",
          miner: "0x0000000000000000000000000000000000000000",
          mixHash: null,
          nonce: null,
          number: 42n,
          parentHash: "0x",
          receiptsRoot: "0x",
          sha3Uncles: "0x",
          size: 0n,
          stateRoot: "0x",
          timestamp: 1_700_000_000n,
          totalDifficulty: null,
          transactionsRoot: "0x",
        },
        transaction: {
          from: "0x0000000000000000000000000000000000000002",
          gas: 0n,
          gasPrice: 0n,
          hash: "0x",
          input: "0x",
          nonce: 0,
          r: null,
          s: null,
          to: "0x0000000000000000000000000000000000000003",
          transactionIndex: 0,
          type: "legacy",
          v: null,
          value: 0n,
        },
      },
    },
  ],
} satisfies FinalizedSinkBatch;

const getExpectedEventId = (
  event: FinalizedSinkBatch["events"][number],
): string =>
  createHash("sha256")
    .update(`${event.checkpoint}:${event.name}:${event.id}`)
    .digest("hex");

beforeEach(() => {
  vi.clearAllMocks();
  mocks.createClient.mockReturnValue({
    close: mocks.close,
    command: mocks.command,
    insert: mocks.insert,
  });
  mocks.command.mockResolvedValue({});
  mocks.insert.mockResolvedValue({});
  mocks.close.mockResolvedValue(undefined);
});

test("createClickHouseSink() validates the target identity", () => {
  expect(() =>
    createClickHouseSink({
      url: "postgres://localhost:5432",
      projectId: "project",
    }),
  ).toThrow("url must be a valid HTTP or HTTPS URL");

  expect(() =>
    createClickHouseSink({
      url: "http://localhost:8123",
      projectId: "project id",
    }),
  ).toThrow("projectId must use letters, numbers, hyphens, and underscores");

  expect(() =>
    createClickHouseSink({
      url: "http://localhost:8123/analytics",
      projectId: "project",
    }),
  ).toThrow("url must not include a path, query string, or fragment");

  expect(() =>
    createClickHouseSink({
      url: "http://localhost:8123",
      projectId: "project",
      schema: { table: "events; DROP TABLE events" },
    }),
  ).toThrow("schema.table must use letters, numbers, and underscores");
});

test("createClickHouseSink() creates a versioned event table", async () => {
  const sink = createClickHouseSink({
    url: "http://localhost:8123",
    projectId: "project",
    schema: { database: "analytics", autoCreate: true },
  });

  await sink.setup?.(context);

  expect(mocks.command).toHaveBeenCalledWith({
    query: expect.stringContaining(
      "CREATE TABLE IF NOT EXISTS analytics.ponder_events",
    ),
  });
  expect(mocks.command).toHaveBeenCalledWith({
    query: expect.stringContaining("schema_version UInt8"),
  });
  expect(mocks.command).toHaveBeenCalledWith({
    query: expect.stringContaining("ReplacingMergeTree"),
  });
});

test("createClickHouseSink() maps finalized events deterministically", async () => {
  const sink = createClickHouseSink({
    url: "http://localhost:8123",
    projectId: "project",
    schema: { table: "events" },
  });

  await sink.setup?.(context);
  await sink.writeFinalizedBatch(batch);

  expect(mocks.insert).toHaveBeenCalledWith({
    table: "events",
    format: "JSONEachRow",
    values: [
      {
        schema_version: 1,
        event_id: getExpectedEventId(batch.events[0]!),
        batch_id: "batch-1",
        project_id: "project",
        chain_id: 1,
        checkpoint: "0x0000000000000001",
        block_number: "42",
        block_timestamp: 1_700_000_000,
        transaction_hash: "0x",
        log_index: 3,
        event_name: "Transfer",
        event_type: "log",
        contract_name: "Token",
        contract_address: "0x0000000000000000000000000000000000000001",
        payload: expect.stringContaining('"amount":"1"'),
      },
    ],
  });
});

test("createClickHouseSink() assigns distinct event ids per callback", async () => {
  const checkpoint = "0x0000000000000001";
  const sink = createClickHouseSink({
    url: "http://localhost:8123",
    projectId: "project",
    schema: { table: "events" },
  });
  const multiCallbackBatch = {
    ...batch,
    events: [
      {
        ...batch.events[0]!,
        id: checkpoint,
        checkpoint,
        name: "Token:Transfer",
        event: { ...batch.events[0]!.event, id: checkpoint },
      },
      {
        ...batch.events[0]!,
        id: checkpoint,
        checkpoint,
        name: "Token:Approval",
        event: { ...batch.events[0]!.event, id: checkpoint },
      },
    ],
  } satisfies FinalizedSinkBatch;

  await sink.setup?.(context);
  await sink.writeFinalizedBatch(multiCallbackBatch);

  const values = mocks.insert.mock.calls[0]![0].values as Array<{
    event_id: string;
  }>;

  expect(values[0]!.event_id).not.toBe(values[1]!.event_id);
  expect(values[0]!.event_id).toBe(
    getExpectedEventId(multiCallbackBatch.events[0]!),
  );
  expect(values[1]!.event_id).toBe(
    getExpectedEventId(multiCallbackBatch.events[1]!),
  );
});

test("createClickHouseSink() omits contract address for account transaction events", async () => {
  const sink = createClickHouseSink({
    url: "http://localhost:8123",
    projectId: "project",
    schema: { table: "events" },
  });
  const transactionBatch = {
    version: 1,
    id: "batch-2",
    checkpoint: "0x0000000000000001",
    events: [
      {
        id: "event-2",
        checkpoint: "0x0000000000000001",
        chain: { id: 1, name: "mainnet" },
        name: "Wallet:transaction",
        type: "transaction",
        event: {
          id: "event-2",
          block: batch.events[0]!.event.block,
          transaction: batch.events[0]!.event.transaction,
        },
      },
    ],
  } satisfies FinalizedSinkBatch;

  await sink.setup?.(context);
  await sink.writeFinalizedBatch(transactionBatch);

  const values = mocks.insert.mock.calls[0]![0].values as Array<{
    contract_address: string | null;
    event_type: string;
  }>;

  expect(values[0]!.event_type).toBe("transaction");
  expect(values[0]!.contract_address).toBeNull();
});

test("createClickHouseSink() retries failed writes with bounded attempts", async () => {
  mocks.insert.mockRejectedValueOnce(new Error("unavailable"));
  const sink = createClickHouseSink({
    url: "http://localhost:8123",
    projectId: "project",
    maxRetries: 1,
    retryDelayMs: 0,
  });

  await sink.setup?.(context);
  await sink.writeFinalizedBatch(batch);

  expect(mocks.insert).toHaveBeenCalledTimes(2);
  expect(context.metrics.recordRetry).toHaveBeenCalledTimes(1);
  expect(context.logger.warn).toHaveBeenCalledWith(
    expect.objectContaining({
      msg: "Retrying ClickHouse sink request",
      operation: "write",
      retry_count: 1,
    }),
  );
});

test("createClickHouseSink() fails after exhausted retries", async () => {
  mocks.insert.mockRejectedValue(new Error("unavailable"));
  const sink = createClickHouseSink({
    url: "http://localhost:8123",
    projectId: "project",
    maxRetries: 1,
    retryDelayMs: 0,
  });

  await sink.setup?.(context);

  await expect(sink.writeFinalizedBatch(batch)).rejects.toThrow("unavailable");
  expect(mocks.insert).toHaveBeenCalledTimes(2);
  expect(context.metrics.recordRetry).toHaveBeenCalledTimes(1);
  expect(context.logger.error).toHaveBeenCalledWith(
    expect.objectContaining({
      msg: "ClickHouse sink request failed",
      operation: "write",
      retry_count: 1,
    }),
  );
});

test("createClickHouseSink() closes its client", async () => {
  const sink = createClickHouseSink({
    url: "http://localhost:8123",
    projectId: "project",
  });

  await sink.shutdown?.();

  expect(mocks.close).toHaveBeenCalledTimes(1);
});
