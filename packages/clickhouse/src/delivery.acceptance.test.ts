import {
  context,
  setupCleanup,
  setupCommon,
  setupDatabaseServices,
  setupIsolatedDatabase,
} from "@/_test/setup.js";
import { getFinalizedEventsMultichain } from "@/runtime/realtime.js";
import { createSinkService } from "@/sink/index.js";
import { ZERO_CHECKPOINT, encodeCheckpoint } from "@/utils/checkpoint.js";
import { afterAll, beforeEach, expect, vi } from "vitest";
import { clickhouseUrl, deliveryTest } from "./_test/acceptance.js";
import { createClickHouseServer } from "./_test/clickhouseServer.js";
import {
  createBlockEvent,
  getPendingDeliveries,
  namespace,
} from "./_test/delivery.js";
import { createClickHouseSink } from "./index.js";

const table = "ponder_clickhouse_delivery";
const qualifiedTable = `default.${table}`;
const clickhouse =
  clickhouseUrl === undefined
    ? undefined
    : createClickHouseServer(clickhouseUrl);

const createSink = (url: string, autoCreate = true) =>
  createClickHouseSink({
    url,
    projectId: "delivery",
    maxRetries: 0,
    retryDelayMs: 0,
    schema: { table, autoCreate },
  });

const setupSink = async (url: string, autoCreate = true) => {
  const sink = createSink(url, autoCreate);
  await sink.setup?.({
    logger: context.common.logger.child({ sink: "clickhouse" }),
    metrics: {
      recordRetry: () => {
        context.common.metrics.ponder_sink_delivery_retry_total.inc({
          sink: "clickhouse",
        });
      },
    },
  });
  return sink;
};

beforeEach(setupCommon);
beforeEach(setupIsolatedDatabase);
beforeEach(setupCleanup);

beforeEach(async () => {
  if (clickhouse === undefined) return;
  await clickhouse.execute(`DROP TABLE IF EXISTS ${qualifiedTable}`);
});

afterAll(async () => {
  if (clickhouse === undefined) return;
  await clickhouse.execute(`DROP TABLE IF EXISTS ${qualifiedTable}`);
});

deliveryTest(
  "does not deliver a Postgres outbox batch to ClickHouse before drain",
  async () => {
    if (clickhouse === undefined || clickhouseUrl === undefined) return;

    const { database } = await setupDatabaseServices({
      namespaceBuild: namespace,
    });
    const sink = await setupSink(clickhouseUrl);
    const service = createSinkService({
      common: context.common,
      database,
      namespace,
      sinks: [sink],
    });
    const event = createBlockEvent({ id: "event-historical" });

    await database.userQB.transaction((tx) => service.enqueue(tx, [event]));

    expect(await getPendingDeliveries(database)).toHaveLength(1);
    expect(await clickhouse.getEventCount(qualifiedTable)).toBe(0);

    await service.drain();

    expect(await getPendingDeliveries(database)).toHaveLength(0);
    expect(await clickhouse.getEventCount(qualifiedTable)).toBe(1);

    await sink.shutdown?.();
  },
);

deliveryTest(
  "replays a durable Postgres outbox batch to ClickHouse after restart",
  async () => {
    if (clickhouse === undefined || clickhouseUrl === undefined) return;

    const { database } = await setupDatabaseServices({
      namespaceBuild: namespace,
    });
    const event = createBlockEvent({ id: "event-replay" });
    const service = createSinkService({
      common: context.common,
      database,
      namespace,
      sinks: [await setupSink("http://127.0.0.1:1", false)],
    });

    await database.userQB.transaction((tx) => service.enqueue(tx, [event]));

    await expect(service.drain()).rejects.toThrow();
    expect(await getPendingDeliveries(database)).toHaveLength(1);
    expect(await clickhouse.getEventCount(qualifiedTable)).toBe(0);

    const recoveredSink = await setupSink(clickhouseUrl);
    const restarted = createSinkService({
      common: context.common,
      database,
      namespace,
      sinks: [recoveredSink],
    });

    await restarted.drain();

    expect(await getPendingDeliveries(database)).toHaveLength(0);
    expect(await clickhouse.getEventCount(qualifiedTable)).toBe(1);

    await recoveredSink.shutdown?.();
  },
);

deliveryTest(
  "replays a successful ClickHouse write after failed acknowledgement duplicate-safe",
  async () => {
    if (clickhouse === undefined || clickhouseUrl === undefined) return;

    const { database } = await setupDatabaseServices({
      namespaceBuild: namespace,
    });
    const sink = await setupSink(clickhouseUrl);
    const service = createSinkService({
      common: context.common,
      database,
      namespace,
      sinks: [sink],
    });
    const event = createBlockEvent({ id: "event-ack-race" });

    await database.userQB.transaction((tx) => service.enqueue(tx, [event]));

    const originalWrap = database.userQB.wrap.bind(database.userQB);
    vi.spyOn(database.userQB, "wrap").mockImplementation(((
      ...args: Parameters<typeof originalWrap>
    ) => {
      const [arg1] = args;
      if (
        typeof arg1 === "object" &&
        arg1 !== null &&
        "label" in arg1 &&
        arg1.label === "delete_sink_delivery"
      ) {
        throw new Error("ack failed");
      }

      return originalWrap(...args);
    }) as typeof database.userQB.wrap);

    await expect(service.drain()).rejects.toThrow("ack failed");
    expect(await getPendingDeliveries(database)).toHaveLength(1);
    expect(await clickhouse.getEventCount(qualifiedTable)).toBe(1);

    vi.restoreAllMocks();

    const replayedSink = await setupSink(clickhouseUrl);
    const restarted = createSinkService({
      common: context.common,
      database,
      namespace,
      sinks: [replayedSink],
    });

    await restarted.drain();

    expect(await getPendingDeliveries(database)).toHaveLength(0);
    expect(await clickhouse.getEventCount(qualifiedTable)).toBe(1);

    await replayedSink.shutdown?.();
  },
);

deliveryTest(
  "does not deliver unfinalized multichain events to ClickHouse",
  async () => {
    if (clickhouse === undefined || clickhouseUrl === undefined) return;

    const { database } = await setupDatabaseServices({
      namespaceBuild: namespace,
    });
    const sink = await setupSink(clickhouseUrl);
    const service = createSinkService({
      common: context.common,
      database,
      namespace,
      sinks: [sink],
    });
    const chainA = createBlockEvent({
      id: "event-a",
      checkpoint: encodeCheckpoint({
        ...ZERO_CHECKPOINT,
        chainId: 1n,
        blockNumber: 1n,
        blockTimestamp: 1n,
      }),
    });
    const chainB = createBlockEvent({
      id: "event-b",
      checkpoint: encodeCheckpoint({
        ...ZERO_CHECKPOINT,
        chainId: 2n,
        blockNumber: 1n,
        blockTimestamp: 2n,
      }),
      chain: { ...chainA.chain, id: 2, name: "optimism" },
    });
    const { finalizedEvents } = getFinalizedEventsMultichain([chainA, chainB], {
      chain: chainB.chain,
      checkpoint: chainB.checkpoint,
    });

    await database.userQB.transaction((tx) =>
      service.enqueue(tx, finalizedEvents),
    );
    await service.drain();

    expect(await clickhouse.getEventCount(qualifiedTable)).toBe(1);

    await sink.shutdown?.();
  },
);
