import { createHash } from "node:crypto";
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
import { createClickHouseSink } from "@ponder/clickhouse";
import type { IndexingSink } from "ponder";
import { afterAll, afterEach, beforeEach, expect, test, vi } from "vitest";
import { createClickHouseServer } from "./clickhouseServer.js";
import {
  createBlockEvent,
  getPendingDeliveries,
  namespace,
} from "./fixtures.js";

const isAcceptanceRun = process.env.PONDER_CLICKHOUSE_ACCEPTANCE === "1";
const clickhouseUrl = process.env.CLICKHOUSE_URL;

if (isAcceptanceRun && clickhouseUrl === undefined) {
  throw new Error(
    "CLICKHOUSE_URL is required for delivery acceptance tests. Export it in the shell or pass it inline.",
  );
}

const deliveryTest =
  isAcceptanceRun &&
  clickhouseUrl !== undefined &&
  process.env.DATABASE_URL !== undefined
    ? test
    : test.skip;

const table = "ponder_clickhouse_delivery";
const qualifiedTable = `default.${table}`;
const clickhouse =
  clickhouseUrl === undefined
    ? undefined
    : createClickHouseServer(clickhouseUrl);

const sinks: IndexingSink[] = [];

const createSink = (url: string, autoCreate = true) =>
  createClickHouseSink({
    url,
    projectId: "delivery",
    maxRetries: 0,
    retryDelayMs: 0,
    schema: { table, autoCreate },
  });

const trackSink = (sink: IndexingSink) => {
  sinks.push(sink);
  return sink;
};

const getEventId = (checkpoint: string, name: string, id: string): string =>
  createHash("sha256").update(`${checkpoint}:${name}:${id}`).digest("hex");

beforeEach(setupCommon);
beforeEach(setupIsolatedDatabase);
beforeEach(setupCleanup);

beforeEach(async () => {
  if (clickhouse === undefined) return;
  await clickhouse.execute(`DROP TABLE IF EXISTS ${qualifiedTable}`);
});

afterEach(async () => {
  await Promise.all(sinks.splice(0).map((sink) => sink.shutdown?.()));
});

afterAll(async () => {
  if (clickhouse === undefined) return;
  await clickhouse.execute(`DROP TABLE IF EXISTS ${qualifiedTable}`);
});

deliveryTest(
  "does not deliver a Postgres outbox batch to ClickHouse before commit",
  async () => {
    if (clickhouse === undefined || clickhouseUrl === undefined) return;

    const { database } = await setupDatabaseServices({
      namespaceBuild: namespace,
    });
    const sink = trackSink(createSink(clickhouseUrl));
    const service = createSinkService({
      common: context.common,
      database,
      namespace,
      sinks: [sink],
    });
    const event = createBlockEvent({ id: "event-historical" });

    await database.userQB.transaction(async (tx) => {
      await service.enqueue(tx, [event]);
      expect(await clickhouse.getEventCount(qualifiedTable)).toBe(0);
    });

    expect(await getPendingDeliveries(database)).toHaveLength(1);
    expect(await clickhouse.getEventCount(qualifiedTable)).toBe(0);

    await service.start();

    expect(await getPendingDeliveries(database)).toHaveLength(0);
    expect(await clickhouse.getEventCount(qualifiedTable)).toBe(1);
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
      sinks: [trackSink(createSink("http://127.0.0.1:1", false))],
    });

    await database.userQB.transaction((tx) => service.enqueue(tx, [event]));

    await expect(service.start()).rejects.toThrow();
    expect(await getPendingDeliveries(database)).toHaveLength(1);
    expect(await clickhouse.getEventCount(qualifiedTable)).toBe(0);

    const restarted = createSinkService({
      common: context.common,
      database,
      namespace,
      sinks: [trackSink(createSink(clickhouseUrl))],
    });

    await restarted.start();

    expect(await getPendingDeliveries(database)).toHaveLength(0);
    expect(await clickhouse.getEventCount(qualifiedTable)).toBe(1);
  },
);

deliveryTest(
  "replays a successful ClickHouse write after failed acknowledgement duplicate-safe",
  async () => {
    if (clickhouse === undefined || clickhouseUrl === undefined) return;

    const { database } = await setupDatabaseServices({
      namespaceBuild: namespace,
    });
    const sink = trackSink(createSink(clickhouseUrl));
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

    try {
      await expect(service.start()).rejects.toThrow("ack failed");
      expect(await getPendingDeliveries(database)).toHaveLength(1);
      expect(await clickhouse.getEventCount(qualifiedTable)).toBe(1);

      const restarted = createSinkService({
        common: context.common,
        database,
        namespace,
        sinks: [trackSink(createSink(clickhouseUrl))],
      });

      await restarted.start();

      expect(await getPendingDeliveries(database)).toHaveLength(0);
      expect(await clickhouse.getEventCount(qualifiedTable)).toBe(1);
    } finally {
      vi.restoreAllMocks();
    }
  },
);

deliveryTest(
  "does not deliver unfinalized multichain events to ClickHouse",
  async () => {
    if (clickhouse === undefined || clickhouseUrl === undefined) return;

    const { database } = await setupDatabaseServices({
      namespaceBuild: namespace,
    });
    const sink = trackSink(createSink(clickhouseUrl));
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
    await service.start();

    const rows = await clickhouse.query<{ event_id: string }>(`
      SELECT event_id
      FROM ${qualifiedTable} FINAL
    `);
    const expectedEventId = getEventId(chainB.checkpoint, "Block", "event-b");

    expect(rows).toStrictEqual([{ event_id: expectedEventId }]);
    expect(rows.map((row) => row.event_id)).not.toContain(
      getEventId(chainA.checkpoint, "Block", "event-a"),
    );
  },
);
