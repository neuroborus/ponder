import { createHash } from "node:crypto";
import {
  type Database,
  getPonderSinkDeliveryTable,
  getPonderSinkSequenceTable,
} from "@/database/index.js";
import type { QB } from "@/database/queryBuilder.js";
import type { Common } from "@/internal/common.js";
import { NonRetryableUserError } from "@/internal/errors.js";
import type {
  Event,
  FinalizedSinkBatch,
  IndexingSink,
  LiveSinkBatch,
  NamespaceBuild,
  ReorgSinkBatch,
  SinkEvent,
  SinkSetupContext,
} from "@/internal/types.js";
import { asc, eq, sql } from "drizzle-orm";
import superjson from "superjson";

export type SinkService = {
  start: () => Promise<void>;
  enqueue: (tx: QB, events: Event[]) => Promise<void>;
  enqueueLive: (tx: QB, events: Event[]) => Promise<void>;
  enqueueReorg: (
    tx: QB,
    params: {
      chain: { id: number; name: string };
      checkpoint: string;
      events: Event[];
    },
  ) => Promise<void>;
  drain: () => Promise<void>;
  hasLiveSinks: boolean;
};

type SinkDeliveryKind = "finalized" | "live" | "reorg";

type SinkDeliveryBatch = FinalizedSinkBatch | LiveSinkBatch | ReorgSinkBatch;

type LiveSink = IndexingSink & {
  writeLiveBatch: NonNullable<IndexingSink["writeLiveBatch"]>;
  writeReorgBatch: NonNullable<IndexingSink["writeReorgBatch"]>;
};

const createSinkEvents = (events: Event[]): SinkEvent[] => {
  const sortedEvents = events
    .slice()
    .sort((a, b) => (a.checkpoint < b.checkpoint ? -1 : 1));

  return sortedEvents.map(
    (event): SinkEvent => ({
      id: event.event.id,
      checkpoint: event.checkpoint,
      chain: { id: event.chain.id, name: event.chain.name },
      name: event.eventCallback.name,
      type: event.type,
      event: event.event,
    }),
  );
};

const createFinalizedSinkBatch = (events: Event[]): FinalizedSinkBatch => {
  const sinkEvents = createSinkEvents(events);

  const checkpoint = sinkEvents[sinkEvents.length - 1]!.checkpoint;
  const id = createHash("sha256")
    .update(
      sinkEvents
        .map((event) => `${event.checkpoint}:${event.name}:${event.id}`)
        .join("\n"),
    )
    .digest("hex");

  return { version: 1, id, checkpoint, events: sinkEvents };
};

const getBatchChain = (events: SinkEvent[]): { id: number; name: string } => {
  const chain = events[0]!.chain;
  if (events.some((event) => event.chain.id !== chain.id)) {
    throw new Error("Live sink batches must contain events from one chain.");
  }
  return chain;
};

const getLiveBatchId = ({
  kind,
  checkpoint,
  events,
}: {
  kind: Exclude<SinkDeliveryKind, "finalized">;
  checkpoint: string;
  events: SinkEvent[];
}): string =>
  createHash("sha256")
    .update(
      `${kind}:${checkpoint}\n${events
        .map((event) => `${event.checkpoint}:${event.name}:${event.id}`)
        .join("\n")}`,
    )
    .digest("hex");

const createLiveSinkBatch = ({
  events,
  sequence,
}: {
  events: Event[];
  sequence: bigint;
}): LiveSinkBatch => {
  const sinkEvents = createSinkEvents(events);
  const chain = getBatchChain(sinkEvents);
  const checkpoint = sinkEvents[sinkEvents.length - 1]!.checkpoint;

  return {
    version: 1,
    id: getLiveBatchId({ kind: "live", checkpoint, events: sinkEvents }),
    checkpoint,
    chain,
    sequence,
    events: sinkEvents,
  };
};

const createReorgSinkBatch = ({
  chain,
  checkpoint,
  events,
  sequence,
}: {
  chain: { id: number; name: string };
  checkpoint: string;
  events: Event[];
  sequence: bigint;
}): ReorgSinkBatch => {
  const sinkEvents = createSinkEvents(events);
  const batchChain = getBatchChain(sinkEvents);
  if (batchChain.id !== chain.id) {
    throw new Error("Reorg sink batches must match the reorged chain.");
  }

  return {
    version: 1,
    id: getLiveBatchId({ kind: "reorg", checkpoint, events: sinkEvents }),
    checkpoint,
    chain,
    sequence,
    events: sinkEvents,
  };
};

const getDeliveryId = ({
  sinkName,
  batchId,
}: {
  sinkName: string;
  batchId: string;
}): string => {
  return createHash("sha256").update(`${sinkName}:${batchId}`).digest("hex");
};

export const createSinkService = ({
  common,
  database,
  namespace,
  sinks,
}: {
  common: Common;
  database: Database;
  namespace: NamespaceBuild;
  sinks: readonly IndexingSink[];
}): SinkService => {
  const PONDER_SINK_DELIVERY = getPonderSinkDeliveryTable(namespace.schema);
  const PONDER_SINK_SEQUENCE = getPonderSinkSequenceTable(namespace.schema);
  const liveSinks = sinks.filter(
    (sink): sink is LiveSink =>
      sink.writeLiveBatch !== undefined && sink.writeReorgBatch !== undefined,
  );

  const getSetupContext = (sinkName: string): SinkSetupContext => ({
    logger: common.logger.child({ sink: sinkName }),
    metrics: {
      recordRetry: () => {
        common.metrics.ponder_sink_delivery_retry_total.inc({ sink: sinkName });
      },
    },
  });

  const getNextSequence = async ({
    tx,
    sinkName,
    chainId,
  }: {
    tx: QB;
    sinkName: string;
    chainId: number;
  }): Promise<bigint> => {
    const result = await tx.wrap({ label: "advance_sink_sequence" }, (db) =>
      db
        .insert(PONDER_SINK_SEQUENCE)
        .values({ sinkName, chainId, sequence: 1n })
        .onConflictDoUpdate({
          target: [PONDER_SINK_SEQUENCE.sinkName, PONDER_SINK_SEQUENCE.chainId],
          set: { sequence: sql`${PONDER_SINK_SEQUENCE.sequence} + 1` },
        })
        .returning(),
    );

    return result[0]!.sequence;
  };

  const enqueueLiveDelivery = async ({
    tx,
    sink,
    kind,
    chain,
    checkpoint,
    events,
  }: {
    tx: QB;
    sink: LiveSink;
    kind: Exclude<SinkDeliveryKind, "finalized">;
    chain: { id: number; name: string };
    checkpoint: string;
    events: Event[];
  }): Promise<void> => {
    const sequence = await getNextSequence({
      tx,
      sinkName: sink.name,
      chainId: chain.id,
    });
    const batch =
      kind === "live"
        ? createLiveSinkBatch({ events, sequence })
        : createReorgSinkBatch({ chain, checkpoint, events, sequence });

    await tx.wrap({ label: "enqueue_sink_delivery" }, (db) =>
      db
        .insert(PONDER_SINK_DELIVERY)
        .values({
          id: getDeliveryId({ sinkName: sink.name, batchId: batch.id }),
          sinkName: sink.name,
          kind,
          chainId: chain.id,
          sequence,
          checkpoint: batch.checkpoint,
          payload: superjson.stringify(batch),
          createdAt: Date.now(),
        })
        .onConflictDoNothing(),
    );
  };

  const drain = async (): Promise<void> => {
    for (const sink of sinks) {
      const deliveries = await database.userQB.wrap(
        { label: "get_sink_delivery" },
        (db) =>
          db
            .select()
            .from(PONDER_SINK_DELIVERY)
            .where(eq(PONDER_SINK_DELIVERY.sinkName, sink.name))
            .orderBy(
              asc(PONDER_SINK_DELIVERY.chainId),
              asc(PONDER_SINK_DELIVERY.sequence),
              asc(PONDER_SINK_DELIVERY.checkpoint),
              asc(PONDER_SINK_DELIVERY.id),
            ),
      );

      common.metrics.ponder_sink_delivery_pending.set(
        { sink: sink.name },
        deliveries.length,
      );

      for (const [index, delivery] of deliveries.entries()) {
        const batch = superjson.parse<SinkDeliveryBatch>(delivery.payload);
        const startTime = Date.now();

        try {
          switch (delivery.kind as SinkDeliveryKind) {
            case "finalized":
              await sink.writeFinalizedBatch(batch as FinalizedSinkBatch);
              break;
            case "live":
              if (sink.writeLiveBatch === undefined) {
                throw new Error(
                  `Sink '${sink.name}' cannot deliver a pending live batch.`,
                );
              }
              await sink.writeLiveBatch(batch as LiveSinkBatch);
              break;
            case "reorg":
              if (sink.writeReorgBatch === undefined) {
                throw new Error(
                  `Sink '${sink.name}' cannot deliver a pending reorg batch.`,
                );
              }
              await sink.writeReorgBatch(batch as ReorgSinkBatch);
              break;
            default:
              throw new Error(`Unknown sink delivery kind '${delivery.kind}'.`);
          }
        } catch (error) {
          const duration = Date.now() - startTime;
          common.metrics.ponder_sink_delivery_total.inc({
            sink: sink.name,
            outcome: "error",
          });
          common.metrics.ponder_sink_delivery_duration_ms.observe(
            { sink: sink.name, outcome: "error" },
            duration,
          );
          common.logger.error({
            msg: "Failed sink delivery",
            sink: sink.name,
            delivery_kind: delivery.kind,
            checkpoint: batch.checkpoint,
            batch_id: batch.id,
            duration,
            error: error as Error,
          });
          throw error;
        }

        const duration = Date.now() - startTime;
        common.metrics.ponder_sink_delivery_total.inc({
          sink: sink.name,
          outcome: "success",
        });
        common.metrics.ponder_sink_delivery_duration_ms.observe(
          { sink: sink.name, outcome: "success" },
          duration,
        );
        common.metrics.ponder_sink_delivery_events_total.inc(
          { sink: sink.name },
          batch.events.length,
        );

        await database.userQB.wrap({ label: "delete_sink_delivery" }, (db) =>
          db
            .delete(PONDER_SINK_DELIVERY)
            .where(eq(PONDER_SINK_DELIVERY.id, delivery.id)),
        );

        common.logger.debug({
          msg: "Delivered sink batch",
          sink: sink.name,
          delivery_kind: delivery.kind,
          checkpoint: batch.checkpoint,
          batch_id: batch.id,
          event_count: batch.events.length,
        });

        common.metrics.ponder_sink_delivery_pending.set(
          { sink: sink.name },
          deliveries.length - index - 1,
        );
      }
    }
  };

  return {
    async start(): Promise<void> {
      if (sinks.length > 0 && database.userQB.$dialect === "pglite") {
        throw new NonRetryableUserError(
          "Analytics sinks require a Postgres database because PGlite does not provide durable transaction boundaries.",
        );
      }

      const initializedSinks: IndexingSink[] = [];

      try {
        for (const sink of sinks) {
          await sink.setup?.(getSetupContext(sink.name));
          initializedSinks.push(sink);
        }
      } catch (error) {
        await Promise.allSettled(
          initializedSinks.map((sink) => sink.shutdown?.()),
        );
        throw error;
      }

      common.shutdown.add(async () => {
        await Promise.all(
          sinks.map(async (sink) => {
            await sink.flush?.();
            await sink.shutdown?.();
          }),
        );
      });

      await drain();
    },
    async enqueue(tx: QB, events: Event[]): Promise<void> {
      if (sinks.length === 0 || events.length === 0) return;

      const batch = createFinalizedSinkBatch(events);
      await tx.wrap({ label: "enqueue_sink_delivery" }, (db) =>
        db
          .insert(PONDER_SINK_DELIVERY)
          .values(
            sinks.map((sink) => ({
              id: getDeliveryId({ sinkName: sink.name, batchId: batch.id }),
              sinkName: sink.name,
              kind: "finalized",
              checkpoint: batch.checkpoint,
              payload: superjson.stringify(batch),
              createdAt: Date.now(),
            })),
          )
          .onConflictDoNothing(),
      );
    },
    async enqueueLive(tx: QB, events: Event[]): Promise<void> {
      if (liveSinks.length === 0 || events.length === 0) return;

      const chain = getBatchChain(createSinkEvents(events));
      const checkpoint = events[0]!.checkpoint;
      for (const sink of liveSinks) {
        await enqueueLiveDelivery({
          tx,
          sink,
          kind: "live",
          chain,
          checkpoint,
          events,
        });
      }
    },
    async enqueueReorg(tx, { chain, checkpoint, events }): Promise<void> {
      if (liveSinks.length === 0 || events.length === 0) return;

      for (const sink of liveSinks) {
        await enqueueLiveDelivery({
          tx,
          sink,
          kind: "reorg",
          chain,
          checkpoint,
          events,
        });
      }
    },
    drain,
    hasLiveSinks: liveSinks.length > 0,
  };
};
