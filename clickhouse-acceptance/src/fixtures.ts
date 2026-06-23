import { type Database, getPonderSinkDeliveryTable } from "@/database/index.js";
import type { Chain, Event } from "@/internal/types.js";
import { ZERO_CHECKPOINT, encodeCheckpoint } from "@/utils/checkpoint.js";

export const namespace = { schema: "public", viewsSchema: undefined };

const testChain = {
  name: "mainnet",
  id: 1,
  rpc: "http://127.0.0.1:1",
  ws: undefined,
  pollingInterval: 1_000,
  finalityBlockCount: 1,
  disableCache: false,
  ethGetLogsBlockRange: undefined,
  viemChain: undefined,
} satisfies Chain;

export const createBlockEvent = ({
  id = "event-1",
  chain = testChain,
  checkpoint = encodeCheckpoint({
    ...ZERO_CHECKPOINT,
    chainId: BigInt(chain.id),
    blockNumber: 1n,
    blockTimestamp: 1n,
  }),
}: {
  id?: string;
  chain?: Chain;
  checkpoint?: string;
} = {}): Event => ({
  type: "block",
  checkpoint,
  chain,
  eventCallback: {
    filter: {
      type: "block",
      chainId: chain.id,
      sourceId: "Block",
      interval: 1,
      offset: 0,
      fromBlock: undefined,
      toBlock: undefined,
      hasTransactionReceipt: false,
      include: [],
    },
    name: "Block",
    fn: () => {},
    chain,
    type: "block",
  },
  event: {
    id,
    block: { hash: "0x", number: 1n, timestamp: 1n },
  },
});

export const getPendingDeliveries = (database: Database) => {
  const PONDER_SINK_DELIVERY = getPonderSinkDeliveryTable(namespace.schema);

  return database.userQB.wrap((db) => db.select().from(PONDER_SINK_DELIVERY));
};
