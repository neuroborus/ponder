import type { FinalizedSinkBatch, LiveSinkBatch, ReorgSinkBatch } from "ponder";

export const createBatch = ({
  batchId = "batch-1",
  checkpoint = "0x0000000000000001",
  eventId = "event-1",
}: {
  batchId?: string;
  checkpoint?: string;
  eventId?: string;
} = {}): FinalizedSinkBatch => ({
  version: 1,
  id: batchId,
  checkpoint,
  events: [
    {
      id: eventId,
      checkpoint,
      chain: { id: 1, name: "mainnet" },
      name: "Token:Transfer",
      type: "log",
      event: {
        id: eventId,
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
});

export const batch = createBatch();

export const createLiveBatch = ({
  batchId = "live-batch-1",
  checkpoint = "0x0000000000000001",
  eventId = "event-1",
  sequence = 1n,
}: {
  batchId?: string;
  checkpoint?: string;
  eventId?: string;
  sequence?: bigint;
} = {}): LiveSinkBatch => {
  const finalizedBatch = createBatch({ batchId, checkpoint, eventId });

  return {
    ...finalizedBatch,
    chain: finalizedBatch.events[0]!.chain,
    sequence,
  };
};

export const createReorgBatch = ({
  batchId = "reorg-batch-1",
  checkpoint = "0x0000000000000001",
  eventId = "event-1",
  sequence = 2n,
}: {
  batchId?: string;
  checkpoint?: string;
  eventId?: string;
  sequence?: bigint;
} = {}): ReorgSinkBatch => {
  const liveBatch = createLiveBatch({
    batchId,
    checkpoint,
    eventId,
    sequence,
  });

  return liveBatch;
};
