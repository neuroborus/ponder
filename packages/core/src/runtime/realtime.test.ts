import { ZERO_CHECKPOINT, encodeCheckpoint } from "@/utils/checkpoint.js";
import { expect, test } from "vitest";
import { getFinalizedEventsMultichain, getReorgedEvents } from "./realtime.js";

const createCheckpoint = ({
  chainId,
  blockNumber,
  blockTimestamp,
}: {
  chainId: bigint;
  blockNumber: bigint;
  blockTimestamp: bigint;
}) =>
  encodeCheckpoint({
    ...ZERO_CHECKPOINT,
    chainId,
    blockNumber,
    blockTimestamp,
  });

test("getFinalizedEventsMultichain() does not wait for another chain", () => {
  const chainA = { id: 1 };
  const chainB = { id: 2 };
  const eventA = {
    id: "a",
    chain: chainA,
    checkpoint: createCheckpoint({
      chainId: 1n,
      blockNumber: 1n,
      blockTimestamp: 1n,
    }),
  };
  const eventB = {
    id: "b",
    chain: chainB,
    checkpoint: createCheckpoint({
      chainId: 2n,
      blockNumber: 1n,
      blockTimestamp: 2n,
    }),
  };

  const result = getFinalizedEventsMultichain([eventA, eventB], {
    chain: chainB,
    checkpoint: eventB.checkpoint,
  });

  expect(result.finalizedEvents).toStrictEqual([eventB]);
  expect(result.remainingEvents).toStrictEqual([eventA]);
});

test("getReorgedEvents() only removes events from the reorged chain", () => {
  const chainA = { id: 1 };
  const chainB = { id: 2 };
  const eventA = {
    chain: chainA,
    checkpoint: createCheckpoint({
      chainId: 1n,
      blockNumber: 2n,
      blockTimestamp: 1n,
    }),
    event: { block: { number: 2n } },
  };
  const eventB = {
    chain: chainB,
    checkpoint: createCheckpoint({
      chainId: 2n,
      blockNumber: 2n,
      blockTimestamp: 2n,
    }),
    event: { block: { number: 2n } },
  };

  const result = getReorgedEvents([eventA, eventB], {
    chain: chainB,
    blockNumber: 1n,
  });

  expect(result.reorgedEvents).toStrictEqual([eventB]);
  expect(result.remainingEvents).toStrictEqual([eventA]);
});
