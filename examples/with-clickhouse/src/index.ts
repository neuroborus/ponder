import { ponder } from "ponder:registry";
import { block } from "ponder:schema";

ponder.on("Mainnet:block", async ({ event, context }) => {
  await context.db
    .insert(block)
    .values({
      number: event.block.number,
      timestamp: event.block.timestamp,
    })
    .onConflictDoNothing();
});
