import { onchainTable } from "ponder";

export const block = onchainTable("block", (t) => ({
  number: t.bigint().primaryKey(),
  timestamp: t.bigint().notNull(),
}));
