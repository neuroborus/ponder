import { createClickHouseSink } from "@ponder/clickhouse";
import { createConfig } from "ponder";

const databaseUrl = process.env.DATABASE_URL;
const clickhouseUrl = process.env.CLICKHOUSE_URL;

if (databaseUrl === undefined || clickhouseUrl === undefined) {
  throw new Error("DATABASE_URL and CLICKHOUSE_URL are required");
}

export default createConfig({
  database: {
    kind: "postgres",
    connectionString: databaseUrl,
  },
  chains: {
    mainnet: {
      id: 1,
      rpc: process.env.PONDER_RPC_URL_1,
    },
  },
  blocks: {
    Mainnet: {
      chain: "mainnet",
      startBlock: "latest",
      interval: 1,
    },
  },
  sinks: [
    createClickHouseSink({
      url: clickhouseUrl,
      projectId: "with-clickhouse",
      schema: { autoCreate: true },
    }),
  ],
});
