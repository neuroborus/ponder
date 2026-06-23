import { createHash } from "node:crypto";
import { createClient } from "@clickhouse/client";
import type {
  FinalizedSinkBatch,
  FinalizedSinkEvent,
  IndexingSink,
  SinkSetupContext,
} from "ponder";

const SCHEMA_VERSION = 1;
const DEFAULT_DATABASE = "default";
const DEFAULT_TABLE = "ponder_events";
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_RETRY_DELAY_MS = 250;
const MAX_RETRIES = 10;
const MAX_RETRY_DELAY_MS = 60_000;

const identifierRegex = /^[A-Za-z_][A-Za-z0-9_]*$/;
const projectIdRegex = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/;

type Operation = "create_table" | "write";

export type ClickHouseSinkConfig = {
  /** ClickHouse HTTP endpoint. */
  url: string;
  /** Stable identifier for this Ponder deployment. */
  projectId: string;
  /** Optional ClickHouse username. */
  username?: string;
  /** Optional ClickHouse password. Never written to logs. */
  password?: string;
  /** Client request timeout in milliseconds. */
  requestTimeout?: number;
  /** Number of retries after the initial failed request. Default: `3`. */
  maxRetries?: number;
  /** Initial retry delay in milliseconds. Default: `250`. */
  retryDelayMs?: number;
  /** Target table configuration. */
  schema?: {
    /** Existing ClickHouse database. Default: `"default"`. */
    database?: string;
    /** Event table name. Default: `"ponder_events"`. */
    table?: string;
    /** Create the event table if it does not exist. Default: `false`. */
    autoCreate?: boolean;
  };
};

type ResolvedClickHouseSinkConfig = {
  url: string;
  projectId: string;
  username: string | undefined;
  password: string | undefined;
  requestTimeout: number | undefined;
  maxRetries: number;
  retryDelayMs: number;
  database: string;
  table: string;
  autoCreate: boolean;
};

type ClickHouseEventRow = {
  schema_version: number;
  event_id: string;
  batch_id: string;
  project_id: string;
  chain_id: number;
  checkpoint: string;
  block_number: string;
  block_timestamp: number;
  transaction_hash: string | null;
  log_index: number | null;
  event_name: string;
  event_type: string;
  contract_name: string | null;
  contract_address: string | null;
  payload: string;
};

const getConfigError = (message: string): Error =>
  new Error(`Invalid ClickHouse sink config: ${message}`);

const validateIdentifier = ({
  value,
  name,
}: {
  value: unknown;
  name: string;
}): string => {
  if (typeof value !== "string" || !identifierRegex.test(value)) {
    throw getConfigError(
      `${name} must use letters, numbers, and underscores and cannot start with a number.`,
    );
  }

  return value;
};

const validateOptionalString = ({
  value,
  name,
}: {
  value: unknown;
  name: string;
}): string | undefined => {
  if (value !== undefined && typeof value !== "string") {
    throw getConfigError(`${name} must be a string.`);
  }

  return value;
};

const validateOptionalInteger = ({
  value,
  name,
  min,
  max,
}: {
  value: unknown;
  name: string;
  min: number;
  max: number;
}): number | undefined => {
  if (
    value !== undefined &&
    (typeof value !== "number" ||
      !Number.isInteger(value) ||
      value < min ||
      value > max)
  ) {
    throw getConfigError(`${name} must be an integer from ${min} to ${max}.`);
  }

  return value;
};

const resolveConfig = (
  config: ClickHouseSinkConfig,
): ResolvedClickHouseSinkConfig => {
  if (typeof config !== "object" || config === null) {
    throw getConfigError("configuration must be an object.");
  }

  if (typeof config.url !== "string" || config.url.trim() === "") {
    throw getConfigError("url must be a valid HTTP or HTTPS URL.");
  }

  let url: URL;
  try {
    url = new URL(config.url);
  } catch {
    throw getConfigError("url must be a valid HTTP or HTTPS URL.");
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw getConfigError("url must be a valid HTTP or HTTPS URL.");
  }
  if (url.pathname !== "/" || url.search !== "" || url.hash !== "") {
    throw getConfigError(
      "url must not include a path, query string, or fragment; use schema.database instead.",
    );
  }

  if (
    typeof config.projectId !== "string" ||
    !projectIdRegex.test(config.projectId)
  ) {
    throw getConfigError(
      "projectId must use letters, numbers, hyphens, and underscores.",
    );
  }

  const schema = config.schema ?? {};
  if (typeof schema !== "object" || schema === null || Array.isArray(schema)) {
    throw getConfigError("schema must be an object.");
  }

  if (
    schema.autoCreate !== undefined &&
    typeof schema.autoCreate !== "boolean"
  ) {
    throw getConfigError("schema.autoCreate must be a boolean.");
  }

  return {
    url: url.toString(),
    projectId: config.projectId,
    username: validateOptionalString({
      value: config.username,
      name: "username",
    }),
    password: validateOptionalString({
      value: config.password,
      name: "password",
    }),
    requestTimeout: validateOptionalInteger({
      value: config.requestTimeout,
      name: "requestTimeout",
      min: 1,
      max: Number.MAX_SAFE_INTEGER,
    }),
    maxRetries:
      validateOptionalInteger({
        value: config.maxRetries,
        name: "maxRetries",
        min: 0,
        max: MAX_RETRIES,
      }) ?? DEFAULT_MAX_RETRIES,
    retryDelayMs:
      validateOptionalInteger({
        value: config.retryDelayMs,
        name: "retryDelayMs",
        min: 0,
        max: MAX_RETRY_DELAY_MS,
      }) ?? DEFAULT_RETRY_DELAY_MS,
    database: validateIdentifier({
      value: schema.database ?? DEFAULT_DATABASE,
      name: "schema.database",
    }),
    table: validateIdentifier({
      value: schema.table ?? DEFAULT_TABLE,
      name: "schema.table",
    }),
    autoCreate: schema.autoCreate ?? false,
  };
};

const getEventName = (event: FinalizedSinkEvent): string => {
  const separator = event.type === "trace" ? "." : ":";
  const [, name] = event.name.split(separator, 2);
  return name ?? event.name;
};

const getContractName = (event: FinalizedSinkEvent): string | null => {
  if (event.type !== "log" && event.type !== "trace") return null;

  const separator = event.type === "trace" ? "." : ":";
  const [name] = event.name.split(separator, 1);
  return name === "" ? null : (name ?? null);
};

const getTransactionHash = (event: FinalizedSinkEvent): string | null => {
  if ("transaction" in event.event) return event.event.transaction.hash;
  return null;
};

const getLogIndex = (event: FinalizedSinkEvent): number | null => {
  if ("log" in event.event) return event.event.log.logIndex;
  return null;
};

const getContractAddress = (event: FinalizedSinkEvent): string | null => {
  if (event.type === "log" && "log" in event.event) {
    return event.event.log.address;
  }
  if (event.type === "trace" && "trace" in event.event) {
    return event.event.trace.to;
  }
  return null;
};

const stringifyPayload = (value: unknown): string =>
  JSON.stringify(value, (_key, nestedValue: unknown) =>
    typeof nestedValue === "bigint" ? nestedValue.toString() : nestedValue,
  );

const getEventId = (event: FinalizedSinkEvent): string =>
  createHash("sha256")
    .update(`${event.checkpoint}:${event.name}:${event.id}`)
    .digest("hex");

const mapEvent = ({
  event,
  batch,
  projectId,
}: {
  event: FinalizedSinkEvent;
  batch: FinalizedSinkBatch;
  projectId: string;
}): ClickHouseEventRow => ({
  schema_version: SCHEMA_VERSION,
  event_id: getEventId(event),
  batch_id: batch.id,
  project_id: projectId,
  chain_id: event.chain.id,
  checkpoint: event.checkpoint,
  block_number: event.event.block.number.toString(),
  block_timestamp: Number(event.event.block.timestamp),
  transaction_hash: getTransactionHash(event),
  log_index: getLogIndex(event),
  event_name: getEventName(event),
  event_type: event.type,
  contract_name: getContractName(event),
  contract_address: getContractAddress(event),
  payload: stringifyPayload(event.event),
});

const getCreateTableQuery = ({
  database,
  table,
}: Pick<ResolvedClickHouseSinkConfig, "database" | "table">): string => {
  const qualifiedTable = `${database}.${table}`;

  return `
CREATE TABLE IF NOT EXISTS ${qualifiedTable} (
  schema_version UInt8,
  event_id String,
  batch_id String,
  project_id String,
  chain_id UInt64,
  checkpoint String,
  block_number UInt64,
  block_timestamp DateTime,
  transaction_hash Nullable(String),
  log_index Nullable(UInt64),
  event_name String,
  event_type LowCardinality(String),
  contract_name Nullable(String),
  contract_address Nullable(String),
  payload String,
  inserted_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
ORDER BY (project_id, event_id)`;
};

const sleep = (duration: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, duration));

/**
 * Creates a finalized, at-least-once ClickHouse analytics sink.
 *
 * The target database must already exist. `schema.autoCreate` creates only the
 * event table and never changes or removes an existing table.
 */
export const createClickHouseSink = (
  config: ClickHouseSinkConfig,
): IndexingSink => {
  const resolved = resolveConfig(config);
  const client = createClient({
    application: "ponder-clickhouse",
    database: resolved.database,
    password: resolved.password,
    request_timeout: resolved.requestTimeout,
    url: resolved.url,
    username: resolved.username,
  });
  let context: SinkSetupContext | undefined;

  const withRetry = async ({
    operation,
    batch,
    execute,
  }: {
    operation: Operation;
    batch?: FinalizedSinkBatch;
    execute: () => Promise<void>;
  }): Promise<void> => {
    for (let attempt = 0; ; attempt++) {
      try {
        await execute();
        return;
      } catch (error) {
        if (attempt === resolved.maxRetries) {
          context?.logger.error({
            msg: "ClickHouse sink request failed",
            operation,
            batch_id: batch?.id,
            checkpoint: batch?.checkpoint,
            event_count: batch?.events.length,
            retry_count: attempt,
            error: error as Error,
          });
          throw error;
        }

        const delay = Math.min(
          resolved.retryDelayMs * 2 ** attempt,
          MAX_RETRY_DELAY_MS,
        );
        context?.metrics.recordRetry();
        context?.logger.warn({
          msg: "Retrying ClickHouse sink request",
          operation,
          batch_id: batch?.id,
          checkpoint: batch?.checkpoint,
          event_count: batch?.events.length,
          retry_count: attempt + 1,
          retry_delay: delay,
          error: error as Error,
        });
        await sleep(delay);
      }
    }
  };

  return {
    name: "clickhouse",
    async setup(setupContext): Promise<void> {
      context = setupContext;

      if (!resolved.autoCreate) return;

      try {
        await withRetry({
          operation: "create_table",
          execute: async () => {
            await client.command({
              query: getCreateTableQuery(resolved),
            });
          },
        });
      } catch (error) {
        await client.close().catch(() => {});
        throw error;
      }

      context.logger.info({
        msg: "Created ClickHouse analytics table if needed",
        database: resolved.database,
        table: resolved.table,
      });
    },
    async writeFinalizedBatch(batch): Promise<void> {
      if (batch.events.length === 0) return;

      await withRetry({
        operation: "write",
        batch,
        execute: async () => {
          await client.insert({
            table: resolved.table,
            format: "JSONEachRow",
            values: batch.events.map((event) =>
              mapEvent({ event, batch, projectId: resolved.projectId }),
            ),
          });
        },
      });

      context?.logger.debug({
        msg: "Wrote finalized ClickHouse batch",
        batch_id: batch.id,
        checkpoint: batch.checkpoint,
        event_count: batch.events.length,
      });
    },
    async shutdown(): Promise<void> {
      await client.close();
    },
  };
};
