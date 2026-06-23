export const createClickHouseServer = (url: string) => {
  const execute = async (query: string): Promise<string> => {
    const response = await fetch(url, { method: "POST", body: query });
    const body = await response.text();

    if (!response.ok) {
      throw new Error(`ClickHouse request failed: ${body}`);
    }

    return body;
  };

  const query = async <row>(sql: string): Promise<row[]> => {
    const body = await execute(`${sql} FORMAT JSONEachRow`);
    if (body.trim() === "") return [];

    return body
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as row);
  };

  const getEventCount = async (qualifiedTable: string): Promise<number> => {
    const rows = await query<{ count: string | number }>(
      `SELECT count() AS count FROM ${qualifiedTable} FINAL`,
    );

    return Number(rows[0]?.count ?? 0);
  };

  return { execute, query, getEventCount };
};
