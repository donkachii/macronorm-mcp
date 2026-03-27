import "dotenv/config";
import express from "express";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { ListToolsRequestSchema, CallToolRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import { createContextMiddleware } from "@ctxprotocol/sdk";
import { cacheGet, cacheSet, makeCacheKey } from "./cache.js";
import pkg from "pg";

const { Pool } = pkg;

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const TOOLS = [
  {
    name: "get_indicator",
    description:
      "Get a macroeconomic indicator for a specific country. Returns normalized data from the best available source (IMF, World Bank, FRED, BIS). Use this for single-country lookups of GDP, inflation, unemployment, debt, trade, FX, and more.",
    _meta: {
      surface: "both",
      queryEligible: true,
      latencyClass: "instant",
      pricing: { executeUsd: "0.001" },
      rateLimit: {
        maxRequestsPerMinute: 300,
        cooldownMs: 0,
        maxConcurrency: 20,
        notes: "Served entirely from local Postgres cache. No upstream rate limits.",
      },
    },
    inputSchema: {
      type: "object",
      properties: {
        indicator: {
          type: "string",
          description: "Canonical indicator ID.",
          examples: [
            "GDP_CURRENT_USD",
            "CPI_ANNUAL_PCT",
            "UNEMPLOYMENT_PCT",
            "GOVT_DEBT_GDP",
            "CURRENT_ACCOUNT_GDP",
            "FX_RATE_USD",
            "POPULATION",
            "POLICY_RATE_PCT",
          ],
        },
        country_iso3: {
          type: "string",
          description: "ISO 3-letter country code.",
          examples: ["NGA", "USA", "DEU", "CHN", "BRA", "ZAF", "GBR", "IND"],
        },
        frequency: {
          type: "string",
          enum: ["annual", "quarterly", "monthly"],
          default: "annual",
          description: "Data frequency.",
        },
        limit: {
          type: "number",
          default: 10,
          description: "Number of periods to return, most recent first.",
        },
      },
      required: ["indicator", "country_iso3"],
    },
    outputSchema: {
      type: "object",
      properties: {
        country_iso3:   { type: "string", description: "ISO 3-letter country code" },
        country_name:   { type: "string", description: "Country display name" },
        canonical_id:   { type: "string", description: "Canonical indicator ID" },
        indicator_name: { type: "string", description: "Indicator display name" },
        frequency:      { type: "string", description: "Data frequency" },
        unit:           { type: "string", description: "Unit of measurement" },
        data: {
          type: "array",
          description: "Array of data points, most recent first",
          items: {
            type: "object",
            properties: {
              period: { type: "string", description: "Time period (e.g. 2023, 2023-Q1, 2023-01)" },
              value:  { type: "number", description: "Indicator value" },
              source: { type: "string", description: "Data source (IMF, WORLD_BANK, FRED, BIS)" },
            },
          },
        },
        fetched_at: { type: "string", description: "ISO 8601 timestamp of response" },
      },
      required: ["country_iso3", "canonical_id", "data", "fetched_at"],
    },
  },
  {
    name: "compare_indicator",
    description:
      "Compare a macroeconomic indicator across multiple countries for the same period. Ideal for cross-country analysis, peer comparisons, and regional benchmarking. Returns the most recent available value per country.",
    _meta: {
      surface: "both",
      queryEligible: true,
      latencyClass: "instant",
      pricing: { executeUsd: "0.001" },
      rateLimit: {
        maxRequestsPerMinute: 200,
        cooldownMs: 0,
        maxConcurrency: 10,
        notes: "Served from local Postgres cache. No upstream rate limits.",
      },
    },
    inputSchema: {
      type: "object",
      properties: {
        indicator: {
          type: "string",
          description: "Canonical indicator ID.",
          examples: ["GDP_GROWTH_PCT", "GOVT_DEBT_GDP", "CPI_ANNUAL_PCT", "UNEMPLOYMENT_PCT"],
        },
        countries: {
          type: "array",
          items: { type: "string" },
          description: "List of ISO 3-letter country codes to compare.",
          examples: [["NGA", "ZAF", "KEN", "GHA"], ["USA", "GBR", "DEU", "FRA", "JPN"]],
        },
        frequency: {
          type: "string",
          enum: ["annual", "quarterly", "monthly"],
          default: "annual",
        },
        period: {
          type: "string",
          description: "Specific period to compare (e.g. '2023'). If omitted, returns most recent available per country.",
          examples: ["2023", "2022", "2023-Q1", "2023-06"],
        },
      },
      required: ["indicator", "countries"],
    },
    outputSchema: {
      type: "object",
      properties: {
        canonical_id:   { type: "string", description: "Canonical indicator ID" },
        indicator_name: { type: "string", description: "Indicator display name" },
        frequency:      { type: "string", description: "Data frequency" },
        unit:           { type: "string", description: "Unit of measurement" },
        results: {
          type: "array",
          description: "One entry per country",
          items: {
            type: "object",
            properties: {
              country_iso3:       { type: "string" },
              country_name:       { type: "string" },
              period:             { type: "string" },
              value:              { type: "number" },
              source:             { type: "string" },
              resolved_frequency: { type: "string", description: "Set when frequency was aggregated from a higher-frequency source" },
            },
          },
        },
        fetched_at: { type: "string" },
      },
      required: ["canonical_id", "results", "fetched_at"],
    },
  },
  {
    name: "list_indicators",
    description:
      "List all available canonical indicators with their categories, units, and frequencies. Use this to discover what data MacroNorm provides before querying.",
    _meta: {
      surface: "both",
      queryEligible: true,
      latencyClass: "instant",
      pricing: { executeUsd: "0.0005" },
      rateLimit: { maxRequestsPerMinute: 60, cooldownMs: 0, maxConcurrency: 5 },
    },
    inputSchema: {
      type: "object",
      properties: {
        category: {
          type: "string",
          description: "Filter by category.",
          enum: ["output", "prices", "labor", "external", "fiscal", "monetary", "demographic", "financial"],
        },
      },
    },
    outputSchema: {
      type: "object",
      properties: {
        indicators: {
          type: "array",
          items: {
            type: "object",
            properties: {
              canonical_id:  { type: "string" },
              name:          { type: "string" },
              category:      { type: "string" },
              unit:          { type: "string" },
              frequency:     { type: "string" },
              description:   { type: "string" },
              source_count:  { type: "number", description: "Number of sources providing this indicator" },
            },
          },
        },
        total: { type: "number" },
      },
      required: ["indicators", "total"],
    },
  },
  {
    name: "list_countries",
    description:
      "List all countries available in MacroNorm with their ISO codes. Use this to find the correct ISO3 code for a country before querying data.",
    _meta: {
      surface: "both",
      queryEligible: true,
      latencyClass: "instant",
      pricing: { executeUsd: "0.0005" },
      rateLimit: { maxRequestsPerMinute: 60, cooldownMs: 0, maxConcurrency: 5 },
    },
    inputSchema: {
      type: "object",
      properties: {
        search: {
          type: "string",
          description: "Search by country name (partial match).",
          examples: ["Nigeria", "Germany", "United"],
        },
      },
    },
    outputSchema: {
      type: "object",
      properties: {
        countries: {
          type: "array",
          items: {
            type: "object",
            properties: {
              iso3:     { type: "string", description: "ISO 3-letter code" },
              iso2:     { type: "string", description: "ISO 2-letter code" },
              name:     { type: "string", description: "Country name" },
              imf_code: { type: "number", description: "IMF numeric code" },
            },
          },
        },
        total: { type: "number" },
      },
      required: ["countries", "total"],
    },
  },
];

const FREQUENCY_FALLBACKS: Record<string, string[]> = {
  annual:    ["annual", "quarterly", "monthly"],
  quarterly: ["quarterly", "monthly"],
  monthly:   ["monthly"],
};

function formatSource(source: string, sourceDatabase: string | null): string {
  if (!sourceDatabase) return source;
  return `${source}_${sourceDatabase}`;
}

async function queryWithFallback(
  country_iso3: string,
  indicator: string,
  frequency: string,
  limit: number
): Promise<{ rows: DbRow[]; resolvedFrequency: string }> {
  const fallbacks = FREQUENCY_FALLBACKS[frequency] ?? [frequency];

  for (const freq of fallbacks) {
    if (freq === frequency) {
      const result = await pool.query(
        `SELECT md.period, md.value, md.source, md.source_code, md.unit,
                ism.source_database
         FROM macro_data md
         LEFT JOIN indicator_source_map ism
           ON ism.canonical_id = md.canonical_id
          AND ism.source       = md.source
          AND ism.source_code  = md.source_code
         WHERE md.country_iso3 = $1
           AND md.canonical_id = $2
           AND md.frequency    = $3
         ORDER BY md.period DESC
         LIMIT $4`,
        [country_iso3, indicator, freq, limit]
      );
      if (result.rows.length > 0) return { rows: result.rows, resolvedFrequency: freq };
    } else if (freq === "quarterly" && frequency === "annual") {
      const result = await pool.query(
        `SELECT
           LEFT(md.period, 4)     AS period,
           AVG(md.value)          AS value,
           MAX(md.source)         AS source,
           MAX(md.source_code)    AS source_code,
           MAX(md.unit)           AS unit,
           MAX(ism.source_database) AS source_database
         FROM macro_data md
         LEFT JOIN indicator_source_map ism
           ON ism.canonical_id = md.canonical_id
          AND ism.source       = md.source
          AND ism.source_code  = md.source_code
         WHERE md.country_iso3 = $1
           AND md.canonical_id = $2
           AND md.frequency    = 'quarterly'
         GROUP BY LEFT(md.period, 4)
         ORDER BY period DESC
         LIMIT $3`,
        [country_iso3, indicator, limit]
      );
      if (result.rows.length > 0) return { rows: result.rows, resolvedFrequency: "annual (aggregated from quarterly)" };
    } else if (freq === "monthly" && frequency === "annual") {
      const result = await pool.query(
        `SELECT
           LEFT(md.period, 4)     AS period,
           AVG(md.value)          AS value,
           MAX(md.source)         AS source,
           MAX(md.source_code)    AS source_code,
           MAX(md.unit)           AS unit,
           MAX(ism.source_database) AS source_database
         FROM macro_data md
         LEFT JOIN indicator_source_map ism
           ON ism.canonical_id = md.canonical_id
          AND ism.source       = md.source
          AND ism.source_code  = md.source_code
         WHERE md.country_iso3 = $1
           AND md.canonical_id = $2
           AND md.frequency    = 'monthly'
         GROUP BY LEFT(md.period, 4)
         ORDER BY period DESC
         LIMIT $3`,
        [country_iso3, indicator, limit]
      );
      if (result.rows.length > 0) return { rows: result.rows, resolvedFrequency: "annual (aggregated from monthly)" };
    } else if (freq === "monthly" && frequency === "quarterly") {
      const result = await pool.query(
        `SELECT
           LEFT(md.period, 4) || '-Q' || CEIL(EXTRACT(MONTH FROM TO_DATE(md.period, 'YYYY-MM')) / 3.0)::int AS period,
           AVG(md.value)            AS value,
           MAX(md.source)           AS source,
           MAX(md.source_code)      AS source_code,
           MAX(md.unit)             AS unit,
           MAX(ism.source_database) AS source_database
         FROM macro_data md
         LEFT JOIN indicator_source_map ism
           ON ism.canonical_id = md.canonical_id
          AND ism.source       = md.source
          AND ism.source_code  = md.source_code
         WHERE md.country_iso3 = $1
           AND md.canonical_id = $2
           AND md.frequency    = 'monthly'
         GROUP BY LEFT(md.period, 4) || '-Q' || CEIL(EXTRACT(MONTH FROM TO_DATE(md.period, 'YYYY-MM')) / 3.0)::int
         ORDER BY period DESC
         LIMIT $3`,
        [country_iso3, indicator, limit]
      );
      if (result.rows.length > 0) return { rows: result.rows, resolvedFrequency: "quarterly (aggregated from monthly)" };
    }
  }

  return { rows: [], resolvedFrequency: frequency };
}

async function handleGetIndicator(args: Record<string, unknown>) {
  const indicator    = args.indicator    as string | undefined;
  const country_iso3 = args.country_iso3 as string | undefined;
  const frequency    = (args.frequency  as string) ?? "annual";
  const limit        = (args.limit      as number) ?? 10;

  if (!indicator || !country_iso3) {
    return {
      country_iso3:   country_iso3 ?? null,
      country_name:   null,
      canonical_id:   indicator   ?? null,
      indicator_name: null,
      frequency,
      unit:           null,
      data:           [],
      fetched_at:     new Date().toISOString(),
    };
  }

  const cacheKey = makeCacheKey("get_indicator", indicator, country_iso3, frequency, limit);
  const cached = await cacheGet(cacheKey);
  if (cached) return cached;

  const metaResult = await pool.query(
    `SELECT c.name as country_name, i.name as indicator_name, i.unit
     FROM countries c, indicators i
     WHERE c.iso3         = $1
       AND i.canonical_id = $2`,
    [country_iso3.toUpperCase(), indicator]
  );
  const meta = metaResult.rows[0] ?? {};

  const { rows, resolvedFrequency } = await queryWithFallback(
    country_iso3.toUpperCase(), indicator, frequency, limit
  );

  const result = {
    country_iso3:   country_iso3.toUpperCase(),
    country_name:   meta.country_name   ?? null,
    canonical_id:   indicator,
    indicator_name: meta.indicator_name ?? null,
    frequency:      resolvedFrequency,
    unit:           meta.unit           ?? null,
    data: rows.map((r) => ({
      period: r.period,
      value:  parseFloat(String(r.value)),
      source: formatSource(r.source, r.source_database),
    })),
    fetched_at: new Date().toISOString(),
  };

  await cacheSet(cacheKey, result);
  return result;
}

async function handleCompareIndicator(args: Record<string, unknown>) {
  const indicator = args.indicator as string | undefined;
  const countries = args.countries as string[] | undefined;
  const frequency = (args.frequency as string) ?? "annual";
  const period    = args.period as string | undefined;

  if (!indicator || !countries?.length) {
    return {
      canonical_id:   indicator ?? null,
      indicator_name: null,
      frequency,
      unit:           null,
      results:        [],
      fetched_at:     new Date().toISOString(),
    };
  }

  const upperCountries = countries.map((c) => c.toUpperCase());
  const cacheKey = makeCacheKey("compare_indicator", indicator, upperCountries.join("+"), frequency, period);
  const cached = await cacheGet(cacheKey);
  if (cached) return cached;

  const indMeta = await pool.query(
    `SELECT name, unit FROM indicators WHERE canonical_id = $1`,
    [indicator]
  );
  const meta = indMeta.rows[0] ?? {};

  const results: CompareResult[] = [];

  for (const iso3 of upperCountries) {
    if (period) {
      const r = await pool.query(
        `SELECT md.country_iso3, c.name as country_name, md.period, md.value,
                md.source, ism.source_database
         FROM macro_data md
         JOIN countries c ON c.iso3 = md.country_iso3
         LEFT JOIN indicator_source_map ism
           ON ism.canonical_id = md.canonical_id
          AND ism.source       = md.source
          AND ism.source_code  = md.source_code
         WHERE md.canonical_id = $1
           AND md.frequency    = $2
           AND md.period       = $3
           AND md.country_iso3 = $4
         ORDER BY md.period DESC
         LIMIT 1`,
        [indicator, frequency, period, iso3]
      );
      if (r.rows.length > 0) {
        results.push({
          country_iso3: r.rows[0].country_iso3,
          country_name: r.rows[0].country_name,
          period:       r.rows[0].period,
          value:        parseFloat(r.rows[0].value),
          source:       formatSource(r.rows[0].source, r.rows[0].source_database ?? null),
        });
        continue;
      }
    }

    const { rows, resolvedFrequency } = await queryWithFallback(iso3, indicator, frequency, 1);
    if (rows.length > 0) {
      const countryMeta = await pool.query(
        `SELECT name FROM countries WHERE iso3 = $1`, [iso3]
      );
      results.push({
        country_iso3:       iso3,
        country_name:       countryMeta.rows[0]?.name ?? iso3,
        period:             rows[0].period,
        value:              parseFloat(String(rows[0].value)),
        source:             rows[0].source,
        resolved_frequency: resolvedFrequency !== frequency ? resolvedFrequency : undefined,
      });
    }
  }

  results.sort((a, b) => (b.value ?? 0) - (a.value ?? 0));

  const result = {
    canonical_id:   indicator,
    indicator_name: meta.name ?? null,
    frequency,
    unit:           meta.unit ?? null,
    results,
    fetched_at: new Date().toISOString(),
  };

  await cacheSet(cacheKey, result);
  return result;
}

async function handleListIndicators(args: Record<string, unknown>) {
  const category = args.category as string | undefined;
  const cacheKey = makeCacheKey("list_indicators", category);
  const cached   = await cacheGet(cacheKey);
  if (cached) return cached;

  const result = await pool.query(
    `SELECT i.canonical_id, i.name, i.category, i.unit, i.frequency, i.description,
            COUNT(ism.source) as source_count
     FROM indicators i
     LEFT JOIN indicator_source_map ism ON ism.canonical_id = i.canonical_id
     ${category ? "WHERE i.category = $1" : ""}
     GROUP BY i.canonical_id, i.name, i.category, i.unit, i.frequency, i.description
     ORDER BY i.category, i.canonical_id`,
    category ? [category] : []
  );

  const data = {
    indicators: result.rows.map((r) => ({
      canonical_id: r.canonical_id,
      name:         r.name,
      category:     r.category,
      unit:         r.unit,
      frequency:    r.frequency,
      description:  r.description,
      source_count: parseInt(r.source_count),
    })),
    total: result.rows.length,
  };

  await cacheSet(cacheKey, data);
  return data;
}

async function handleListCountries(args: Record<string, unknown>) {
  const search   = args.search as string | undefined;
  const cacheKey = makeCacheKey("list_countries", search);
  const cached   = await cacheGet(cacheKey);
  if (cached) return cached;

  const result = await pool.query(
    `SELECT iso3, iso2, name, imf_code
     FROM countries
     WHERE is_active = TRUE
     ${search ? "AND name ILIKE $1" : ""}
     ORDER BY name`,
    search ? [`%${search}%`] : []
  );

  const data = {
    countries: result.rows,
    total:     result.rows.length,
  };

  await cacheSet(cacheKey, data);
  return data;
}

function createServer() {
  const mcpServer = new Server(
    { name: "macronorm", version: "1.0.0" },
    { capabilities: { tools: {} } }
  );

  mcpServer.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: TOOLS,
  }));

  mcpServer.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args = {} } = request.params;
    console.log(`Tool called: ${name}`);
    let data: unknown;

    switch (name) {
      case "get_indicator":
        data = await handleGetIndicator(args as Record<string, unknown>);
        break;
      case "compare_indicator":
        data = await handleCompareIndicator(args as Record<string, unknown>);
        break;
      case "list_indicators":
        data = await handleListIndicators(args as Record<string, unknown>);
        break;
      case "list_countries":
        data = await handleListCountries(args as Record<string, unknown>);
        break;
      default:
        return {
          content: [{ type: "text", text: `Unknown tool: ${name}` }],
          isError: true,
        };
    }

    return {
      content: [{ type: "text", text: JSON.stringify(data) }],
      structuredContent: data,
    };
  });

  return mcpServer;
}

const app = express();
app.use(express.json());
app.use("/mcp", createContextMiddleware());

app.post("/mcp", async (req, res) => {
  const mcpServer = createServer();
  const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
  await mcpServer.connect(transport);
  await transport.handleRequest(req, res, req.body);
});

app.get("/health", (_req, res) => {
  res.json({ status: "ok", service: "macronorm", version: "1.0.0" });
});

const PORT = process.env.PORT ?? 3000;
app.listen(PORT, () => {
  console.log(`MacroNorm MCP server running on port ${PORT}`);
});

process.on("uncaughtException", (err) => {
  console.error("Uncaught exception:", err);
});

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled rejection:", reason);
});

setInterval(() => {}, 1 << 30);

interface DbRow {
  period:          string;
  value:           string | number;
  source:          string;
  source_code:     string;
  source_database: string | null;
  unit:            string;
}

interface CompareResult {
  country_iso3:        string;
  country_name:        string;
  period:              string;
  value:               number;
  source:              string;
  resolved_frequency?: string;
}