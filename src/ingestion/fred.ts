import "dotenv/config";
import fetch from "node-fetch";
import pkg from "pg";

const { Pool } = pkg;

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const FRED_API_KEY = process.env.FRED_API_KEY!;
const FRED_BASE_URL = "https://api.stlouisfed.org/fred";

const FRED_SERIES: FredSeries[] = [
  { series_id: "CPIAUCSL",  canonical_id: "CPI_MONTHLY_PCT",      frequency: "monthly",   unit: "percent" },
  { series_id: "UNRATE",    canonical_id: "UNEMPLOYMENT_MONTHLY", frequency: "monthly",   unit: "percent" },
  { series_id: "FEDFUNDS",  canonical_id: "POLICY_RATE_PCT",      frequency: "monthly",   unit: "percent" },
  { series_id: "M2SL",      canonical_id: "MONEY_SUPPLY_M2_USD",  frequency: "monthly",   unit: "USD"     },
  { series_id: "DEXUSEU",   canonical_id: "FX_RATE_MONTHLY",      frequency: "monthly",   unit: "LCU"     },
];

async function fetchFredSeries(seriesId: string): Promise<FredObservation[]> {
  const url = `${FRED_BASE_URL}/series/observations?series_id=${seriesId}&api_key=${FRED_API_KEY}&file_type=json&sort_order=desc&limit=60`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`FRED API error for ${seriesId}: ${res.status}`);
  const data = (await res.json()) as FredResponse;
  return data.observations.filter((o) => o.value !== ".");
}

async function logIngestion(
  client: pkg.PoolClient,
  source: string,
  startedAt: Date
): Promise<string> {
  const result = await client.query(
    `INSERT INTO ingestion_log (source, started_at, status)
     VALUES ($1, $2, 'running')
     RETURNING id`,
    [source, startedAt]
  );
  return result.rows[0].id;
}

async function completeIngestion(
  client: pkg.PoolClient,
  logId: string,
  recordsWritten: number
): Promise<void> {
  await client.query(
    `UPDATE ingestion_log
     SET status = 'success', completed_at = NOW(), records_written = $1
     WHERE id = $2`,
    [recordsWritten, logId]
  );
}

async function failIngestion(
  client: pkg.PoolClient,
  logId: string,
  errorMessage: string
): Promise<void> {
  await client.query(
    `UPDATE ingestion_log
     SET status = 'failed', completed_at = NOW(), error_message = $1
     WHERE id = $2`,
    [errorMessage, logId]
  );
}

async function upsertObservations(
  client: pkg.PoolClient,
  series: FredSeries,
  observations: FredObservation[]
): Promise<number> {
  let count = 0;
  for (const obs of observations) {
    const period = formatPeriod(obs.date, series.frequency);
    await client.query(
      `INSERT INTO macro_data
         (country_iso3, canonical_id, period, frequency, value, unit, source, source_code, last_updated)
       VALUES ($1,$2,$3,$4,$5,$6,'FRED',$7,$8)
       ON CONFLICT (country_iso3, canonical_id, period, frequency, source)
       DO UPDATE SET
         value        = EXCLUDED.value,
         last_updated = EXCLUDED.last_updated,
         ingested_at  = NOW()`,
      [
        "USA",
        series.canonical_id,
        period,
        series.frequency,
        parseFloat(obs.value),
        series.unit,
        series.series_id,
        new Date(obs.date),
      ]
    );
    count++;
  }
  return count;
}

function formatPeriod(date: string, frequency: string): string {
  if (frequency === "monthly") return date.slice(0, 7);
  if (frequency === "quarterly") {
    const [year, month] = date.split("-").map(Number);
    const quarter = Math.ceil(month / 3);
    return `${year}-Q${quarter}`;
  }
  return date.slice(0, 4);
}

export async function ingestFred(): Promise<void> {
  const client = await pool.connect();
  const startedAt = new Date();
  let logId: string | null = null;
  let totalRecords = 0;

  try {
    logId = await logIngestion(client, "FRED", startedAt);

    for (const series of FRED_SERIES) {
      console.log(`Fetching FRED series ${series.series_id}...`);
      const observations = await fetchFredSeries(series.series_id);
      await client.query("BEGIN");
      const count = await upsertObservations(client, series, observations);
      await client.query("COMMIT");
      totalRecords += count;
      console.log(`  ${series.series_id}: ${count} records written.`);
    }

    await completeIngestion(client, logId, totalRecords);

    await client.query(
      `UPDATE source_metadata SET last_ingested = NOW() WHERE source = 'FRED'`
    );

    console.log(`FRED ingestion complete. Total records: ${totalRecords}`);
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    if (logId) await failIngestion(client, logId, String(err));
    throw err;
  } finally {
    client.release();
  }
}

if (process.argv[1].endsWith("fred.ts")) {
  ingestFred()
    .then(() => pool.end())
    .catch((err) => {
      console.error("FRED ingestion failed:", err);
      pool.end();
      process.exit(1);
    });
}

interface FredSeries {
  series_id: string;
  canonical_id: string;
  frequency: "annual" | "quarterly" | "monthly";
  unit: string;
}

interface FredObservation {
  date: string;
  value: string;
}

interface FredResponse {
  observations: FredObservation[];
}