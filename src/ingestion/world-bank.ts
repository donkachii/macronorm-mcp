import "dotenv/config";
import fetch from "node-fetch";
import pkg from "pg";

const { Pool } = pkg;

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const WB_BASE_URL = "https://api.worldbank.org/v2";
const BATCH_SIZE = 50;
const REQUEST_DELAY_MS = 50;

async function fetchWorldBankIndicator(
  indicatorCode: string,
  countryIso3: string
): Promise<WBObservation[]> {
  const url = `${WB_BASE_URL}/country/${countryIso3}/indicator/${indicatorCode}?format=json&per_page=20&mrv=20`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`World Bank API error ${res.status} for ${indicatorCode}/${countryIso3}`);
  const data = (await res.json()) as [WBMeta, WBObservation[] | null];
  if (!data[1]) return [];
  return data[1].filter((o) => o.value !== null);
}

async function fetchActiveCountries(client: pkg.PoolClient): Promise<Country[]> {
  const result = await client.query(
    `SELECT iso3, wb_code FROM countries WHERE is_active = TRUE AND wb_code IS NOT NULL`
  );
  return result.rows;
}

async function fetchWBIndicatorMappings(client: pkg.PoolClient): Promise<WBMapping[]> {
  const result = await client.query(
    `SELECT ism.canonical_id, ism.source_code, i.frequency, i.unit
     FROM indicator_source_map ism
     JOIN indicators i ON i.canonical_id = ism.canonical_id
     WHERE ism.source = 'WORLD_BANK'`
  );
  return result.rows;
}

async function upsertObservations(
  client: pkg.PoolClient,
  countryIso3: string,
  mapping: WBMapping,
  observations: WBObservation[]
): Promise<number> {
  let count = 0;
  for (const obs of observations) {
    if (obs.value === null) continue;
    const period = formatPeriod(String(obs.date), mapping.frequency);
    await client.query(
      `INSERT INTO macro_data
         (country_iso3, canonical_id, period, frequency, value, unit, source, source_code, last_updated)
       VALUES ($1,$2,$3,$4,$5,$6,'WORLD_BANK',$7,NOW())
       ON CONFLICT (country_iso3, canonical_id, period, frequency, source)
       DO UPDATE SET
         value       = EXCLUDED.value,
         last_updated = EXCLUDED.last_updated,
         ingested_at = NOW()`,
      [
        countryIso3,
        mapping.canonical_id,
        period,
        mapping.frequency,
        obs.value,
        mapping.unit,
        mapping.source_code,
      ]
    );
    count++;
  }
  return count;
}

function formatPeriod(date: string, frequency: string): string {
  if (frequency === "monthly") return date.slice(0, 7);
  if (frequency === "quarterly") {
    const [year, quarter] = date.split("Q");
    return `${year.trim()}-Q${quarter}`;
  }
  return date.slice(0, 4);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function logIngestion(client: pkg.PoolClient, startedAt: Date): Promise<string> {
  const result = await client.query(
    `INSERT INTO ingestion_log (source, started_at, status)
     VALUES ('WORLD_BANK', $1, 'running')
     RETURNING id`,
    [startedAt]
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

export async function ingestWorldBank(): Promise<void> {
  const client = await pool.connect();
  const startedAt = new Date();
  let logId: string | null = null;
  let totalRecords = 0;
  let totalErrors = 0;

  try {
    logId = await logIngestion(client, startedAt);

    const countries = await fetchActiveCountries(client);
    const mappings = await fetchWBIndicatorMappings(client);

    console.log(`Ingesting ${mappings.length} indicators across ${countries.length} countries...`);

    for (const mapping of mappings) {
      console.log(`\nIndicator: ${mapping.canonical_id} (${mapping.source_code})`);
      let indicatorRecords = 0;

      for (let i = 0; i < countries.length; i += BATCH_SIZE) {
        const batch = countries.slice(i, i + BATCH_SIZE);

        await Promise.all(
          batch.map(async (country) => {
            try {
              const observations = await fetchWorldBankIndicator(
                mapping.source_code,
                country.wb_code
              );
              if (observations.length === 0) return;
              await client.query("BEGIN");
              const count = await upsertObservations(client, country.iso3, mapping, observations);
              await client.query("COMMIT");
              indicatorRecords += count;
              totalRecords += count;
            } catch {
              await client.query("ROLLBACK").catch(() => {});
              totalErrors++;
            }
          })
        );

        await sleep(REQUEST_DELAY_MS);
      }

      console.log(`  ${indicatorRecords} records written.`);
    }

    await completeIngestion(client, logId, totalRecords);

    await client.query(
      `UPDATE source_metadata SET last_ingested = NOW() WHERE source = 'WORLD_BANK'`
    );

    console.log(`\nWorld Bank ingestion complete.`);
    console.log(`Total records: ${totalRecords}`);
    console.log(`Total errors:  ${totalErrors}`);
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    if (logId) await failIngestion(client, logId, String(err));
    throw err;
  } finally {
    client.release();
  }
}

if (process.argv[1].endsWith("world-bank.ts")) {
  ingestWorldBank()
    .then(() => pool.end())
    .catch((err) => {
      console.error("World Bank ingestion failed:", err);
      pool.end();
      process.exit(1);
    });
}

interface Country {
  iso3: string;
  wb_code: string;
}

interface WBMapping {
  canonical_id: string;
  source_code: string;
  frequency: "annual" | "quarterly" | "monthly";
  unit: string;
}

interface WBMeta {
  page: number;
  pages: number;
  per_page: string;
  total: number;
}

interface WBObservation {
  date: string;
  value: number | null;
}