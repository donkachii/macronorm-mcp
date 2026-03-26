import "dotenv/config";
import fetch from "node-fetch";
import pkg from "pg";

const { Pool } = pkg;

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const IMF_BASE_URL = "https://www.imf.org/external/datamapper/api/v1";
const REQUEST_DELAY_MS = 300;

const IMF_BILLION_SCALE_INDICATORS = new Set([
  "CURRENT_ACCOUNT_USD",
  "EXPORTS_USD",
  "IMPORTS_USD",
  "FDI_INFLOWS_USD",
  "FDI_NET_USD",
  "RESERVES_USD",
  "GOVT_DEBT_USD",
]);

async function fetchIMFIndicatorAllCountries(
  indicatorCode: string
): Promise<Record<string, Record<string, number | null>>> {
  const url = `${IMF_BASE_URL}/${indicatorCode}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`IMF API error ${res.status} for ${indicatorCode}`);
  const data = (await res.json()) as IMFResponse;
  return data?.values?.[indicatorCode] ?? {};
}

async function fetchCountryISO3Map(client: pkg.PoolClient): Promise<Map<string, string>> {
  const result = await client.query(
    `SELECT iso3, imf_code FROM countries WHERE is_active = TRUE AND imf_code IS NOT NULL`
  );
  const map = new Map<string, string>();
  for (const row of result.rows) {
    map.set(String(row.imf_code), row.iso3);
  }
  return map;
}

async function fetchIMFIndicatorMappings(client: pkg.PoolClient): Promise<IMFMapping[]> {
  const result = await client.query(
    `SELECT ism.canonical_id, ism.source_code, i.frequency, i.unit
     FROM indicator_source_map ism
     JOIN indicators i ON i.canonical_id = ism.canonical_id
     WHERE ism.source = 'IMF'`
  );
  return result.rows;
}

async function upsertCountryData(
  client: pkg.PoolClient,
  countryIso3: string,
  mapping: IMFMapping,
  yearValues: Record<string, number | null>
): Promise<number> {
  let count = 0;
  for (const [year, value] of Object.entries(yearValues)) {
    if (value === null) continue;
    const scaledValue = IMF_BILLION_SCALE_INDICATORS.has(mapping.canonical_id)
      ? value * 1_000_000_000
      : value;
    await client.query(
      `INSERT INTO macro_data
         (country_iso3, canonical_id, period, frequency, value, unit, source, source_code, last_updated)
       VALUES ($1,$2,$3,$4,$5,$6,'IMF',$7,NOW())
       ON CONFLICT (country_iso3, canonical_id, period, frequency, source)
       DO UPDATE SET
         value        = EXCLUDED.value,
         last_updated = EXCLUDED.last_updated,
         ingested_at  = NOW()`,
      [
        countryIso3,
        mapping.canonical_id,
        year,
        mapping.frequency,
        scaledValue,
        mapping.unit,
        mapping.source_code,
      ]
    );
    count++;
  }
  return count;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function logIngestion(client: pkg.PoolClient, startedAt: Date): Promise<string> {
  const result = await client.query(
    `INSERT INTO ingestion_log (source, started_at, status)
     VALUES ('IMF', $1, 'running')
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

export async function ingestIMF(): Promise<void> {
  const client = await pool.connect();
  const startedAt = new Date();
  let logId: string | null = null;
  let totalRecords = 0;
  let totalErrors = 0;

  try {
    logId = await logIngestion(client, startedAt);

    const imfCodeToIso3 = await fetchCountryISO3Map(client);
    const mappings = await fetchIMFIndicatorMappings(client);

    console.log(`Ingesting ${mappings.length} IMF indicators...`);

    for (const mapping of mappings) {
      console.log(`\nIndicator: ${mapping.canonical_id} (${mapping.source_code})`);
      let indicatorRecords = 0;

      try {
        const allCountryData = await fetchIMFIndicatorAllCountries(mapping.source_code);

        for (const [imfCode, yearValues] of Object.entries(allCountryData)) {
          const iso3 = imfCodeToIso3.get(imfCode);
          if (!iso3) continue;

          try {
            await client.query("BEGIN");
            const count = await upsertCountryData(client, iso3, mapping, yearValues);
            await client.query("COMMIT");
            indicatorRecords += count;
            totalRecords += count;
          } catch {
            await client.query("ROLLBACK").catch(() => {});
            totalErrors++;
          }
        }
      } catch (err) {
        console.warn(`  Skipping ${mapping.source_code}: ${err}`);
        totalErrors++;
      }

      console.log(`  ${indicatorRecords} records written.`);
      await sleep(REQUEST_DELAY_MS);
    }

    await completeIngestion(client, logId, totalRecords);

    await client.query(
      `UPDATE source_metadata SET last_ingested = NOW() WHERE source = 'IMF'`
    );

    console.log(`\nIMF ingestion complete.`);
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

if (process.argv[1].endsWith("imf.ts")) {
  ingestIMF()
    .then(() => pool.end())
    .catch((err) => {
      console.error("IMF ingestion failed:", err);
      pool.end();
      process.exit(1);
    });
}

interface IMFMapping {
  canonical_id: string;
  source_code: string;
  frequency: "annual" | "quarterly" | "monthly";
  unit: string;
}

interface IMFResponse {
  values?: Record<string, Record<string, Record<string, number | null>>>;
}