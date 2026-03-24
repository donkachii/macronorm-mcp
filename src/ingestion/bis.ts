import "dotenv/config";
import fetch from "node-fetch";
import pkg from "pg";

const { Pool } = pkg;

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const BIS_BASE_URL = "https://stats.bis.org/api/v2/data/dataflow/BIS";
const REQUEST_DELAY_MS = 500;

const BIS_DATASETS: BISDataset[] = [
  {
    canonical_id:   "HOUSE_PRICE_INDEX",
    source_code:    "PROP",
    dataflow:       "WS_SPP",
    version:        "1.0",
    key:            "Q..N.628",
    country_column: "REF_AREA",
    frequency:      "quarterly",
    unit:           "index",
  },
  {
    canonical_id:   "CREDIT_GDP",
    source_code:    "CREDIT_GDP",
    dataflow:       "WS_TC",
    version:        "2.0",
    key:            "Q..P.A.M.XDC.A",
    country_column: "BORROWERS_CTY",
    frequency:      "quarterly",
    unit:           "percent",
  },
  {
    canonical_id:   "POLICY_RATE_PCT",
    source_code:    "POLICY_RATE",
    dataflow:       "WS_CBPOL",
    version:        "1.0",
    key:            "M.US+GB+DE+FR+JP+CN+AU+CA+CH+SE+NO+NZ+KR+IN+BR+MX+ZA+NG+TR+RU+ID+AR+TH+PL+CZ+HU+RO+IL+SA+AE",
    country_column: "REF_AREA",
    frequency:      "monthly",
    unit:           "percent",
},
];

async function fetchBISDataset(dataset: BISDataset): Promise<BISObservation[]> {
  const url = `${BIS_BASE_URL}/${dataset.dataflow}/${dataset.version}/${dataset.key}?format=csv&startPeriod=2000`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`BIS API error ${res.status} for ${dataset.source_code}`);
  const text = await res.text();
  return parseCSV(text, dataset.country_column);
}

function parseCSV(csv: string, countryColumn: string): BISObservation[] {
  const lines = csv.split("\n").filter((l) => l.trim());
  if (lines.length < 2) return [];

  const headers = lines[0].split(",").map((h) => h.trim().replace(/^"|"$/g, ""));
  const countryIndex    = headers.indexOf(countryColumn);
  const timePeriodIndex = headers.indexOf("TIME_PERIOD");
  const obsValueIndex   = headers.indexOf("OBS_VALUE");

  if (countryIndex === -1 || timePeriodIndex === -1 || obsValueIndex === -1) return [];

  const observations: BISObservation[] = [];

  for (let i = 1; i < lines.length; i++) {
    const cols        = lines[i].split(",").map((c) => c.trim().replace(/^"|"$/g, ""));
    const countryCode = cols[countryIndex];
    const period      = cols[timePeriodIndex];
    const raw         = cols[obsValueIndex];

    if (!countryCode || !period || !raw || raw === "") continue;
    const value = parseFloat(raw);
    if (isNaN(value)) continue;

    observations.push({ countryCode, period, value });
  }

  return observations;
}

function formatPeriod(period: string, frequency: string): string {
  if (frequency === "monthly"   && period.length === 7)      return period;
  if (frequency === "quarterly" && period.includes("Q"))     return period;
  return period.slice(0, 4);
}

async function fetchBISCountryMap(client: pkg.PoolClient): Promise<Map<string, string>> {
  const result = await client.query(
    `SELECT iso3, iso2, bis_code FROM countries WHERE is_active = TRUE`
  );
  const map = new Map<string, string>();
  for (const row of result.rows) {
    if (row.bis_code) map.set(row.bis_code, row.iso3);
    if (row.iso2)     map.set(row.iso2,     row.iso3);
    map.set(row.iso3, row.iso3);
  }
  return map;
}

async function upsertObservations(
  client: pkg.PoolClient,
  dataset: BISDataset,
  observations: BISObservation[],
  countryMap: Map<string, string>
): Promise<number> {
  let count = 0;
  for (const obs of observations) {
    const iso3 = countryMap.get(obs.countryCode);
    if (!iso3) continue;
    const period = formatPeriod(obs.period, dataset.frequency);
    await client.query(
      `INSERT INTO macro_data
         (country_iso3, canonical_id, period, frequency, value, unit, source, source_code, last_updated)
       VALUES ($1,$2,$3,$4,$5,$6,'BIS',$7,NOW())
       ON CONFLICT (country_iso3, canonical_id, period, frequency, source)
       DO UPDATE SET
         value        = EXCLUDED.value,
         last_updated = EXCLUDED.last_updated,
         ingested_at  = NOW()`,
      [
        iso3,
        dataset.canonical_id,
        period,
        dataset.frequency,
        obs.value,
        dataset.unit,
        dataset.source_code,
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
     VALUES ('BIS', $1, 'running')
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

export async function ingestBIS(): Promise<void> {
  const client = await pool.connect();
  const startedAt = new Date();
  let logId: string | null = null;
  let totalRecords = 0;
  let totalErrors = 0;

  try {
    logId = await logIngestion(client, startedAt);
    const countryMap = await fetchBISCountryMap(client);

    console.log(`Ingesting ${BIS_DATASETS.length} BIS datasets...`);

    for (const dataset of BIS_DATASETS) {
      console.log(`\nDataset: ${dataset.canonical_id} (${dataset.dataflow})`);

      try {
        const observations = await fetchBISDataset(dataset);
        console.log(`  Parsed ${observations.length} raw observations.`);

        await client.query("BEGIN");
        const count = await upsertObservations(client, dataset, observations, countryMap);
        await client.query("COMMIT");

        totalRecords += count;
        console.log(`  ${count} records written.`);
      } catch (err) {
        await client.query("ROLLBACK").catch(() => {});
        console.warn(`  Skipping ${dataset.source_code}: ${err}`);
        totalErrors++;
      }

      await sleep(REQUEST_DELAY_MS);
    }

    await completeIngestion(client, logId, totalRecords);

    await client.query(
      `UPDATE source_metadata SET last_ingested = NOW() WHERE source = 'BIS'`
    );

    console.log(`\nBIS ingestion complete.`);
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

if (process.argv[1].endsWith("bis.ts")) {
  ingestBIS()
    .then(() => pool.end())
    .catch((err) => {
      console.error("BIS ingestion failed:", err);
      pool.end();
      process.exit(1);
    });
}

interface BISDataset {
  canonical_id:   string;
  source_code:    string;
  dataflow:       string;
  version:        string;
  key:            string;
  country_column: string;
  frequency:      "annual" | "quarterly" | "monthly";
  unit:           string;
}

interface BISObservation {
  countryCode: string;
  period:      string;
  value:       number;
}