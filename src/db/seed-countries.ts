import "dotenv/config";
import fetch from "node-fetch";
import pkg from "pg";

const { Pool } = pkg;

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function fetchWorldBankCountries(): Promise<WBCountry[]> {
  const url = "https://api.worldbank.org/v2/country?format=json&per_page=300";
  const res = await fetch(url);
  if (!res.ok) throw new Error(`World Bank countries API error: ${res.status}`);
  const data = (await res.json()) as [WBMeta, WBCountry[]];
  return data[1].filter((c) => c.region?.id !== "NA");
}

async function fetchIMFCountryCodes(): Promise<Map<string, number>> {
  const url = "https://www.imf.org/external/datamapper/api/v1/countries";
  const res = await fetch(url);
  if (!res.ok) throw new Error(`IMF countries API error: ${res.status}`);
  const data = (await res.json()) as IMFCountryResponse;
  const map = new Map<string, number>();
  for (const [code, meta] of Object.entries(data.countries ?? {})) {
    if (meta.label) map.set(code, parseInt(code, 10) || 0);
  }
  return map;
}

const BIS_CODE_MAP: Record<string, string> = {
  AR: "AR", AU: "AU", AT: "AT", BE: "BE", BR: "BR", CA: "CA",
  CL: "CL", CN: "CN", CZ: "CZ", DK: "DK", FI: "FI", FR: "FR",
  DE: "DE", GR: "GR", HK: "HK", HU: "HU", IN: "IN", ID: "ID",
  IE: "IE", IL: "IL", IT: "IT", JP: "JP", KR: "KR", LT: "LT",
  LU: "LU", LV: "LV", MX: "MX", NL: "NL", NZ: "NZ", NG: "NG",
  NO: "NO", PL: "PL", PT: "PT", RO: "RO", RU: "RU", SA: "SA",
  SG: "SG", SK: "SK", SI: "SI", ZA: "ZA", ES: "ES", SE: "SE",
  CH: "CH", TH: "TH", TR: "TR", GB: "GB", US: "US",
};

const EDGE_CASES: Record<string, Partial<DBCountry>> = {
  XKX: { iso3: "XKX", iso2: "XK", name: "Kosovo",            imf_code: 967, wb_code: "XKX", bis_code: null, un_m49: null },
  TWN: { iso3: "TWN", iso2: "TW", name: "Taiwan",             imf_code: 528, wb_code: "TWN", bis_code: null, un_m49: "158" },
  PSE: { iso3: "PSE", iso2: "PS", name: "West Bank and Gaza", imf_code: 487, wb_code: "PSE", bis_code: null, un_m49: "275" },
  GGY: { iso3: "GGY", iso2: "GG", name: "Guernsey",           imf_code: null, wb_code: "GGY", bis_code: null, un_m49: "831" },
  JEY: { iso3: "JEY", iso2: "JE", name: "Jersey",             imf_code: null, wb_code: "JEY", bis_code: null, un_m49: "832" },
};

async function upsertCountries(countries: DBCountry[]): Promise<number> {
  const client = await pool.connect();
  let count = 0;
  try {
    await client.query("BEGIN");
    for (const c of countries) {
      await client.query(
        `INSERT INTO countries (iso3, iso2, name, imf_code, wb_code, bis_code, un_m49)
         VALUES ($1,$2,$3,$4,$5,$6,$7)
         ON CONFLICT (iso3) DO UPDATE SET
           name     = EXCLUDED.name,
           imf_code = EXCLUDED.imf_code,
           wb_code  = EXCLUDED.wb_code,
           bis_code = EXCLUDED.bis_code,
           un_m49   = EXCLUDED.un_m49`,
        [c.iso3, c.iso2, c.name, c.imf_code, c.wb_code, c.bis_code, c.un_m49]
      );
      count++;
    }
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
  return count;
}

export async function bootstrapCountries(): Promise<void> {
  console.log("Fetching countries from World Bank...");
  const wbCountries = await fetchWorldBankCountries();

  console.log("Fetching country codes from IMF...");
  const imfMap = await fetchIMFCountryCodes();

  const rows: DBCountry[] = wbCountries
    .filter((c) => c.iso2Code && c.id && c.name)
    .map((c) => ({
      iso3:     c.id,
      iso2:     c.iso2Code,
      name:     c.name,
      imf_code: imfMap.get(c.id) ?? null,
      wb_code:  c.id,
      bis_code: BIS_CODE_MAP[c.iso2Code] ?? null,
      un_m49:   null,
    }));

  for (const edge of Object.values(EDGE_CASES)) {
    const existing = rows.findIndex((r) => r.iso3 === edge.iso3);
    if (existing >= 0) {
      rows[existing] = { ...rows[existing], ...edge };
    } else {
      rows.push(edge as DBCountry);
    }
  }

  const count = await upsertCountries(rows);
  console.log(`Seeded ${count} countries.`);
}

interface WBMeta {
  page: number;
  pages: number;
  per_page: string;
  total: number;
}

interface WBCountry {
  id: string;
  iso2Code: string;
  name: string;
  region?: { id: string; value: string };
}

interface IMFCountryResponse {
  countries?: Record<string, { label?: string }>;
}

interface DBCountry {
  iso3: string;
  iso2: string | null;
  name: string;
  imf_code: number | null;
  wb_code: string | null;
  bis_code: string | null;
  un_m49: string | null;
}