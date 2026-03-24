# MacroNorm — Normalized International Macroeconomic Data Feed

MacroNorm is an MCP server that unbundles Trading Economics ($4,800/yr) and Haver Analytics ($5,000+/yr). when given any indicator and country, it returns normalized macroeconomic data from the best available source — IMF, World Bank, FRED, or BIS — in a single consistent schema.

## What It Does

Developers pay weeks of integration work to normalize macro data across 5 incompatible institutional APIs. Universities and hedge funds pay $4,800/year for Trading Economics to do exactly this normalization. MacroNorm delivers the same unified schema as an MCP tool for $0.001/call.

Input: An indicator ID, country ISO-3 code, and frequency
Output: Normalized time-series data with source attribution

```json
{
  "country_iso3": "NGA",
  "country_name": "Nigeria",
  "canonical_id": "GDP_CURRENT_USD",
  "indicator_name": "GDP (Current USD)",
  "frequency": "annual",
  "unit": "USD",
  "data": [
    { "period": "2024", "value": 362814000000, "source": "IMF_WEO" },
    { "period": "2023", "value": 362814000000, "source": "IMF_WEO" },
    { "period": "2022", "value": 477386000000, "source": "IMF_WEO" }
  ],
  "fetched_at": "2026-03-24T07:42:14.589Z"
}
```

## Architecture

```
Request (indicator + country + frequency)
        │
        ▼
┌───────────────────┐
│   Redis Cache     │  Returns in <10ms if cached (24hr TTL)
└───────────────────┘
        │ MISS
        ▼
┌───────────────────┐
│  Postgres Query   │  Pre-ingested normalized data, instant lookup
└───────────────────┘
        │
        ▼
┌───────────────────┐
│ Frequency Fallback│  Aggregates monthly → annual if exact match unavailable
└───────────────────┘
        │
        ▼
┌───────────────────┐
│  Cache + Return   │  Stores result, returns clean JSON
└───────────────────┘
```

Background ingestion workers run on cron — FRED daily, IMF/World Bank/BIS weekly — so all MCP responses are served from local cache with zero upstream API calls at request time.

## Files

| File                          | Purpose                                                                 |
| ----------------------------- | ----------------------------------------------------------------------- |
| `src/server/index.ts`         | MCP server, tool definitions, request handlers                          |
| `src/server/cache.ts`         | Redis cache layer with 24hr TTL                                         |
| `src/ingestion/fred.ts`       | FRED ingestion worker                                                   |
| `src/ingestion/world-bank.ts` | World Bank ingestion worker                                             |
| `src/ingestion/imf.ts`        | IMF ingestion worker                                                    |
| `src/ingestion/bis.ts`        | BIS ingestion worker                                                    |
| `src/ingestion/scheduler.ts`  | Cron scheduler for all workers                                          |
| `src/db/schema.sql`           | 6-table schema: countries, indicators, source maps, data, ingestion log |
| `src/db/setup.ts`             | Bootstrap entry point                                                   |
| `src/db/seed-countries.ts`    | Seeds 220 countries from World Bank + IMF APIs                          |
| `src/db/seed-indicators.ts`   | Authors 95 canonical indicators with source mappings                    |

## Data Sources

| Source         | Coverage                                          | Auth         |
| -------------- | ------------------------------------------------- | ------------ |
| IMF WEO        | 196 countries, annual macro projections + history | None         |
| World Bank WDI | 218 countries, 16,000+ series                     | None         |
| FRED           | 840,000+ US series, daily updates                 | Free API key |
| BIS            | 47 countries, credit, house prices, policy rates  | None         |

## Coverage

- **220 countries** with full crosswalk: IMF numeric, World Bank ISO-3, BIS ISO-2, UN M49
- **95 canonical indicators** across 10 categories
- **356,000+ normalized records**
- Edge cases handled: Kosovo (XKX), Taiwan (TWN), West Bank (PSE), Guernsey (GGY), Jersey (JEY)

## Indicators by Category

| Category        | Examples                                                                            |
| --------------- | ----------------------------------------------------------------------------------- |
| Output & Growth | GDP (current, constant, PPP, per capita), GDP growth, output gap, capital formation |
| Prices          | CPI, PPI, food inflation, house prices, oil prices                                  |
| Labor           | Unemployment, youth unemployment, labor force participation, wage growth            |
| External Sector | Current account, trade balance, FDI, reserves, remittances, external debt           |
| Fiscal          | Government debt, balance, revenue, expenditure, tax revenue, interest payments      |
| Monetary        | Policy rates, M2, exchange rates, lending rates, credit to private sector           |
| Financial       | NPL ratio, bank capital, stock market cap, credit gap, debt service ratio           |
| Demographic     | Population, urbanization, life expectancy, fertility rate                           |
| Energy          | Energy use, electricity access, renewables, CO2 emissions                           |
| Development     | GNI per capita, poverty headcount, Gini index, internet users                       |

## Source Attribution

Every data point returns the institution and database it came from:

| Source field     | Meaning                                 |
| ---------------- | --------------------------------------- |
| `IMF_WEO`        | IMF World Economic Outlook              |
| `IMF_IFS`        | IMF International Financial Statistics  |
| `WORLD_BANK_WDI` | World Bank World Development Indicators |
| `FRED`           | St. Louis Fed FRED                      |
| `BIS_WS_SPP`     | BIS Selected Property Prices            |
| `BIS_WS_CBPOL`   | BIS Central Bank Policy Rates           |

## Frequency Normalization

When the requested frequency has no exact data, MacroNorm automatically aggregates from higher-frequency data and labels the result transparently:

```json
{
  "frequency": "annual (aggregated from monthly)",
  "data": [
    { "period": "2024", "value": 5.144, "source": "FRED" },
    { "period": "2023", "value": 5.064, "source": "FRED" }
  ]
}
```

## Tools

### `get_indicator`

Returns time-series data for a single indicator and country.

**Parameters:**

- `indicator` — canonical indicator ID (e.g. `GDP_CURRENT_USD`, `CPI_ANNUAL_PCT`, `POLICY_RATE_PCT`)
- `country_iso3` — ISO 3-letter country code (e.g. `NGA`, `DEU`, `CHN`)
- `frequency` — `annual`, `quarterly`, or `monthly` (default: `annual`)
- `limit` — number of periods to return, most recent first (default: 10)

### `compare_indicator`

Compares a single indicator across multiple countries. Returns the most recent available value per country, sorted descending.

**Parameters:**

- `indicator` — canonical indicator ID
- `countries` — array of ISO-3 codes (e.g. `["NGA", "ZAF", "KEN", "GHA"]`)
- `frequency` — `annual`, `quarterly`, or `monthly` (default: `annual`)
- `period` — specific period to pin the comparison to (optional, e.g. `"2023"`)

### `list_indicators`

Returns the full catalog of 95 available indicators with categories, units, frequencies, and source counts. Accepts an optional `category` filter.

### `list_countries`

Returns all 220 available countries with ISO-3, ISO-2, and IMF codes. Accepts an optional `search` parameter for partial name matching.

## Performance

| Scenario                    | Response Time |
| --------------------------- | ------------- |
| Cache HIT                   | <10ms         |
| Cache MISS (Postgres query) | 10–50ms       |
| Cache TTL                   | 24 hours      |

## Local Setup

```bash
# Install dependencies
npm install

# Configure environment
cp .env.example .env
# Fill in DATABASE_URL, REDIS_URL, FRED_API_KEY

# Create database and run schema
psql -U your_user -d postgres -c "CREATE DATABASE macronorm;"
psql -U your_user -d macronorm -f src/db/schema.sql

# Seed countries and indicators
npm run bootstrap

# Run ingestion (~10 minutes on first run)
npm run ingest:all

# Start MCP server
npm run dev

# Start scheduler (separate terminal)
npm run scheduler
```

Test:

```bash
# Health check
curl https://localhost:3000/health

# List tools
curl -X POST http://localhost:3000/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","method":"tools/list","id":1}'

# Query Nigeria GDP
curl -X POST http://localhost:3000/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","method":"tools/call","id":2,"params":{"name":"get_indicator","arguments":{"indicator":"GDP_CURRENT_USD","country_iso3":"NGA","frequency":"annual","limit":5}}}'
```

## Deployment

Deployed on Railway with two services — MCP server and scheduler — backed by Railway Postgres and a hosted Redis instance.

Required environment variables:

```
DATABASE_URL=postgresql://...
REDIS_URL=redis://...
FRED_API_KEY=your_fred_api_key
```

## Pricing

- **Query mode**: $0.10 per response
- **Execute mode**: $0.001 per call
