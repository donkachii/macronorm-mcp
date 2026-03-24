import "dotenv/config";
import { bootstrapCountries } from "./seed-countries.js";
import { bootstrapIndicators } from "./seed-indicators.js";
import pkg from "pg";

const { Pool } = pkg;

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function isAlreadySeeded(): Promise<boolean> {
  const result = await pool.query("SELECT COUNT(*) FROM countries");
  return parseInt(result.rows[0].count, 10) > 0;
}

async function main(): Promise<void> {
  const force = process.argv.includes("--force");

  if (!force && (await isAlreadySeeded())) {
    console.log("Database already seeded. Run with --force to re-seed.");
    await pool.end();
    return;
  }

  try {
    await bootstrapCountries();
    await bootstrapIndicators();
    console.log("Bootstrap complete.");
  } catch (err) {
    console.error("Bootstrap failed:", err);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();