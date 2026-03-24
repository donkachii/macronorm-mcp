import "dotenv/config";
import cron from "node-cron";
import { ingestFred } from "./fred.js";
import { ingestWorldBank } from "./world-bank.js";
import { ingestIMF } from "./imf.js";
import { ingestBIS } from "./bis.js";
import pkg from "pg";

const { Pool } = pkg;
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function runIngestion(
  name: string,
  fn: () => Promise<void>
): Promise<void> {
  console.log(`[${new Date().toISOString()}] Starting ${name} ingestion...`);
  try {
    await fn();
    console.log(`[${new Date().toISOString()}] ${name} ingestion complete.`);
  } catch (err) {
    console.error(`[${new Date().toISOString()}] ${name} ingestion failed:`, err);
  }
}

async function runAll(): Promise<void> {
  await runIngestion("FRED", ingestFred);
  await runIngestion("World Bank", ingestWorldBank);
  await runIngestion("IMF", ingestIMF);
  await runIngestion("BIS", ingestBIS);
}

async function updateNextScheduled(source: string, nextRun: Date): Promise<void> {
  await pool.query(
    `UPDATE source_metadata SET next_scheduled = $1 WHERE source = $2`,
    [nextRun, source]
  );
}

cron.schedule("0 4 * * *", async () => {
  console.log(`[${new Date().toISOString()}] Running daily FRED ingestion...`);
  await runIngestion("FRED", ingestFred);
  const next = new Date();
  next.setDate(next.getDate() + 1);
  next.setHours(4, 0, 0, 0);
  await updateNextScheduled("FRED", next);
}, { timezone: "UTC" });

cron.schedule("0 2 * * 1", async () => {
  console.log(`[${new Date().toISOString()}] Running weekly IMF ingestion...`);
  await runIngestion("IMF", ingestIMF);
  const next = new Date();
  next.setDate(next.getDate() + 7);
  next.setHours(2, 0, 0, 0);
  await updateNextScheduled("IMF", next);
}, { timezone: "UTC" });

cron.schedule("0 3 * * 1", async () => {
  console.log(`[${new Date().toISOString()}] Running weekly World Bank ingestion...`);
  await runIngestion("World Bank", ingestWorldBank);
  const next = new Date();
  next.setDate(next.getDate() + 7);
  next.setHours(3, 0, 0, 0);
  await updateNextScheduled("WORLD_BANK", next);
}, { timezone: "UTC" });

cron.schedule("0 5 * * 1", async () => {
  console.log(`[${new Date().toISOString()}] Running weekly BIS ingestion...`);
  await runIngestion("BIS", ingestBIS);
  const next = new Date();
  next.setDate(next.getDate() + 7);
  next.setHours(5, 0, 0, 0);
  await updateNextScheduled("BIS", next);
}, { timezone: "UTC" });

console.log("Scheduler started.");
console.log("  FRED:       daily at 04:00 UTC");
console.log("  IMF:        weekly on Monday at 02:00 UTC");
console.log("  World Bank: weekly on Monday at 03:00 UTC");
console.log("  BIS:        weekly on Monday at 05:00 UTC");

if (process.argv.includes("--run-now")) {
  console.log("\nRunning all ingestion workers immediately (--run-now flag)...");
  runAll().then(() => {
    console.log("All ingestion complete.");
    pool.end();
    process.exit(0);
  });
} else {
  setInterval(() => {}, 1 << 30);
}