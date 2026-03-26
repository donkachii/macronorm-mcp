import "dotenv/config";
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import pkg from "pg";

const { Pool } = pkg;

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const __dirname = dirname(fileURLToPath(import.meta.url));
const schema = readFileSync(join(__dirname, "schema.sql"), "utf8");

async function isAlreadySetUp(): Promise<boolean> {
  const result = await pool.query(
    `SELECT EXISTS (
       SELECT FROM information_schema.tables
       WHERE table_name = 'countries'
     ) AS exists`
  );
  return result.rows[0].exists === true;
}

async function main(): Promise<void> {
  const client = await pool.connect();
  try {
    const alreadySetUp = await isAlreadySetUp();
    if (alreadySetUp) {
      console.log("Schema already exists, skipping.");
      return;
    }
    await client.query(schema);
    console.log("Schema applied successfully.");
  } catch (err) {
    console.error("Schema error:", err);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

main();