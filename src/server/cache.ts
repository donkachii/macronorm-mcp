import { createClient } from "redis";

const CACHE_TTL_SECONDS = 60 * 60 * 24;

let client: ReturnType<typeof createClient> | null = null;

export async function getCache(): Promise<ReturnType<typeof createClient> | null> {
  if (!process.env.REDIS_URL) {
    console.warn("REDIS_URL not set — cache disabled");
    return null;
  }

  if (!client) {
    try {
      const c = createClient({ url: process.env.REDIS_URL });
      c.on("error", (err) => console.error("Redis client error:", err));
      await c.connect();
      client = c;
    } catch (err) {
      console.error("Redis connection failed:", err);
      return null;
    }
  }

  return client;
}

export async function cacheGet(key: string): Promise<unknown | null> {
  const redis = await getCache();
  if (!redis) return null;

  try {
    const value = await redis.get(key);
    if (!value) return null;
    return JSON.parse(value);
  } catch (err) {
    console.error("cacheGet error:", err);
    return null;
  }
}

export async function cacheSet(key: string, value: unknown): Promise<void> {
  const redis = await getCache();
  if (!redis) return;

  try {
    await redis.set(key, JSON.stringify(value), { EX: CACHE_TTL_SECONDS });
  } catch (err) {
    console.error("cacheSet error:", err);
  }
}

export function makeCacheKey(...parts: (string | number | undefined)[]): string {
  return `macronorm:${parts.filter(Boolean).join(":")}`;
}