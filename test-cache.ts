import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";

const transport = new SSEClientTransport(new URL("http://localhost:3001/sse"));
const client = new Client({ name: "test", version: "1.0.0" });
await client.connect(transport);

console.log("--- Call 1 ---");
const r1 = await client.callTool({ name: "get_indicator", arguments: { indicator: "GDP_CURRENT_USD", country_iso3: "NGA", frequency: "annual", limit: 5 } });
console.log(JSON.parse((r1.content as any)[0].text).fetched_at);

console.log("--- Call 2 ---");
const r2 = await client.callTool({ name: "get_indicator", arguments: { indicator: "GDP_CURRENT_USD", country_iso3: "NGA", frequency: "annual", limit: 5 } });
console.log(JSON.parse((r2.content as any)[0].text).fetched_at);

await client.close();