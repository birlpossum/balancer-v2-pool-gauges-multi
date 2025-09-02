// Policy Reference: Kleros ATQ Registry Guidelines Version 2.3.0
import fetch from "node-fetch";
import { ContractTag, ITagService } from "atq-types";

// Balancer v2 Gauges on supported chains:
// - Optimism (chainId 10)
// - Gnosis (chainId 100)
// - Avalanche C-Chain (chainId 43114)
// - Base (chainId 8453)
// - Arbitrum One (chainId 42161)
// Subgraphs sourced from official docs: https://docs-v2.balancer.fi/reference/subgraph/#v2-subgraphs
// For each chain, set the correct deployment ID for both Gauges and v2 Pools subgraphs below.
const SUBGRAPH_URLS: Record<string, { gauges: string; v2pools: string }> = {
  "10": {
    gauges:
      "https://gateway.thegraph.com/api/[api-key]/deployments/id/Qmdtj1ix1nUCRtSoiyF7a3oKMSvrKT8KTEFJdep53EHtRy",
    v2pools:
      "https://gateway.thegraph.com/api/[api-key]/deployments/id/QmWUgkiUM5c3BW1Z51DUkZfnyQfyfesE8p3BRnEtA9vyPL",
  },
  "100": {
    gauges:
      "https://gateway.thegraph.com/api/[api-key]/deployments/id/QmPyH9BbVshMZRp5T7WBPnx1J5GVi46GX2NmheRQUkqF39",
    v2pools:
      "https://gateway.thegraph.com/api/[api-key]/deployments/id/QmXXSKeLh14DnJgR1ncHhAHciqacfRshcHKXasAGy7LP4Y",
  },
  "43114": {
    gauges:
      "https://gateway.thegraph.com/api/[api-key]/deployments/id/QmQYUD5riMQmA8yzJQjSFonEZxkA9PLEoaxpQVjQdnBPHM",
    v2pools:
      "https://gateway.thegraph.com/api/[api-key]/deployments/id/QmNudbtVu2eACfxNpFz37MVwKxxHPh1Lg5MzFKwQZG2xsU",
  },
  "8453": {
    gauges:
      "https://gateway.thegraph.com/api/[api-key]/deployments/id/QmYrM1KZSwVHhDXEnMCvPrShqLmoAWbsULZgHjgt92fQTY",
    v2pools:
      "https://gateway.thegraph.com/api/[api-key]/deployments/id/QmRKBwBwPKtFz4mQp5jvH44USVprM4C77Nr4m77UGCbGv9",
  },
  "42161": {
    gauges:
      "https://gateway-arbitrum.network.thegraph.com/api/[api-key]/deployments/id/QmT3h6pogdPkxfWsBxKNtpq7kR9fqKaQ9jGxe7fZx7MUVE",
    v2pools:
      "https://gateway-arbitrum.network.thegraph.com/api/[api-key]/deployments/id/QmPbjY6L1NhPjpBv7wDTfG9EPx5FpCuBqeg1XxByzBTLcs",
  },
};

// ---------- Shared bits you already had ----------
interface VaultsPoolNameInfo { id: string; address: string; name: string; symbol: string }
interface VaultsGraphQLData { pools: VaultsPoolNameInfo[] }
interface VaultsGraphQLResponse { data?: VaultsGraphQLData; errors?: { message: string }[] }

const headers: Record<string, string> = {
  "Content-Type": "application/json",
  Accept: "application/json",
};

// Pin queries to a consistent snapshot by fetching the currently indexed block
const GET_META_BLOCK_QUERY = `
  query { _meta { block { number } } }
`;

async function fetchIndexedBlockNumber(subgraphUrl: string): Promise<number> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30_000);
  try {
    const resp = await fetch(subgraphUrl, {
      method: "POST",
      headers,
      body: JSON.stringify({ query: GET_META_BLOCK_QUERY }),
      signal: controller.signal,
    } as any);
    if (!resp.ok) throw new Error(`HTTP error (meta): ${resp.status}`);
    const json: any = await resp.json();
    const blockNumber = json?.data?._meta?.block?.number;
    if (typeof blockNumber !== "number") {
      throw new Error("Failed to read _meta.block.number from subgraph response");
    }
    return blockNumber;
  } catch (e: any) {
    if (e?.name === "AbortError") {
      throw new Error("Request timed out while querying _meta for block number.");
    }
    throw e;
  } finally {
    clearTimeout(timeout);
  }
}

// Policy note: Cursor-based pagination using 'id'. Balancer pools do not expose a single-field
// unique AND strictly sequential cursor in the schema. The Graph does not support multi-field
// orderBy for a composite cursor. Therefore, per policy's fallback, we use the unique 'id' field
// with ascending order and id_gt for completeness.
const GET_VAULTS_POOL_NAMES_QUERY = `
  query GetVaultsPools($lastId: ID) {
    pools(
      first: 1000,
      orderBy: id,
      orderDirection: asc,
      where: { id_gt: $lastId }
    ) {
      id
      address
      name
      symbol
    }
  }
`;

function isError(e: unknown): e is Error {
  return typeof e === "object" && e !== null && "message" in e && typeof (e as Error).message === "string";
}

function truncateString(text: string, maxLength: number) {
  return text.length > maxLength ? text.substring(0, maxLength - 3) + "..." : text;
}

// v2 Pools name lookup by poolId (id)
// Policy note: Same rationale as above — use 'id' for cursoring as a unique fallback; no single-field
// unique+sequential alternative is available. We fetch at a pinned block height for determinism.
const GET_V2_POOL_NAMES_BY_ID = `
  query GetPools($lastId: ID, $block: Int!) {
    pools(first: 1000, orderBy: id, orderDirection: asc, where: { id_gt: $lastId }, block: { number: $block }) {
      id
      address
      name
      symbol
      poolType
    }
  }
`;

async function fetchV2PoolInfoMap(subgraphUrl: string, blockNumber: number): Promise<Map<string, { name: string; symbol: string; address: string; poolType?: string }>> {
  const m = new Map<string, { name: string; symbol: string; address: string; poolType?: string }>();
  let lastId = "0x0";
  let more = true;
  while (more) {
    const r = await fetch(subgraphUrl, {
      method: "POST",
      headers,
      body: JSON.stringify({ query: GET_V2_POOL_NAMES_BY_ID, variables: { lastId, block: blockNumber } })
    } as any);
    if (!r.ok) throw new Error(`HTTP error (v2 pools): ${r.status}`);
    const j = await r.json() as { data?: { pools?: Array<{ id?: string; address?: string; name?: string; symbol?: string; poolType?: string }> } };
    const page = j?.data?.pools ?? [];
    for (const p of page) {
      if (p?.id && p?.name) {
        m.set(String(p.id).toLowerCase(), {
          name: p.name,
          symbol: p.symbol || "",
          address: (p.address || "").toLowerCase(),
          poolType: p.poolType || undefined,
        });
      }
    }
    more = page.length === 1000;
    if (more) lastId = String(page[page.length - 1].id);
  }
  return m;
}

// ---------- New: Gauges types + query ----------
interface Gauge {
  id: string;                      // gauge contract address
  poolId?: string | null;          // Balancer poolId (string)
  isKilled?: boolean | null;       // filter to live gauges
  pool?: { address?: string | null } | null; // BPT token address (pool address)
}

interface GaugesGraphQLData { liquidityGauges: Gauge[] }
interface GaugesGraphQLResponse { data?: GaugesGraphQLData; errors?: { message: string }[] }

// Policy note: Gauges subgraph also lacks a single-field unique+sequential cursor. We therefore
// use 'id' with id_gt and ascending order per the policy's allowed fallback.
const GET_GAUGES_QUERY = `
  query GetGauges($lastId: ID, $block: Int!) {
    liquidityGauges(
      first: 1000
      orderBy: id
      orderDirection: asc
      where: { id_gt: $lastId, poolId_not: null, isKilled_not: true }
      block: { number: $block }
    ) {
      id
      poolId
      isKilled
      pool { address }
    }
  }
`;

// ---------- Fetch helpers ----------
async function fetchPoolNamesMap(subgraphUrl: string): Promise<Map<string, { name: string; symbol: string }>> {
  const names = new Map<string, { name: string; symbol: string }>();
  let lastId = "0x0000000000000000000000000000000000000000";
  let isMore = true;

  while (isMore) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30_000);
    let response: any;
    try {
      response = await fetch(subgraphUrl, {
        method: "POST",
        headers,
        body: JSON.stringify({ query: GET_VAULTS_POOL_NAMES_QUERY, variables: { lastId } }),
        signal: controller.signal,
      } as any);
    } catch (e: any) {
      clearTimeout(timeout);
      if (e?.name === "AbortError") throw new Error("Request timed out while querying the vaults subgraph.");
      throw e;
    } finally {
      clearTimeout(timeout);
    }
    if (!response.ok) throw new Error(`HTTP error (vaults): ${response.status}`);

    const result = (await response.json()) as VaultsGraphQLResponse;
    if (result.errors) {
      result.errors.forEach((error) => console.error(`GraphQL error (vaults): ${error.message}`));
      throw new Error("GraphQL error from vaults subgraph.");
    }
    const page = result.data?.pools ?? [];
    for (const p of page) {
      if (p?.address && p?.name) {
        names.set(String(p.address).toLowerCase(), { name: p.name, symbol: p.symbol });
      }
    }
    isMore = page.length === 1000;
    if (isMore) {
      const nextLastId = page[page.length - 1].id;
      if (!nextLastId || nextLastId === lastId) throw new Error("Pagination cursor (vaults) did not advance; aborting.");
      lastId = nextLastId;
    }
  }
  return names;
}

async function fetchGauges(subgraphUrl: string, lastId: string, blockNumber: number): Promise<Gauge[]> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30_000);
  let response: any;
  try {
    response = await fetch(subgraphUrl, {
      method: "POST",
      headers,
      body: JSON.stringify({ query: GET_GAUGES_QUERY, variables: { lastId, block: blockNumber } }),
      signal: controller.signal,
    } as any);
  } catch (e: any) {
    if (e?.name === "AbortError") throw new Error("Request timed out while querying the gauges subgraph.");
    throw e;
  } finally {
    clearTimeout(timeout);
  }
  if (!response.ok) throw new Error(`HTTP error (gauges): ${response.status}`);

  const result = (await response.json()) as GaugesGraphQLResponse;
  if (result.errors) {
    result.errors.forEach((error) => console.error(`GraphQL error (gauges): ${error.message}`));
    throw new Error("GraphQL error from gauges subgraph.");
  }
  return result.data?.liquidityGauges ?? [];
}

// ---------- Transform to Kleros-friendly tags ----------
function transformGaugesToTags(
  chainId: string,
  gauges: Gauge[],
  v2Map?: Map<string, { name: string; symbol: string; address: string; poolType?: string }>
): ContractTag[] {
  const tags: ContractTag[] = [];
  const seen = new Set<string>();

  for (const g of gauges) {
    if (!g?.id || seen.has(g.id)) continue;
    seen.add(g.id);
    const info = g.poolId ? v2Map?.get(String(g.poolId).toLowerCase()) : undefined;
    const poolName = info?.name?.trim() ?? "";
    const poolType = info?.poolType?.trim() ?? "";
    // Skip gauges whose pool name is not known to avoid ambiguous tags
    if (!poolName) continue;
    const maxLen = 50;
    const baseName = `Pool Gauge — ${poolName}`;
    const publicNameTag = truncateString(baseName, maxLen);
    const noteParts: string[] = ["Liquidity gauge for Balancer V2"];
    if (poolType) noteParts.push(`${poolType} pool.`);
    else noteParts.push("pool.");
    const publicNote = noteParts.join(" ");

    tags.push({
      "Contract Address": `eip155:${chainId}:${g.id}`,
      "Public Name Tag": publicNameTag,
      "Project Name": "Balancer",
      "UI/Website Link": "https://balancer.fi",
      "Public Note": publicNote,
    });
  }
  return tags;
}

// ---------- Main service ----------
class TagService implements ITagService {
  returnTags = async (chainId: string, apiKey: string): Promise<ContractTag[]> => {
    const originalChainId = chainId;
    const trimmedChainId = (chainId ?? "").trim();
    // Enforce decimal string format only
    if (!/^\d+$/.test(trimmedChainId)) {
      throw new Error(`Unsupported Chain ID: ${originalChainId}. Only 10 (Optimism), 100 (Gnosis), 43114 (Avalanche), 8453 (Base), and 42161 (Arbitrum) are supported in this module.`);
    }
    const chainIdNum = Number(trimmedChainId);
    if (!Number.isInteger(chainIdNum) || (chainIdNum !== 10 && chainIdNum !== 100 && chainIdNum !== 43114 && chainIdNum !== 8453 && chainIdNum !== 42161)) {
      throw new Error(`Unsupported Chain ID: ${originalChainId}. Only 10 (Optimism), 100 (Gnosis), 43114 (Avalanche), 8453 (Base), and 42161 (Arbitrum) are supported in this module.`);
    }
    if (!apiKey || apiKey.trim().length === 0) {
      throw new Error("Missing API key. A The Graph gateway API key is required.");
    }

    const chainKey = String(chainIdNum);
    const gaugesUrl = SUBGRAPH_URLS[chainKey]?.gauges?.replace("[api-key]", encodeURIComponent(apiKey));
    const v2PoolsUrl = SUBGRAPH_URLS[chainKey]?.v2pools?.replace("[api-key]", encodeURIComponent(apiKey));
    if (!gaugesUrl || !v2PoolsUrl) {
      // Treat missing URLs as unsupported chain per policy
      throw new Error(`Unsupported Chain ID: ${originalChainId}. Only 10 (Optimism), 100 (Gnosis), 43114 (Avalanche), 8453 (Base), and 42161 (Arbitrum) are supported in this module.`);
    }

    // Pin to a consistent snapshot across both subgraphs.
    // Policy: While not explicitly mandated, pinning avoids race conditions across paginated requests
    // and between different subgraphs. We query each subgraph's _meta.block.number and use the min
    // to ensure all data is visible at the chosen height.
    const gaugesBlock = await fetchIndexedBlockNumber(gaugesUrl);
    const v2PoolsBlock = await fetchIndexedBlockNumber(v2PoolsUrl);
    const blockNumber = Math.min(gaugesBlock, v2PoolsBlock);

    // Page gauges with id_gt cursor
    let lastId = "0x0000000000000000000000000000000000000000";
    let prevLastId = "";
    const allGauges: Gauge[] = [];
    let isMore = true;

    while (isMore) {
      const page = await fetchGauges(gaugesUrl, lastId, blockNumber);
      allGauges.push(...page);

      isMore = page.length === 1000;
      if (isMore) {
        const nextLastId = page[page.length - 1].id;
        if (!nextLastId || nextLastId === lastId || nextLastId === prevLastId) {
          throw new Error("Pagination cursor (gauges) did not advance; aborting.");
        }
        prevLastId = lastId;
        lastId = nextLastId;
      }
    }

    // Enrich with pool names from v2 Pools subgraph keyed by poolId
    const v2Map = await fetchV2PoolInfoMap(v2PoolsUrl, blockNumber);

    // Build tags
    return transformGaugesToTags(String(chainIdNum), allGauges, v2Map);
  };
}

const tagService = new TagService();
export const returnTags = tagService.returnTags;