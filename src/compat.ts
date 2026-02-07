import * as Lark from "@larksuiteoapi/node-sdk";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { OpenClawConfig } from "openclaw/plugin-sdk";
import { DEFAULT_ACCOUNT_ID } from "openclaw/plugin-sdk";

type DmPolicy = "pairing" | "allowlist" | "open" | "disabled";
type GroupPolicy = "open" | "allowlist" | "disabled";

type FeishuAccountConfig = Record<string, unknown>;
type FeishuConfig = FeishuAccountConfig & {
  accounts?: Record<string, FeishuAccountConfig>;
};

type ResolvedFeishuConfig = {
  enabled: boolean;
  dmPolicy: DmPolicy;
  groupPolicy: GroupPolicy;
  allowFrom: Array<string | number>;
  groupAllowFrom: Array<string | number>;
  historyLimit: number;
  dmHistoryLimit: number;
  textChunkLimit: number;
  chunkMode: "length" | "newline";
  blockStreaming: boolean;
  streaming: boolean;
  mediaMaxMb: number;
  groups: Record<
    string,
    {
      enabled?: boolean;
      requireMention?: boolean;
      allowFrom?: Array<string | number>;
      systemPrompt?: string;
      skills?: string[];
    }
  >;
};

const DEFAULT_HISTORY_LIMIT = 20;
const DEFAULT_TEXT_CHUNK_LIMIT = 4000;
const DEFAULT_MEDIA_MAX_MB = 15;

function normalizeAccountId(raw?: string | null): string {
  const trimmed = (raw ?? "").trim();
  return trimmed || DEFAULT_ACCOUNT_ID;
}

function resolveAccountConfig(cfg: OpenClawConfig, accountId: string): FeishuAccountConfig | null {
  const feishuCfg = cfg.channels?.feishu as FeishuConfig | undefined;
  const accounts = feishuCfg?.accounts;
  if (!accounts || typeof accounts !== "object") {
    return null;
  }
  return accounts[accountId] ?? null;
}

function stripAccountsField(cfg: FeishuConfig): FeishuAccountConfig {
  const { accounts: _ignored, ...rest } = cfg ?? {};
  return rest;
}

function expandUserPath(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) {
    return trimmed;
  }
  if (trimmed === "~") {
    return os.homedir();
  }
  if (trimmed.startsWith("~/")) {
    return path.join(os.homedir(), trimmed.slice(2));
  }
  if (trimmed.startsWith("~")) {
    return path.resolve(os.homedir(), trimmed.slice(1));
  }
  return path.resolve(trimmed);
}

function resolveSecretFromFile(raw?: unknown): string | undefined {
  if (typeof raw !== "string") {
    return undefined;
  }
  const resolved = expandUserPath(raw);
  try {
    const content = fs.readFileSync(resolved, "utf-8").trim();
    return content || undefined;
  } catch {
    return undefined;
  }
}

function mergeFeishuConfig(cfg: OpenClawConfig, accountId?: string | null): FeishuAccountConfig {
  const feishuCfg = (cfg.channels?.feishu as FeishuConfig | undefined) ?? {};
  const base = stripAccountsField(feishuCfg);
  const resolvedAccountId = normalizeAccountId(accountId);
  const accountCfg = resolveAccountConfig(cfg, resolvedAccountId) ?? {};
  return { ...base, ...accountCfg };
}

export function listFeishuAccountIds(cfg: OpenClawConfig): string[] {
  const feishuCfg = cfg.channels?.feishu as FeishuConfig | undefined;
  const accounts = feishuCfg?.accounts;
  if (!accounts || typeof accounts !== "object") {
    return [DEFAULT_ACCOUNT_ID];
  }
  const ids = Object.keys(accounts).filter(Boolean);
  return ids.length > 0 ? ids.sort() : [DEFAULT_ACCOUNT_ID];
}

export function resolveDefaultFeishuAccountId(cfg: OpenClawConfig): string {
  const ids = listFeishuAccountIds(cfg);
  if (ids.includes(DEFAULT_ACCOUNT_ID)) {
    return DEFAULT_ACCOUNT_ID;
  }
  return ids[0] ?? DEFAULT_ACCOUNT_ID;
}

export function resolveFeishuAccount(params: {
  cfg: OpenClawConfig;
  accountId?: string | null;
}): {
  accountId: string;
  enabled: boolean;
  configured: boolean;
  name?: string;
  appId?: string;
  appSecret?: string;
  domain?: string;
  tokenSource: "appSecret" | "appSecretFile" | "none";
  config: FeishuAccountConfig;
} {
  const accountId = normalizeAccountId(params.accountId);
  const merged = mergeFeishuConfig(params.cfg, accountId);

  const enabled = (merged as { enabled?: boolean }).enabled !== false;
  const appId = typeof merged.appId === "string" ? merged.appId.trim() : "";
  let appSecret = typeof merged.appSecret === "string" ? merged.appSecret.trim() : "";
  let tokenSource: "appSecret" | "appSecretFile" | "none" = appSecret ? "appSecret" : "none";

  if (!appSecret && merged.appSecretFile) {
    const fileSecret = resolveSecretFromFile(merged.appSecretFile);
    if (fileSecret) {
      appSecret = fileSecret;
      tokenSource = "appSecretFile";
      merged.appSecret = fileSecret;
    }
  }

  const configured = Boolean(appId && appSecret);
  const name = typeof merged.name === "string" ? merged.name.trim() : undefined;
  const domain =
    typeof merged.domain === "string" && merged.domain.trim()
      ? merged.domain.trim()
      : undefined;

  return {
    accountId,
    enabled,
    configured,
    name,
    appId: appId || undefined,
    appSecret: appSecret || undefined,
    domain,
    tokenSource,
    config: merged,
  };
}

export function resolveFeishuConfig(params: {
  cfg: OpenClawConfig;
  accountId?: string | null;
}): ResolvedFeishuConfig {
  const merged = mergeFeishuConfig(params.cfg, params.accountId);
  const enabled = (merged as { enabled?: boolean }).enabled !== false;
  const dmPolicy = (merged as { dmPolicy?: DmPolicy }).dmPolicy ?? "pairing";
  const groupPolicy = (merged as { groupPolicy?: GroupPolicy }).groupPolicy ?? "open";
  const allowFrom = Array.isArray(merged.allowFrom) ? merged.allowFrom : [];
  const groupAllowFrom = Array.isArray(merged.groupAllowFrom) ? merged.groupAllowFrom : [];
  const historyLimit = Number.isFinite(merged.historyLimit)
    ? Number(merged.historyLimit)
    : DEFAULT_HISTORY_LIMIT;
  const dmHistoryLimit = Number.isFinite(merged.dmHistoryLimit)
    ? Number(merged.dmHistoryLimit)
    : historyLimit;
  const textChunkLimit = Number.isFinite(merged.textChunkLimit)
    ? Number(merged.textChunkLimit)
    : DEFAULT_TEXT_CHUNK_LIMIT;
  const chunkMode = merged.chunkMode === "newline" ? "newline" : "length";
  const blockStreaming = (merged as { blockStreaming?: boolean }).blockStreaming !== false;
  const streaming = (merged as { streaming?: boolean }).streaming === true;
  const mediaMaxMb = Number.isFinite(merged.mediaMaxMb)
    ? Number(merged.mediaMaxMb)
    : DEFAULT_MEDIA_MAX_MB;
  const groups =
    merged.groups && typeof merged.groups === "object" ? (merged.groups as ResolvedFeishuConfig["groups"]) : {};

  return {
    enabled,
    dmPolicy,
    groupPolicy,
    allowFrom,
    groupAllowFrom,
    historyLimit,
    dmHistoryLimit,
    textChunkLimit,
    chunkMode,
    blockStreaming,
    streaming,
    mediaMaxMb,
    groups,
  };
}

function resolveGroupConfig(groups: ResolvedFeishuConfig["groups"], chatId?: string | null) {
  const id = (chatId ?? "").trim();
  if (!id) {
    return undefined;
  }
  if (groups[id]) {
    return groups[id];
  }
  const lowered = id.toLowerCase();
  const matchKey = Object.keys(groups).find((key) => key.toLowerCase() === lowered);
  return matchKey ? groups[matchKey] : undefined;
}

export function resolveFeishuGroupRequireMention(params: {
  cfg: OpenClawConfig;
  accountId?: string | null;
  chatId?: string | null;
}): boolean {
  const resolved = resolveFeishuConfig({ cfg: params.cfg, accountId: params.accountId });
  const group = resolveGroupConfig(resolved.groups, params.chatId);
  if (group && typeof group.requireMention === "boolean") {
    return group.requireMention;
  }
  return true;
}

export function normalizeFeishuTarget(raw: string): string | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }

  const lowered = trimmed.toLowerCase();
  if (lowered.startsWith("chat:")) {
    return trimmed.slice("chat:".length).trim() || null;
  }
  if (lowered.startsWith("user:")) {
    return trimmed.slice("user:".length).trim() || null;
  }
  if (lowered.startsWith("open_id:")) {
    return trimmed.slice("open_id:".length).trim() || null;
  }

  return trimmed;
}

function normalizeFeishuDomain(domain?: string | null): string | undefined {
  const trimmed = (domain ?? "").trim();
  if (!trimmed) {
    return undefined;
  }
  const lower = trimmed.toLowerCase();
  if (lower === "feishu" || lower === "cn" || lower === "china") {
    return "https://open.feishu.cn";
  }
  if (lower === "lark" || lower === "global" || lower === "intl" || lower === "international") {
    return "https://open.larksuite.com";
  }
  const withScheme = /^https?:\/\//i.test(trimmed) ? trimmed : `https://${trimmed}`;
  return withScheme.replace(/\/+$/, "").replace(/\/open-apis$/i, "");
}

export async function probeFeishu(
  appId?: string | null,
  appSecret?: string | null,
  timeoutMs?: number,
  domain?: string | null,
): Promise<{ ok: boolean; error?: string; appId?: string; botName?: string; botOpenId?: string }> {
  const appIdTrimmed = (appId ?? "").trim();
  const appSecretTrimmed = (appSecret ?? "").trim();
  if (!appIdTrimmed || !appSecretTrimmed) {
    return { ok: false, error: "missing credentials (appId, appSecret)" };
  }

  const client = new Lark.Client({
    appId: appIdTrimmed,
    appSecret: appSecretTrimmed,
    ...(domain ? { domain: normalizeFeishuDomain(domain) } : {}),
  });

  const timeout = Number.isFinite(timeoutMs) && (timeoutMs as number) > 0 ? Number(timeoutMs) : 8000;
  const request = (client as unknown as { request: (args: Record<string, unknown>) => Promise<unknown> })
    .request({
      method: "GET",
      url: "/open-apis/bot/v3/info",
      data: {},
    });

  let response: unknown;
  try {
    response = await Promise.race([
      request,
      new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), timeout)),
    ]);
  } catch (err) {
    return { ok: false, appId: appIdTrimmed, error: err instanceof Error ? err.message : String(err) };
  }

  const res = response as { code?: number; msg?: string; bot?: { bot_name?: string; open_id?: string }; data?: { bot?: { bot_name?: string; open_id?: string } } };
  if (res?.code !== 0) {
    return { ok: false, appId: appIdTrimmed, error: `API error: ${res?.msg || `code ${res?.code}`}` };
  }
  const bot = res?.bot || res?.data?.bot;
  return { ok: true, appId: appIdTrimmed, botName: bot?.bot_name, botOpenId: bot?.open_id };
}
