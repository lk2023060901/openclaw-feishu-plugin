import * as Lark from "@larksuiteoapi/node-sdk";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import type { ChannelLogSink, OpenClawConfig, PluginRuntime } from "openclaw/plugin-sdk";
import { extensionForMime, resolveFeishuConfig } from "openclaw/plugin-sdk";

const DEFAULT_THINKING_TEXT = process.env.FEISHU_THINKING_TEXT || "Thinking...";
const THINKING_THRESHOLD_MS = Number(process.env.FEISHU_THINKING_THRESHOLD_MS ?? 2500);
const MAX_LOCAL_FILE_MB = Number(process.env.FEISHU_BRIDGE_MAX_LOCAL_FILE_MB ?? 15);
const MAX_ATTACHMENTS = Number(process.env.FEISHU_BRIDGE_MAX_ATTACHMENTS ?? 4);
const DEFAULT_LOG_LEVEL = (process.env.FEISHU_BRIDGE_LOG_LEVEL ||
  process.env.FEISHU_PLUGIN_LOG_LEVEL ||
  "warn").toLowerCase();
const DEFAULT_LOG_PATH =
  process.env.FEISHU_BRIDGE_LOG_PATH ||
  process.env.FEISHU_PLUGIN_LOG_PATH ||
  "~/.openclaw/feishu-plugin/plugin.log";
const DEBUG_ENABLED =
  process.env.FEISHU_BRIDGE_DEBUG === "1" ||
  process.env.FEISHU_BRIDGE_DEBUG === "true" ||
  process.env.FEISHU_PLUGIN_DEBUG === "1" ||
  process.env.FEISHU_PLUGIN_DEBUG === "true";

const LOG_LEVELS: Record<string, number> = {
  error: 0,
  warn: 1,
  info: 2,
  debug: 3,
};

type EventLogger = {
  logEvent: (level: "debug" | "info" | "warn" | "error", event: string, details?: Record<string, unknown>) => void;
  logWarnEvent: (event: string, details?: Record<string, unknown>, err?: unknown) => void;
  logErrorEvent: (event: string, details?: Record<string, unknown>, err?: unknown) => void;
  logInfoEvent: (event: string, details?: Record<string, unknown>) => void;
  logDebugEvent: (event: string, details?: Record<string, unknown>) => void;
};

type DmPolicy = "pairing" | "allowlist" | "open" | "disabled";
type GroupPolicy = "open" | "allowlist" | "disabled";

type ResolvedFeishuConfig = {
  enabled: boolean;
  dmPolicy: DmPolicy;
  groupPolicy: GroupPolicy;
  allowFrom: string[];
  groupAllowFrom: string[];
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

type FeishuSender = {
  sender_id?: {
    open_id?: string;
    user_id?: string;
    union_id?: string;
  };
};

type FeishuMention = {
  key?: string;
};

type FeishuMessage = {
  chat_id?: string;
  chat_type?: string;
  message_type?: string;
  content?: string;
  mentions?: FeishuMention[];
  create_time?: string | number;
  message_id?: string;
};

type FeishuEventPayload = {
  message?: FeishuMessage;
  event?: {
    message?: FeishuMessage;
    sender?: FeishuSender;
  };
  sender?: FeishuSender;
  mentions?: FeishuMention[];
};

type FeishuMediaRef = {
  path: string;
  contentType?: string;
  placeholder: string;
};

type NormalizedAllowFrom = {
  entries: string[];
  entriesLower: string[];
  hasWildcard: boolean;
  hasEntries: boolean;
};

const SUPPORTED_MSG_TYPES = new Set([
  "text",
  "post",
  "image",
  "file",
  "audio",
  "media",
  "sticker",
]);

const SEEN_TTL_MS = 10 * 60 * 1000;
const seenMessages = new Map<string, number>();

function isDuplicate(messageId?: string) {
  if (!messageId) {
    return false;
  }
  const now = Date.now();
  for (const [key, ts] of seenMessages) {
    if (now - ts > SEEN_TTL_MS) {
      seenMessages.delete(key);
    }
  }
  if (seenMessages.has(messageId)) {
    return true;
  }
  seenMessages.set(messageId, now);
  return false;
}

function formatErr(err: unknown): string {
  if (err instanceof Error) {
    return err.message;
  }
  return String(err);
}

function normalizeLogLevel(value: string | undefined): "debug" | "info" | "warn" | "error" {
  const raw = String(value ?? "").trim().toLowerCase();
  if (raw === "debug" || raw === "info" || raw === "warn" || raw === "error") {
    return raw;
  }
  if (raw === "warning") {
    return "warn";
  }
  return "warn";
}

function safeJsonStringify(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function coerceLogMessage(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (value instanceof Error) {
    return value.message;
  }
  return safeJsonStringify(value);
}

function serializeError(err: unknown): Record<string, unknown> | undefined {
  if (!err) {
    return undefined;
  }
  if (err instanceof Error) {
    const payload: Record<string, unknown> = {
      name: err.name,
      message: err.message,
      stack: err.stack,
    };
    const anyErr = err as Error & { code?: unknown; cause?: unknown };
    if (anyErr.code != null) {
      payload.code = anyErr.code;
    }
    if (anyErr.cause != null) {
      payload.cause =
        anyErr.cause instanceof Error
          ? { name: anyErr.cause.name, message: anyErr.cause.message, stack: anyErr.cause.stack }
          : safeJsonStringify(anyErr.cause);
    }
    return payload;
  }
  if (typeof err === "object") {
    return { message: safeJsonStringify(err) };
  }
  return { message: String(err) };
}

function createEventLogger(params: {
  resolvePath: (input: string) => string;
  log?: ChannelLogSink;
  accountId?: string;
}): EventLogger {
  let logDirReady = false;
  let resolvedPath: string | null = null;
  const level = DEBUG_ENABLED ? "debug" : normalizeLogLevel(DEFAULT_LOG_LEVEL);

  if (DEFAULT_LOG_PATH && DEFAULT_LOG_PATH.trim()) {
    try {
      resolvedPath = params.resolvePath(DEFAULT_LOG_PATH.trim());
    } catch {
      resolvedPath = path.join(os.homedir(), ".openclaw", "feishu-plugin", "plugin.log");
    }
  }

  const shouldLog = (lvl: "debug" | "info" | "warn" | "error") => {
    const current = LOG_LEVELS[level] ?? LOG_LEVELS.warn;
    const value = LOG_LEVELS[lvl] ?? LOG_LEVELS.warn;
    return value <= current;
  };

  const ensureLogDir = () => {
    if (!resolvedPath || logDirReady) {
      return;
    }
    try {
      fs.mkdirSync(path.dirname(resolvedPath), { recursive: true });
      logDirReady = true;
    } catch {
      // ignore
    }
  };

  const emitGateway = (
    lvl: "debug" | "info" | "warn" | "error",
    event: string,
    details?: Record<string, unknown>,
  ) => {
    if (!params.log) {
      return;
    }
    const payload = details ? safeJsonStringify(details) : "";
    const message = payload
      ? `[feishu-plugin] ${event} ${payload}`
      : `[feishu-plugin] ${event}`;
    if (lvl === "debug") {
      params.log.debug?.(message);
    } else if (lvl === "info") {
      params.log.info?.(message);
    } else if (lvl === "error") {
      params.log.error?.(message);
    } else {
      params.log.warn?.(message);
    }
  };

  const logEvent = (
    lvl: "debug" | "info" | "warn" | "error",
    event: string,
    details: Record<string, unknown> = {},
  ) => {
    if (!shouldLog(lvl)) {
      return;
    }
    const payload = {
      ts: new Date().toISOString(),
      level: lvl,
      event,
      accountId: params.accountId,
      ...details,
    };
    emitGateway(lvl, event, payload);
    if (!resolvedPath) {
      return;
    }
    try {
      ensureLogDir();
      fs.appendFileSync(resolvedPath, `${JSON.stringify(payload)}\n`);
    } catch {
      // ignore
    }
  };

  const logWarnEvent = (event: string, details: Record<string, unknown> = {}, err?: unknown) => {
    const payload = { ...details };
    if (err !== undefined) {
      payload.error = serializeError(err);
    }
    logEvent("warn", event, payload);
  };

  const logErrorEvent = (event: string, details: Record<string, unknown> = {}, err?: unknown) => {
    const payload = { ...details };
    if (err !== undefined) {
      payload.error = serializeError(err);
    }
    logEvent("error", event, payload);
  };

  return {
    logEvent,
    logWarnEvent,
    logErrorEvent,
    logInfoEvent: (event, details) => logEvent("info", event, details),
    logDebugEvent: (event, details) => logEvent("debug", event, details),
  };
}

function normalizeFeishuDomain(value?: string | null): string | undefined {
  const trimmed = value?.trim();
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

function buildSdkLogger(log?: ChannelLogSink) {
  return {
    debug: (msg: unknown) => log?.debug?.(coerceLogMessage(msg)),
    info: (msg: unknown) => log?.info?.(coerceLogMessage(msg)),
    warn: (msg: unknown) => log?.warn?.(coerceLogMessage(msg)),
    error: (msg: unknown) => log?.error?.(coerceLogMessage(msg)),
    trace: (msg: unknown) => log?.debug?.(coerceLogMessage(msg)),
  };
}

export function createFeishuClient(params: {
  appId: string;
  appSecret: string;
  domain?: string | null;
  log?: ChannelLogSink;
}): Lark.Client {
  const domain = normalizeFeishuDomain(params.domain);
  return new Lark.Client({
    appId: params.appId,
    appSecret: params.appSecret,
    ...(domain ? { domain } : {}),
    logger: buildSdkLogger(params.log),
  });
}

function createFeishuWsClient(params: {
  appId: string;
  appSecret: string;
  domain?: string | null;
  log?: ChannelLogSink;
}): Lark.WSClient {
  const domain = normalizeFeishuDomain(params.domain);
  return new Lark.WSClient({
    appId: params.appId,
    appSecret: params.appSecret,
    ...(domain ? { domain } : {}),
    loggerLevel: Lark.LoggerLevel.info,
    logger: buildSdkLogger(params.log),
  });
}

function decodeHtmlEntities(input: string) {
  return String(input ?? "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, "\"")
    .replace(/&#39;/g, "'");
}

function normalizeFeishuText(raw: string) {
  let text = String(raw ?? "");
  text = text.replace(/<\s*br\s*\/?>/gi, "\n");
  text = text.replace(/<\s*\/p\s*>\s*<\s*p\s*>/gi, "\n");
  text = text.replace(/<\s*p\s*>/gi, "");
  text = text.replace(/<\s*\/p\s*>/gi, "");
  text = text.replace(/<[^>]+>/g, "");
  text = decodeHtmlEntities(text);
  text = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  text = text.replace(/\n{3,}/g, "\n\n");
  text = text.replace(/(^|\n)([-*•])\n(?=\S)/g, "$1$2 ");
  text = text.replace(/(^|\n)(\d+[\.|\)])\n(?=\S)/g, "$1$2 ");
  return text.trim();
}

function extractJsonObject(text: string, startIndex: number): { json: string; end: number } | null {
  let depth = 0;
  let inString = false;
  let escape = false;
  for (let i = startIndex; i < text.length; i += 1) {
    const ch = text[i];
    if (inString) {
      if (escape) {
        escape = false;
        continue;
      }
      if (ch === "\\") {
        escape = true;
        continue;
      }
      if (ch === "\"") {
        inString = false;
      }
      continue;
    }
    if (ch === "\"") {
      inString = true;
      continue;
    }
    if (ch === "{") {
      depth += 1;
    }
    if (ch === "}") {
      depth -= 1;
      if (depth === 0) {
        return { json: text.slice(startIndex, i + 1), end: i };
      }
    }
  }
  return null;
}

function parseCardFromText(text: string) {
  const raw = String(text || "");
  const marker = "CARD_JSON:";
  const markerIndex = raw.indexOf(marker);
  if (markerIndex < 0) {
    return null;
  }
  const after = raw.slice(markerIndex + marker.length);
  const braceOffset = after.indexOf("{");
  if (braceOffset < 0) {
    return null;
  }
  const start = markerIndex + marker.length + braceOffset;
  const extracted = extractJsonObject(raw, start);
  if (!extracted) {
    return null;
  }
  let card: unknown;
  try {
    card = JSON.parse(extracted.json);
  } catch {
    return null;
  }
  const before = raw.slice(0, markerIndex);
  const afterText = raw.slice(extracted.end + 1);
  const merged = `${before}\n${afterText}`.replace(/\n{3,}/g, "\n\n").trim();
  return { card, text: merged };
}

function normalizeCardContent(card: unknown) {
  if (card == null) {
    return "";
  }
  if (typeof card === "string") {
    const trimmed = card.trim();
    if (!trimmed) {
      return trimmed;
    }
    if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
      try {
        return JSON.stringify(JSON.parse(trimmed));
      } catch {
        return trimmed;
      }
    }
    return trimmed;
  }
  return JSON.stringify(card);
}

function parseMediaLines(replyText: string) {
  const text = String(replyText ?? "");
  const lines = text.split(/\r?\n/);
  const media: string[] = [];
  const kept: string[] = [];

  const pushMedia = (raw: string) => {
    let value = String(raw || "").trim();
    if (!value) {
      return;
    }
    value = value.replace(/^</, "").replace(/>$/, "").replace(/[),.;，。；]+$/, "").trim();
    if (!value) {
      return;
    }
    media.push(value);
  };

  for (const line of lines) {
    const m = line.match(/^\s*MEDIA\s*[:：]\s*(.+?)\s*$/i);
    if (m) {
      pushMedia(m[1]);
      continue;
    }
    const inlineRe = /MEDIA\s*[:：]\s*(\S+)/gi;
    let mm: RegExpExecArray | null;
    let foundInline = false;
    while ((mm = inlineRe.exec(line))) {
      foundInline = true;
      pushMedia(mm[1]);
    }
    if (foundInline) {
      kept.push(line.replace(inlineRe, "").trim());
      continue;
    }
    kept.push(line);
  }

  return { text: kept.join("\n").trim(), mediaUrls: [...new Set(media)] };
}

function extractMarkdownLocalMediaPaths(text: string): string[] {
  const out: string[] = [];
  const mdImageRe = /!\[[^\]]*\]\(([^)]+)\)/g;
  let match: RegExpExecArray | null;
  while ((match = mdImageRe.exec(text))) {
    const raw = (match[1] || "").trim().replace(/^</, "").replace(/>$/, "");
    if (!raw) {
      continue;
    }
    if (raw.startsWith("file://")) {
      out.push(raw.replace("file://", ""));
    } else if (raw.startsWith("/")) {
      out.push(raw);
    } else if (raw.startsWith("~")) {
      out.push(raw);
    }
  }
  const barePathRe = /\/(Users|home|tmp)\/[^\s)]+\.(png|jpg|jpeg|gif|webp|bmp)/gi;
  while ((match = barePathRe.exec(text))) {
    out.push(match[0]);
  }
  return [...new Set(out)];
}

function stripMarkdownLocalMediaRefs(text: string) {
  return String(text ?? "")
    .replace(/!\[[^\]]*\]\(([^)]+)\)/g, "[image]")
    .replace(/\/(Users|home)\/[^\s)]+\.(png|jpg|jpeg|gif|webp|bmp)/gi, "[image]")
    .trim();
}

function isPathInside(child: string, parent: string) {
  const rel = path.relative(parent, child);
  return !!rel && !rel.startsWith("..") && !path.isAbsolute(rel);
}

function parsePathList(raw: string | undefined, resolvePath: (input: string) => string) {
  return (raw ?? "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => resolvePath(entry));
}

function resolveAllowedOutboundDirs(resolvePath: (input: string) => string) {
  const fallback = `${resolvePath("~/.openclaw/media")},/tmp`;
  return parsePathList(process.env.FEISHU_BRIDGE_ALLOWED_OUTBOUND_MEDIA_DIRS || fallback, resolvePath);
}

function isAllowedOutboundPath(filePath: string, allowedDirs: string[]) {
  const resolved = path.resolve(filePath);
  return allowedDirs.some((dir) => resolved === dir || isPathInside(resolved, dir));
}

function extLower(p: string) {
  return path.extname(p || "").toLowerCase().replace(/^\./, "");
}

function guessMimeByExt(p: string) {
  const e = extLower(p);
  if (e === "png") return "image/png";
  if (e === "jpg" || e === "jpeg") return "image/jpeg";
  if (e === "gif") return "image/gif";
  if (e === "webp") return "image/webp";
  if (e === "mp4") return "video/mp4";
  if (e === "mov") return "video/quicktime";
  if (e === "mp3") return "audio/mpeg";
  if (e === "wav") return "audio/wav";
  if (e === "m4a") return "audio/mp4";
  if (e === "opus") return "audio/opus";
  return "application/octet-stream";
}

function isProbablyImagePath(p: string) {
  return ["png", "jpg", "jpeg", "gif", "webp", "bmp"].includes(extLower(p));
}

function isProbablyVideoPath(p: string) {
  return ["mp4", "mov"].includes(extLower(p));
}

function isProbablyAudioPath(p: string) {
  return ["mp3", "wav", "m4a", "opus", "ogg"].includes(extLower(p));
}

function safeFileSizeOk(filePath: string) {
  try {
    const stat = fs.statSync(filePath);
    if (!stat.isFile()) {
      return { ok: false, reason: "not_file" };
    }
    const maxBytes = MAX_LOCAL_FILE_MB * 1024 * 1024;
    if (stat.size > maxBytes) {
      return { ok: false, reason: "too_large" };
    }
    return { ok: true };
  } catch {
    return { ok: false, reason: "stat_failed" };
  }
}

function toNodeReadableStream(maybeStream: unknown): Readable | null {
  if (!maybeStream) {
    return null;
  }
  if (typeof (maybeStream as { pipe?: unknown }).pipe === "function") {
    return maybeStream as Readable;
  }
  if (
    typeof (maybeStream as { getReader?: unknown }).getReader === "function" &&
    typeof Readable.fromWeb === "function"
  ) {
    return Readable.fromWeb(maybeStream as any);
  }
  return null;
}

async function readStreamWithLimit(stream: Readable, maxBytes: number): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let total = 0;
  for await (const chunk of stream) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    total += buffer.length;
    if (total > maxBytes) {
      throw new Error(`resource exceeds ${Math.round(maxBytes / (1024 * 1024))}MB limit`);
    }
    chunks.push(buffer);
  }
  return Buffer.concat(chunks);
}

async function readResponseToBuffer(payload: unknown, maxBytes: number): Promise<Buffer> {
  const stream = toNodeReadableStream(payload);
  if (stream) {
    return await readStreamWithLimit(stream, maxBytes);
  }
  if (Buffer.isBuffer(payload)) {
    if (payload.length > maxBytes) {
      throw new Error(`resource exceeds ${Math.round(maxBytes / (1024 * 1024))}MB limit`);
    }
    return payload;
  }
  if (payload instanceof ArrayBuffer) {
    const buffer = Buffer.from(payload);
    if (buffer.length > maxBytes) {
      throw new Error(`resource exceeds ${Math.round(maxBytes / (1024 * 1024))}MB limit`);
    }
    return buffer;
  }
  if (ArrayBuffer.isView(payload)) {
    const buffer = Buffer.from(payload.buffer, payload.byteOffset, payload.byteLength);
    if (buffer.length > maxBytes) {
      throw new Error(`resource exceeds ${Math.round(maxBytes / (1024 * 1024))}MB limit`);
    }
    return buffer;
  }
  throw new Error("unexpected response payload");
}

async function downloadFeishuMessageResource(params: {
  client: Lark.Client;
  runtime: PluginRuntime;
  messageId: string;
  fileKey: string;
  type: "image" | "file" | "audio" | "video";
  maxBytes: number;
  fileName?: string;
}): Promise<FeishuMediaRef> {
  const res = await params.client.im.messageResource.get({
    params: { type: params.type },
    path: { message_id: params.messageId, file_key: params.fileKey },
  });

  let streamSource: unknown = res as unknown;
  const resObj = res as Record<string, unknown> | null;
  if (resObj && typeof resObj.getReadableStream === "function") {
    streamSource = (resObj as { getReadableStream: () => unknown }).getReadableStream();
  } else if (resObj && "data" in resObj) {
    const data = resObj.data as Record<string, unknown> | undefined;
    if (data && typeof data.getReadableStream === "function") {
      streamSource = (data as { getReadableStream: () => unknown }).getReadableStream();
    } else {
      streamSource = data ?? resObj.data;
    }
  }

  const buffer = await readResponseToBuffer(streamSource, params.maxBytes);
  const headers =
    (resObj && typeof resObj.headers === "object" ? (resObj.headers as Record<string, string>) : undefined) ??
    undefined;
  const contentType =
    headers?.["content-type"] || headers?.["Content-Type"] || guessMimeByExt(params.fileName ?? "");

  const saved = await params.runtime.channel.media.saveMediaBuffer(
    buffer,
    contentType,
    "inbound",
    params.maxBytes,
    params.fileName,
  );

  return {
    path: saved.path,
    contentType: saved.contentType,
    placeholder: params.type === "image"
      ? "<media:image>"
      : params.type === "audio"
        ? "<media:audio>"
        : params.type === "video"
          ? "<media:video>"
          : "<media:document>",
  };
}

function normalizeAllowFrom(list?: Array<string | number>): NormalizedAllowFrom {
  const entries = (list ?? []).map((value) => String(value).trim()).filter(Boolean);
  const hasWildcard = entries.includes("*");
  const normalized = entries
    .filter((value) => value !== "*")
    .map((value) => value.replace(/^(feishu|lark):/i, ""));
  const normalizedLower = normalized.map((value) => value.toLowerCase());
  return {
    entries: normalized,
    entriesLower: normalizedLower,
    hasWildcard,
    hasEntries: entries.length > 0,
  };
}

function normalizeAllowFromWithStore(params: {
  allowFrom?: Array<string | number>;
  storeAllowFrom?: string[];
}): NormalizedAllowFrom {
  const combined = [...(params.allowFrom ?? []), ...(params.storeAllowFrom ?? [])]
    .map((value) => String(value).trim())
    .filter(Boolean);
  return normalizeAllowFrom(combined);
}

function isSenderAllowed(params: { allow: NormalizedAllowFrom; senderId?: string }) {
  const { allow, senderId } = params;
  if (!allow.hasEntries) {
    return true;
  }
  if (allow.hasWildcard) {
    return true;
  }
  if (senderId && allow.entries.includes(senderId)) {
    return true;
  }
  if (senderId && allow.entriesLower.includes(senderId.toLowerCase())) {
    return true;
  }
  return false;
}

function extractFromPostJson(postJson: Record<string, unknown>) {
  const lines: string[] = [];
  const imageKeys: string[] = [];

  const pushLine = (value: string) => {
    const trimmed = String(value ?? "").trimEnd();
    if (trimmed.trim()) {
      lines.push(trimmed);
    }
  };

  const inline = (node: unknown): string => {
    if (!node) {
      return "";
    }
    if (Array.isArray(node)) {
      return node.map(inline).join("");
    }
    if (typeof node !== "object") {
      return "";
    }
    const record = node as Record<string, unknown>;
    const tag = record.tag;
    if (typeof tag === "string") {
      if (tag === "text") return String(record.text ?? "");
      if (tag === "a") return String(record.text ?? record.href ?? "");
      if (tag === "at") return record.user_name ? `@${record.user_name}` : "@";
      if (tag === "md") return String(record.text ?? "");
      if (tag === "img") {
        if (record.image_key) imageKeys.push(String(record.image_key));
        return "[image]";
      }
      if (tag === "file") return "[file]";
      if (tag === "media") return "[video]";
      if (tag === "hr") return "\n";
      if (tag === "code_block") {
        const lang = String(record.language || "").trim();
        const code = String(record.text || "");
        return `\n\n\`\`\`${lang ? ` ${lang}` : ""}\n${code}\n\`\`\`\n\n`;
      }
    }
    let acc = "";
    for (const value of Object.values(record)) {
      if (value && (typeof value === "object" || Array.isArray(value))) {
        acc += inline(value);
      }
    }
    return acc;
  };

  if (postJson?.title) {
    pushLine(normalizeFeishuText(String(postJson.title)));
  }

  const content = postJson?.content;
  if (Array.isArray(content)) {
    for (const paragraph of content) {
      if (Array.isArray(paragraph)) {
        const joined = paragraph.map(inline).join("");
        const normalized = normalizeFeishuText(joined);
        if (normalized) {
          pushLine(normalized);
        }
      } else {
        const normalized = normalizeFeishuText(inline(paragraph));
        if (normalized) {
          pushLine(normalized);
        }
      }
    }
  } else if (content) {
    const normalized = normalizeFeishuText(inline(content));
    if (normalized) {
      pushLine(normalized);
    }
  }

  const text = lines.join("\n").replace(/\n{3,}/g, "\n\n").trim();
  return { text, imageKeys: [...new Set(imageKeys)] };
}

async function sendText(
  client: Lark.Client,
  receiveId: string,
  text: string,
  receiveIdType: "chat_id" | "open_id" | "union_id" = "chat_id",
): Promise<string | null> {
  const res = await client.im.v1.message.create({
    params: { receive_id_type: receiveIdType },
    data: { receive_id: receiveId, msg_type: "text", content: JSON.stringify({ text }) },
  });
  const data = (res as { data?: { message_id?: string } }).data;
  return data?.message_id ?? null;
}

async function sendCard(
  client: Lark.Client,
  receiveId: string,
  card: unknown,
  receiveIdType: "chat_id" | "open_id" | "union_id" = "chat_id",
) {
  const content = normalizeCardContent(card);
  if (!content) {
    return null;
  }
  const res = await client.im.v1.message.create({
    params: { receive_id_type: receiveIdType },
    data: { receive_id: receiveId, msg_type: "interactive", content },
  });
  const data = (res as { data?: { message_id?: string } }).data;
  return data?.message_id ?? null;
}

async function updateTextMessage(client: Lark.Client, messageId: string, text: string) {
  return client.im.v1.message.update({
    path: { message_id: messageId },
    data: { msg_type: "text", content: JSON.stringify({ text }) },
  });
}

async function deleteMessage(client: Lark.Client, messageId: string) {
  return client.im.v1.message.delete({ path: { message_id: messageId } });
}

async function downloadUrlToTempFile(runtime: PluginRuntime, url: string) {
  const fetched = await runtime.channel.media.fetchRemoteMedia({
    url,
    maxBytes: MAX_LOCAL_FILE_MB * 1024 * 1024,
  });
  const ext = extensionForMime(fetched.contentType ?? undefined) || path.extname(url) || ".bin";
  const tmp = path.join(
    os.tmpdir(),
    `feishu_out_${Date.now()}_${Math.random().toString(16).slice(2)}${ext}`,
  );
  await fs.promises.writeFile(tmp, fetched.buffer);
  return { path: tmp, contentType: fetched.contentType };
}

async function uploadAndSendMedia(params: {
  client: Lark.Client;
  runtime: PluginRuntime;
  receiveId: string;
  receiveIdType: "chat_id" | "open_id" | "union_id";
  mediaUrlOrPath: string;
  captionText?: string;
  allowedOutboundDirs: string[];
  log?: ChannelLogSink;
  eventLogger?: EventLogger;
}): Promise<boolean> {
  let tempPath: string | null = null;
  let localPath: string | null = null;

  const raw = String(params.mediaUrlOrPath || "").trim();
  if (!raw) {
    return false;
  }

  try {
    if (raw.startsWith("file://")) {
      localPath = raw.replace("file://", "");
    } else if (raw === "~") {
      localPath = os.homedir();
    } else if (raw.startsWith("~/")) {
      localPath = path.join(os.homedir(), raw.slice(2));
    } else if (raw.startsWith("~")) {
      localPath = path.resolve(os.homedir(), raw.slice(1));
    } else if (raw.startsWith("./") || raw.startsWith("../")) {
      for (const baseDir of params.allowedOutboundDirs) {
        const candidate = path.resolve(baseDir, raw);
        if (isAllowedOutboundPath(candidate, params.allowedOutboundDirs)) {
          localPath = candidate;
          break;
        }
      }
    } else if (raw.startsWith("/")) {
      localPath = raw;
    } else if (raw.startsWith("http://") || raw.startsWith("https://")) {
      const downloaded = await downloadUrlToTempFile(params.runtime, raw);
      tempPath = downloaded.path;
      localPath = tempPath;
    } else if (raw.startsWith("data:")) {
      const match = raw.match(/^data:([^;]+);base64,(.*)$/);
      if (!match) {
        await sendText(params.client, params.receiveId, params.captionText || raw, params.receiveIdType);
        return true;
      }
      const mime = match[1];
      const b64 = match[2];
      const ext =
        mime.includes("png")
          ? "png"
          : mime.includes("jpeg") || mime.includes("jpg")
            ? "jpg"
            : mime.includes("webp")
              ? "webp"
              : "bin";
      tempPath = path.join(
        os.tmpdir(),
        `feishu_out_${Date.now()}_${Math.random().toString(16).slice(2)}.${ext}`,
      );
      fs.writeFileSync(tempPath, Buffer.from(b64, "base64"));
      localPath = tempPath;
    } else {
      await sendText(params.client, params.receiveId, params.captionText || raw, params.receiveIdType);
      return true;
    }

    if (!localPath) {
      return false;
    }

    const resolved = path.resolve(localPath);
    if (resolved.startsWith("/") && !tempPath) {
      if (!isAllowedOutboundPath(resolved, params.allowedOutboundDirs)) {
        params.eventLogger?.logWarnEvent("outbound_media_blocked", {
          reason: "not_allowed",
          path: resolved,
          receiveIdType: params.receiveIdType,
          receiveId: params.receiveId,
        });
        params.log?.warn?.(`feishu outbound media blocked (not allowed): ${resolved}`);
        return false;
      }
      const ok = safeFileSizeOk(resolved);
      if (!ok.ok) {
        params.eventLogger?.logWarnEvent("outbound_media_blocked", {
          reason: ok.reason || "size_or_read_error",
          path: resolved,
          receiveIdType: params.receiveIdType,
          receiveId: params.receiveId,
        });
        params.log?.warn?.(`feishu outbound media blocked (${ok.reason}): ${resolved}`);
        return false;
      }
    }

    if (isProbablyImagePath(resolved)) {
      const res = await params.client.im.image.create({
        data: { image_type: "message", image: fs.createReadStream(resolved) },
      });
      const imageKey = (res as { data?: { image_key?: string } }).data?.image_key || (res as { image_key?: string }).image_key;
      if (!imageKey) {
        throw new Error("upload image failed");
      }
      await params.client.im.v1.message.create({
        params: { receive_id_type: params.receiveIdType },
        data: { receive_id: params.receiveId, msg_type: "image", content: JSON.stringify({ image_key: imageKey }) },
      });
      if (params.captionText?.trim()) {
        await sendText(params.client, params.receiveId, params.captionText.trim(), params.receiveIdType);
      }
      return true;
    }

    if (isProbablyVideoPath(resolved) && extLower(resolved) === "mp4") {
      const res = await params.client.im.file.create({
        data: { file_type: "mp4", file_name: path.basename(resolved), file: fs.createReadStream(resolved) },
      });
      const fileKey = (res as { data?: { file_key?: string } }).data?.file_key || (res as { file_key?: string }).file_key;
      if (!fileKey) {
        throw new Error("upload mp4 failed");
      }
      await params.client.im.v1.message.create({
        params: { receive_id_type: params.receiveIdType },
        data: { receive_id: params.receiveId, msg_type: "media", content: JSON.stringify({ file_key: fileKey }) },
      });
      if (params.captionText?.trim()) {
        await sendText(params.client, params.receiveId, params.captionText.trim(), params.receiveIdType);
      }
      return true;
    }

    if (isProbablyAudioPath(resolved) && extLower(resolved) === "opus") {
      const res = await params.client.im.file.create({
        data: { file_type: "opus", file_name: path.basename(resolved), file: fs.createReadStream(resolved) },
      });
      const fileKey = (res as { data?: { file_key?: string } }).data?.file_key || (res as { file_key?: string }).file_key;
      if (!fileKey) {
        throw new Error("upload opus failed");
      }
      await params.client.im.v1.message.create({
        params: { receive_id_type: params.receiveIdType },
        data: { receive_id: params.receiveId, msg_type: "audio", content: JSON.stringify({ file_key: fileKey }) },
      });
      if (params.captionText?.trim()) {
        await sendText(params.client, params.receiveId, params.captionText.trim(), params.receiveIdType);
      }
      return true;
    }

    const res = await params.client.im.file.create({
      data: { file_type: "stream", file_name: path.basename(resolved), file: fs.createReadStream(resolved) },
    });
    const fileKey = (res as { data?: { file_key?: string } }).data?.file_key || (res as { file_key?: string }).file_key;
    if (!fileKey) {
      throw new Error("upload file failed");
    }
    await params.client.im.v1.message.create({
      params: { receive_id_type: params.receiveIdType },
      data: { receive_id: params.receiveId, msg_type: "file", content: JSON.stringify({ file_key: fileKey }) },
    });
    if (params.captionText?.trim()) {
      await sendText(params.client, params.receiveId, params.captionText.trim(), params.receiveIdType);
    }
    return true;
  } finally {
    if (tempPath) {
      try {
        fs.unlinkSync(tempPath);
      } catch {
        // ignore
      }
    }
  }
}

function normalizeReplyForDelivery(params: {
  payloadText: string;
  mediaUrls: string[];
  channelCard?: unknown;
}) {
  let text = String(params.payloadText ?? "");
  let media = params.mediaUrls.filter(Boolean).map((value) => String(value));
  let card: unknown = params.channelCard;

  const cardParsed = parseCardFromText(text);
  if (cardParsed?.card) {
    card = cardParsed.card;
    text = cardParsed.text || "";
  }

  const parsed = parseMediaLines(text);
  text = parsed.text;
  media = media.concat(parsed.mediaUrls || []);

  const mdPaths = extractMarkdownLocalMediaPaths(text);
  if (mdPaths.length > 0) {
    media = media.concat(mdPaths);
    text = stripMarkdownLocalMediaRefs(text);
  }

  media = [...new Set(media)].filter(Boolean).slice(0, MAX_ATTACHMENTS);
  return { text, mediaUrls: media, card };
}

async function deliverNormalizedReply(params: {
  client: Lark.Client;
  runtime: PluginRuntime;
  receiveId: string;
  receiveIdType: "chat_id" | "open_id" | "union_id";
  text: string;
  mediaUrls: string[];
  card?: unknown;
  placeholderId?: string;
  emptyFromAgent?: boolean;
  allowedOutboundDirs: string[];
  log?: ChannelLogSink;
  eventLogger?: EventLogger;
}) {
  const trimmedText = (params.text || "").trim();
  const hasMedia = params.mediaUrls.length > 0;
  const hasCard = Boolean(params.card);

  if (!trimmedText && !hasMedia && !hasCard) {
    params.eventLogger?.logWarnEvent("deliver_empty", {
      receiveIdType: params.receiveIdType,
      receiveId: params.receiveId,
      placeholder: Boolean(params.placeholderId),
      emptyFromAgent: Boolean(params.emptyFromAgent),
    });
    if (params.placeholderId) {
      const fallback = params.emptyFromAgent
        ? "(OpenClaw returned an empty reply. Please retry.)"
        : "(Empty reply)";
      try {
        await updateTextMessage(params.client, params.placeholderId, fallback);
        return;
      } catch {
        return;
      }
    }
    return;
  }

  let sentAny = false;

  try {
    if (hasMedia) {
      for (const mediaUrl of params.mediaUrls.slice(0, MAX_ATTACHMENTS)) {
        const sent = await uploadAndSendMedia({
          client: params.client,
          runtime: params.runtime,
          receiveId: params.receiveId,
          receiveIdType: params.receiveIdType,
          mediaUrlOrPath: mediaUrl,
          allowedOutboundDirs: params.allowedOutboundDirs,
          log: params.log,
          eventLogger: params.eventLogger,
        });
        if (sent) {
          sentAny = true;
        }
      }
    }

    if (hasCard) {
      const res = await sendCard(params.client, params.receiveId, params.card, params.receiveIdType);
      if (res) {
        sentAny = true;
      }
    }

    if (trimmedText) {
      if (params.placeholderId) {
        try {
          await updateTextMessage(params.client, params.placeholderId, trimmedText);
          return;
        } catch {
          // fall through
        }
      }
      await sendText(params.client, params.receiveId, trimmedText, params.receiveIdType);
      sentAny = true;
    }

    if (params.placeholderId && sentAny) {
      try {
        await deleteMessage(params.client, params.placeholderId);
      } catch {
        // ignore
      }
    }

    if (params.placeholderId && !sentAny) {
      params.eventLogger?.logWarnEvent("deliver_no_output", {
        receiveIdType: params.receiveIdType,
        receiveId: params.receiveId,
        hasMedia,
        hasCard,
        textLen: trimmedText.length,
        emptyFromAgent: Boolean(params.emptyFromAgent),
      });
    }
  } catch (err) {
    params.eventLogger?.logErrorEvent(
      "deliver_failed",
      { receiveIdType: params.receiveIdType, receiveId: params.receiveId },
      err,
    );
    throw err;
  }
}

async function buildInboundFromMessage(params: {
  client: Lark.Client;
  runtime: PluginRuntime;
  message: FeishuMessage;
  maxImageBytes: number;
  maxFileBytes: number;
  log?: ChannelLogSink;
  eventLogger?: EventLogger;
}): Promise<{ text: string; rawText: string; mediaRefs: FeishuMediaRef[] }> {
  const messageId = params.message.message_id;
  const messageType = params.message.message_type;
  const rawContent = params.message.content;

  let text = "";
  let rawText = "";
  let mediaRefs: FeishuMediaRef[] = [];

  if (!messageType || !rawContent) {
    return { text, rawText, mediaRefs };
  }

  if (messageType === "text") {
    try {
      const parsed = JSON.parse(rawContent);
      text = normalizeFeishuText(parsed?.text ?? "");
      rawText = text;
    } catch (err) {
      params.eventLogger?.logWarnEvent("text_parse_failed", { messageId }, err);
      params.log?.warn?.(`feishu text parse failed: ${formatErr(err)}`);
    }
  }

  if (messageType === "post") {
    try {
      const parsed = JSON.parse(rawContent);
      const { text: postText, imageKeys } = extractFromPostJson(parsed ?? {});
      text = postText;
      rawText = postText;
      if (messageId && imageKeys.length > 0) {
        for (const key of imageKeys.slice(0, MAX_ATTACHMENTS)) {
          try {
            const ref = await downloadFeishuMessageResource({
              client: params.client,
              runtime: params.runtime,
              messageId,
              fileKey: key,
              type: "image",
              maxBytes: params.maxImageBytes,
            });
            mediaRefs.push(ref);
          } catch (err) {
            params.eventLogger?.logWarnEvent(
              "post_image_download_failed",
              { messageId, imageKey: key },
              err,
            );
            params.log?.warn?.(`feishu post image download failed: ${formatErr(err)}`);
          }
        }
      }
    } catch (err) {
      params.eventLogger?.logWarnEvent("post_parse_failed", { messageId }, err);
      params.log?.warn?.(`feishu post parse failed: ${formatErr(err)}`);
    }
  }

  if (messageType === "image") {
    try {
      const parsed = JSON.parse(rawContent);
      const imageKey = parsed?.image_key;
      if (imageKey && messageId) {
        const ref = await downloadFeishuMessageResource({
          client: params.client,
          runtime: params.runtime,
          messageId,
          fileKey: imageKey,
          type: "image",
          maxBytes: params.maxImageBytes,
        });
        mediaRefs.push(ref);
      }
      text = text || "[image]";
      rawText = rawText || text;
    } catch (err) {
      params.eventLogger?.logWarnEvent("image_parse_failed", { messageId }, err);
      params.log?.warn?.(`feishu image parse failed: ${formatErr(err)}`);
      text = text || "[image]";
      rawText = rawText || text;
    }
  }

  if (messageType === "media") {
    try {
      const parsed = JSON.parse(rawContent);
      const fileKey = parsed?.file_key;
      const fileName = parsed?.file_name || "video.bin";
      if (fileKey && messageId) {
        const ref = await downloadFeishuMessageResource({
          client: params.client,
          runtime: params.runtime,
          messageId,
          fileKey,
          type: "video",
          maxBytes: params.maxFileBytes,
          fileName,
        });
        mediaRefs.push(ref);
      }
      text = text || `[video] ${fileName}`;
      rawText = rawText || text;
    } catch (err) {
      params.eventLogger?.logWarnEvent("media_parse_failed", { messageId }, err);
      params.log?.warn?.(`feishu media parse failed: ${formatErr(err)}`);
      text = text || "[video]";
      rawText = rawText || text;
    }
  }

  if (messageType === "file") {
    try {
      const parsed = JSON.parse(rawContent);
      const fileKey = parsed?.file_key;
      const fileName = parsed?.file_name || "file.bin";
      if (fileKey && messageId) {
        const ref = await downloadFeishuMessageResource({
          client: params.client,
          runtime: params.runtime,
          messageId,
          fileKey,
          type: "file",
          maxBytes: params.maxFileBytes,
          fileName,
        });
        mediaRefs.push(ref);
      }
      text = text || `[file] ${fileName}`;
      rawText = rawText || text;
    } catch (err) {
      params.eventLogger?.logWarnEvent("file_parse_failed", { messageId }, err);
      params.log?.warn?.(`feishu file parse failed: ${formatErr(err)}`);
      text = text || "[file]";
      rawText = rawText || text;
    }
  }

  if (messageType === "audio") {
    try {
      const parsed = JSON.parse(rawContent);
      const fileKey = parsed?.file_key;
      const fileName = parsed?.file_name || "audio.opus";
      if (fileKey && messageId) {
        const ref = await downloadFeishuMessageResource({
          client: params.client,
          runtime: params.runtime,
          messageId,
          fileKey,
          type: "audio",
          maxBytes: params.maxFileBytes,
          fileName,
        });
        mediaRefs.push(ref);
      }
      text = text || `[audio] ${fileName}`;
      rawText = rawText || text;
    } catch (err) {
      params.eventLogger?.logWarnEvent("audio_parse_failed", { messageId }, err);
      params.log?.warn?.(`feishu audio parse failed: ${formatErr(err)}`);
      text = text || "[audio]";
      rawText = rawText || text;
    }
  }

  if (messageType === "sticker") {
    text = text || "[sticker]";
    rawText = rawText || text;
  }

  return { text, rawText, mediaRefs };
}

export async function sendFeishuTextMessage(params: {
  appId: string;
  appSecret: string;
  domain?: string | null;
  to: string;
  text: string;
  receiveIdType?: "chat_id" | "open_id" | "union_id";
}) {
  const client = createFeishuClient({
    appId: params.appId,
    appSecret: params.appSecret,
    domain: params.domain,
  });
  return sendText(client, params.to, params.text, params.receiveIdType ?? "chat_id");
}

export async function processFeishuMessage(params: {
  client: Lark.Client;
  data: unknown;
  cfg: OpenClawConfig;
  accountId: string;
  runtime: PluginRuntime;
  feishuCfg: ResolvedFeishuConfig;
  botName?: string;
  resolvePath: (input: string) => string;
  log?: ChannelLogSink;
  eventLogger?: EventLogger;
}) {
  const payload = params.data as FeishuEventPayload;
  const message = payload.message ?? payload.event?.message;
  const sender = payload.sender ?? payload.event?.sender;

  if (!message) {
    params.eventLogger?.logWarnEvent("message_missing", {
      accountId: params.accountId,
      event: "im.message.receive_v1",
    });
    params.log?.warn?.("feishu event missing message");
    return;
  }

  const chatId = message.chat_id;
  if (!chatId) {
    params.eventLogger?.logWarnEvent("message_missing_chat_id", {
      accountId: params.accountId,
      messageId: message.message_id,
    });
    params.log?.warn?.("feishu event missing chat_id");
    return;
  }

  if (isDuplicate(message.message_id)) {
    return;
  }

  const msgType = message.message_type;
  if (!msgType || !SUPPORTED_MSG_TYPES.has(msgType)) {
    params.eventLogger?.logDebugEvent("unsupported_message_type", {
      accountId: params.accountId,
      messageId: message.message_id,
      messageType: msgType ?? "unknown",
    });
    params.log?.debug?.(`feishu unsupported message type: ${msgType ?? "unknown"}`);
    return;
  }

  const senderId = sender?.sender_id?.open_id || sender?.sender_id?.user_id || "unknown";
  const senderUnionId = sender?.sender_id?.union_id;
  const isGroup = message.chat_type === "group";
  const envImageMb = process.env.FEISHU_BRIDGE_MAX_INBOUND_IMAGE_MB;
  const envFileMb = process.env.FEISHU_BRIDGE_MAX_INBOUND_FILE_MB;
  const maxImageMb = envImageMb ? Number(envImageMb) : params.feishuCfg.mediaMaxMb;
  const maxFileMb = envFileMb ? Number(envFileMb) : params.feishuCfg.mediaMaxMb;
  const maxImageBytes = maxImageMb * 1024 * 1024;
  const maxFileBytes = maxFileMb * 1024 * 1024;

  const storeAllowFrom = await params.runtime.channel.pairing
    .readAllowFromStore("feishu")
    .catch(() => []);

  const groupConfig = params.feishuCfg.groups?.[chatId];
  const senderAllowed = (allow: NormalizedAllowFrom) =>
    isSenderAllowed({ allow, senderId }) ||
    (senderUnionId ? isSenderAllowed({ allow, senderId: senderUnionId }) : false);

  if (isGroup) {
    if (groupConfig?.enabled === false) {
      return;
    }

    if (groupConfig?.allowFrom) {
      const groupAllow = normalizeAllowFromWithStore({
        allowFrom: groupConfig.allowFrom,
        storeAllowFrom,
      });
      if (!senderAllowed(groupAllow)) {
        return;
      }
    }

    if (params.feishuCfg.groupPolicy === "disabled") {
      return;
    }

    if (params.feishuCfg.groupPolicy === "allowlist") {
      const groupAllow = normalizeAllowFromWithStore({
        allowFrom:
          params.feishuCfg.groupAllowFrom.length > 0
            ? params.feishuCfg.groupAllowFrom
            : params.feishuCfg.allowFrom,
        storeAllowFrom,
      });
      if (!groupAllow.hasEntries) {
        return;
      }
      if (!senderAllowed(groupAllow)) {
        return;
      }
    }
  }

  if (!isGroup) {
    const dmPolicy = params.feishuCfg.dmPolicy;
    if (dmPolicy === "disabled") {
      return;
    }
    if (dmPolicy !== "open") {
      const dmAllow = normalizeAllowFromWithStore({
        allowFrom: params.feishuCfg.allowFrom,
        storeAllowFrom,
      });
      const allowed = dmAllow.hasWildcard || (dmAllow.hasEntries && senderAllowed(dmAllow));
      if (!allowed) {
        if (dmPolicy === "pairing") {
          try {
            const res = await params.runtime.channel.pairing.upsertPairingRequest({
              channel: "feishu",
              id: senderId,
              meta: {
                unionId: senderUnionId,
                name: sender?.sender_id?.user_id,
              },
            });
            if (res.created) {
              const reply = params.runtime.channel.pairing.buildPairingReply({
                channel: "feishu",
                idLine: `Your Feishu Open ID: ${senderId}`,
                code: res.code,
              });
              await sendText(params.client, senderId, reply, "open_id");
            }
          } catch (err) {
            params.eventLogger?.logErrorEvent(
              "pairing_failed",
              { accountId: params.accountId, senderId },
              err,
            );
            params.log?.error?.(`feishu pairing failed: ${formatErr(err)}`);
          }
          return;
        }
        return;
      }
    }
  }

  const mentions = message.mentions ?? payload.mentions ?? [];
  const wasMentioned = mentions.length > 0;
  if (isGroup) {
    const requireMention = groupConfig?.requireMention ?? true;
    if (requireMention && !wasMentioned) {
      return;
    }
  }

  const inbound = await buildInboundFromMessage({
    client: params.client,
    runtime: params.runtime,
    message,
    maxImageBytes,
    maxFileBytes,
    log: params.log,
    eventLogger: params.eventLogger,
  });

  let text = inbound.text;
  let rawText = inbound.rawText || inbound.text;
  for (const mention of mentions) {
    if (mention.key) {
      text = text.split(mention.key).join("").trim();
      rawText = rawText.split(mention.key).join("").trim();
    }
  }

  const mediaPaths = inbound.mediaRefs.map((ref) => ref.path);
  const mediaTypes = inbound.mediaRefs.map((ref) => ref.contentType).filter(Boolean) as string[];

  let bodyText = text;
  if (!bodyText && inbound.mediaRefs.length > 0) {
    bodyText = inbound.mediaRefs[0].placeholder || "[media]";
  }
  if (!bodyText && inbound.mediaRefs.length === 0) {
    return;
  }

  const senderName = sender?.sender_id?.user_id || "unknown";

  const ctx = {
    Body: bodyText,
    RawBody: rawText || bodyText,
    From: senderId,
    To: chatId,
    SenderId: senderId,
    SenderName: senderName,
    ChatType: isGroup ? "group" : "dm",
    Provider: "feishu",
    Surface: "feishu",
    Timestamp: Number(message.create_time),
    MessageSid: message.message_id,
    AccountId: params.accountId,
    OriginatingChannel: "feishu",
    OriginatingTo: chatId,
    MediaPath: mediaPaths[0],
    MediaType: mediaTypes[0],
    MediaUrl: mediaPaths[0],
    MediaPaths: mediaPaths.length > 0 ? mediaPaths : undefined,
    MediaUrls: mediaPaths.length > 0 ? mediaPaths : undefined,
    MediaTypes: mediaTypes.length > 0 ? mediaTypes : undefined,
    WasMentioned: isGroup ? wasMentioned : undefined,
  };

  const allowedOutboundDirs = resolveAllowedOutboundDirs(params.resolvePath);

  let thinkingTimer: NodeJS.Timeout | null = null;
  let placeholderId = "";
  let placeholderIssued = false;

  const schedulePlaceholder = () => {
    if (THINKING_THRESHOLD_MS <= 0) {
      return;
    }
    if (thinkingTimer || placeholderId || placeholderIssued) {
      return;
    }
    thinkingTimer = setTimeout(async () => {
      thinkingTimer = null;
      if (placeholderId || placeholderIssued) {
        return;
      }
      try {
        const id = await sendText(
          params.client,
          chatId,
          DEFAULT_THINKING_TEXT,
          "chat_id",
        );
        placeholderId = id || "";
        placeholderIssued = true;
      } catch {
        placeholderIssued = true;
      }
    }, THINKING_THRESHOLD_MS);
  };

  const clearPlaceholder = () => {
    if (thinkingTimer) {
      clearTimeout(thinkingTimer);
      thinkingTimer = null;
    }
  };

  await params.runtime.channel.reply.dispatchReplyWithBufferedBlockDispatcher({
    ctx,
    cfg: params.cfg,
    dispatcherOptions: {
      deliver: async (payload, info) => {
        const initialMedia = [
          ...(payload.mediaUrls ?? []),
          ...(payload.mediaUrl ? [payload.mediaUrl] : []),
        ];
        const channelCard = payload.channelData?.card;
        const normalized = normalizeReplyForDelivery({
          payloadText: payload.text ?? "",
          mediaUrls: initialMedia,
          channelCard,
        });

        if (info.kind === "block") {
          if (!normalized.text) {
            return;
          }
          clearPlaceholder();
          if (!placeholderId) {
            try {
              const id = await sendText(params.client, chatId, normalized.text, "chat_id");
              placeholderId = id || "";
            } catch {
              // ignore
            } finally {
              placeholderIssued = true;
            }
            return;
          }
          try {
            await updateTextMessage(params.client, placeholderId, normalized.text);
          } catch {
            try {
              const id = await sendText(params.client, chatId, normalized.text, "chat_id");
              if (id) {
                placeholderId = id;
              }
            } catch {
              // ignore
            } finally {
              placeholderIssued = true;
            }
          }
          return;
        }

        clearPlaceholder();

        await deliverNormalizedReply({
          client: params.client,
          runtime: params.runtime,
          receiveId: chatId,
          receiveIdType: "chat_id",
          text: normalized.text,
          mediaUrls: normalized.mediaUrls,
          card: normalized.card,
          placeholderId,
          emptyFromAgent: !normalized.text && normalized.mediaUrls.length === 0 && !normalized.card,
          allowedOutboundDirs,
          log: params.log,
          eventLogger: params.eventLogger,
        });
      },
      onError: (err) => {
        clearPlaceholder();
        params.eventLogger?.logErrorEvent(
          "reply_error",
          { accountId: params.accountId, messageId: message.message_id, chatId },
          err,
        );
        params.log?.error?.(`feishu reply error: ${formatErr(err)}`);
        if (placeholderId) {
          updateTextMessage(
            params.client,
            placeholderId,
            "（OpenClaw 未返回回复，可能超时或队列拥堵，请稍后重试）",
          ).catch(() => {});
        }
      },
      onReplyStart: () => {
        schedulePlaceholder();
      },
    },
    replyOptions: {
      disableBlockStreaming: !params.feishuCfg.blockStreaming,
    },
  });
}

export async function monitorFeishuProvider(params: {
  cfg: OpenClawConfig;
  accountId: string;
  account: { config: { appId?: string; appSecret?: string; domain?: string | null }; name?: string };
  runtime: PluginRuntime;
  resolvePath: (input: string) => string;
  abortSignal?: AbortSignal;
  log?: ChannelLogSink;
}) {
  const eventLogger = createEventLogger({
    resolvePath: params.resolvePath,
    log: params.log,
    accountId: params.accountId,
  });
  const { appId, appSecret, domain } = params.account.config;
  if (!appId || !appSecret) {
    eventLogger.logErrorEvent("fatal_missing_credentials", {
      accountId: params.accountId,
    });
    throw new Error(`Feishu app ID/secret missing for account "${params.accountId}"`);
  }

  const feishuCfg = resolveFeishuConfig({
    cfg: params.cfg,
    accountId: params.accountId,
  }) as ResolvedFeishuConfig;

  if (!feishuCfg.enabled) {
    params.log?.info?.(`feishu account "${params.accountId}" disabled`);
    return;
  }

  const client = createFeishuClient({ appId, appSecret, domain, log: params.log });
  const wsClient = createFeishuWsClient({ appId, appSecret, domain, log: params.log });

  const eventDispatcher = new Lark.EventDispatcher({}).register({
    "im.message.receive_v1": async (data) => {
      try {
        await processFeishuMessage({
          client,
          data,
          cfg: params.cfg,
          accountId: params.accountId,
          runtime: params.runtime,
          feishuCfg,
          botName: params.account.name,
          resolvePath: params.resolvePath,
          log: params.log,
          eventLogger,
        });
      } catch (err) {
        eventLogger.logErrorEvent(
          "message_handler_error",
          { accountId: params.accountId },
          err,
        );
        params.log?.error?.(`feishu process error: ${formatErr(err)}`);
      }
    },
  });

  const handleAbort = () => {
    params.log?.info?.("feishu ws abort requested");
  };

  if (params.abortSignal) {
    params.abortSignal.addEventListener("abort", handleAbort, { once: true });
  }

  try {
    params.log?.info?.("feishu ws client starting");
    await wsClient.start({ eventDispatcher });
    params.log?.info?.("feishu ws client started");
    if (params.abortSignal) {
      await new Promise<void>((resolve) => {
        if (params.abortSignal?.aborted) {
          resolve();
          return;
        }
        params.abortSignal?.addEventListener("abort", () => resolve(), { once: true });
      });
    } else {
      await new Promise<void>(() => {});
    }
  } finally {
    if (params.abortSignal) {
      params.abortSignal.removeEventListener("abort", handleAbort);
    }
  }
}
