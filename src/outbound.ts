import type { ChannelOutboundAdapter, PluginRuntime } from "openclaw/plugin-sdk";
import { createFeishuClient, sendFeishuTextMessage, uploadAndSendMedia } from "./feishu.js";
import { normalizeFeishuTarget, resolveFeishuAccount } from "./compat.js";

function resolveReceiveIdType(id: string): "chat_id" | "open_id" | "union_id" {
  const trimmed = id.trim();
  if (trimmed.startsWith("oc_")) {
    return "chat_id";
  }
  if (trimmed.startsWith("ou_")) {
    return "open_id";
  }
  return "open_id";
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

function chunkByLength(text: string, limit: number): string[] {
  if (!limit || text.length <= limit) {
    return [text];
  }
  const out: string[] = [];
  for (let i = 0; i < text.length; i += limit) {
    out.push(text.slice(i, i + limit));
  }
  return out;
}

export function createFeishuOutbound(
  runtime: PluginRuntime,
  resolvePath: (input: string) => string,
): ChannelOutboundAdapter {
  return {
    deliveryMode: "direct",
    chunkerMode: "markdown",
    textChunkLimit: 4000,
    chunker: (text, limit) => chunkByLength(text, limit),
    sendText: async ({ cfg, to, text, accountId }) => {
      const account = resolveFeishuAccount({ cfg, accountId });
      if (!account.config.appId || !account.config.appSecret) {
        throw new Error("Feishu app credentials not configured");
      }
      const receiveId = normalizeFeishuTarget(to) ?? to;
      const receiveIdType = resolveReceiveIdType(receiveId);
      const messageId =
        (await sendFeishuTextMessage({
          appId: account.config.appId,
          appSecret: account.config.appSecret,
          domain: account.config.domain,
          to: receiveId,
          text: text ?? "",
          receiveIdType,
        })) ?? `feishu_${Date.now()}`;
      return { channel: "feishu", messageId };
    },
    sendMedia: async ({ cfg, to, text, mediaUrl, accountId }) => {
      const account = resolveFeishuAccount({ cfg, accountId });
      if (!account.config.appId || !account.config.appSecret) {
        throw new Error("Feishu app credentials not configured");
      }
      const receiveId = normalizeFeishuTarget(to) ?? to;
      const receiveIdType = resolveReceiveIdType(receiveId);
      const client = createFeishuClient({
        appId: account.config.appId,
        appSecret: account.config.appSecret,
        domain: account.config.domain,
      });
      let messageId: string | null = null;
      if (text?.trim()) {
        messageId = await sendFeishuTextMessage({
          appId: account.config.appId,
          appSecret: account.config.appSecret,
          domain: account.config.domain,
          to: receiveId,
          text,
          receiveIdType,
        });
      }
      if (mediaUrl) {
        const allowedOutboundDirs = resolveAllowedOutboundDirs(resolvePath);
        await uploadAndSendMedia({
          client,
          runtime,
          receiveId,
          receiveIdType,
          mediaUrlOrPath: mediaUrl,
          allowedOutboundDirs,
        });
      }
      return { channel: "feishu", messageId: messageId ?? `feishu_${Date.now()}` };
    },
  };
}
