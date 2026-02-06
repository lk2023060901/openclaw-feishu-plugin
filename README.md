# openclaw-feishu-plugin

## 中文介绍

`openclaw-feishu-plugin` 是 OpenClaw 的飞书（Feishu/Lark）通道插件，提供与现有桥接器一致的能力，用于在飞书中接收消息并将 OpenClaw 的回复发送回飞书。

主要特性：
- 飞书消息接入（群聊/私聊）
- 文本、图片、文件等消息发送
- 可配置的“等待回复中...”占位提示
- 与 OpenClaw 网关无缝集成

快速使用：
1. 将插件放入 OpenClaw 的 `extensions` 目录。
2. 在 `openclaw.json` 中开启 `channels.feishu` 并配置 `appId` / `appSecret`。
3. 启动 OpenClaw 网关即可开始使用。
