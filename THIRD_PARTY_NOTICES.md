# 第三方声明（THIRD_PARTY NOTICES）

paseo-im 在开发过程中参考并依赖了以下第三方项目，感谢原作者。

## 参考实现（非依赖，设计参考）

| 项目 | 许可证 | 使用方式 |
| --- | --- | --- |
| [dsh-im](https://github.com/xmanrui/dsh-im) | MIT | 各 IM 渠道协议事实来源（报文结构、状态机、已知坑位），微信/WhatsApp/钉钉等适配器实现均对照其协议卡实现 |
| [paseo-telegram](https://github.com/Daaaaave/paseo-telegram) | MIT | Paseo 插件组织形式（manifest / client / server 布局）参考 |

## 运行时依赖

| 包 | 许可证 | 用途 |
| --- | --- | --- |
| [@larksuiteoapi/node-sdk](https://www.npmjs.com/package/@larksuiteoapi/node-sdk) | MIT | 飞书开放平台 SDK |
| [@tencent-connect/qqbot-nodejs](https://www.npmjs.com/package/@tencent-connect/qqbot-nodejs) | MIT | QQ 机器人协议 SDK |
| [@wecom/aibot-node-sdk](https://www.npmjs.com/package/@wecom/aibot-node-sdk) | MIT | 企业微信智能机器人 SDK |
| [@whiskeysockets/baileys](https://www.npmjs.com/package/@whiskeysockets/baileys) | MIT | WhatsApp 多设备协议（非官方） |
| [dingtalk-stream](https://www.npmjs.com/package/dingtalk-stream) | MIT | 钉钉 Stream 模式 SDK |
| [entities](https://www.npmjs.com/package/entities) | BSD-2-Clause | HTML 实体解码 |
| [marked](https://www.npmjs.com/package/marked) | MIT | Markdown 解析 |
| [qrcode](https://www.npmjs.com/package/qrcode) | MIT | 扫码配对二维码本地生成 |
| [ws](https://www.npmjs.com/package/ws) | MIT | WebSocket（QQ/TG/Slack 等长连接） |
| [react](https://www.npmjs.com/package/react) / [react-native](https://www.npmjs.com/package/react-native) | MIT | 插件设置页 UI（宿主提供运行时，仅开发期类型/编译） |
| [zod](https://www.npmjs.com/package/zod) | MIT | 配置与契约校验 |

## 宿主平台 SDK

| 包 | 许可证 | 说明 |
| --- | --- | --- |
| [@getpaseo/plugin](https://www.npmjs.com/package/@getpaseo/plugin) / [@getpaseo/client](https://www.npmjs.com/package/@getpaseo/client) | 包内未声明 | [Paseo](https://paseo.sh) 宿主平台的官方插件/客户端 SDK。本项目是 Paseo 插件，运行时依赖宿主 daemon；分发与使用遵循 Paseo 平台生态条款。发布前已向平台方确认条款前，本仓库不随附 SDK 源码（仅作为 peer/开发依赖引用）。 |

## 说明

- 各依赖许可证以上表为准（提取自其 package.json 或 npm registry 元数据，核对于 2026-11-09）。
- 本项目自身代码以 [MIT](./LICENSE) 提供。
- 免责声明：微信/iMessage 通道使用未公开或非官方协议，存在稳定性与账号风险，使用即代表知晓并自行承担（详见 README 各渠道备注）。
