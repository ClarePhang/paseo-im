# 安全政策（SECURITY）

## 报告漏洞

请通过 **GitHub Security Advisories**（仓库 Security 标签 → Report a vulnerability）私下报告，**不要**在公开 issue/PR/IM 会话中披露细节。

包含：受影响版本（commit/版本号）、渠道、复现步骤或 PoC、影响评估。

## 承诺

- 48 小时内确认收到，7 天内给出初步评估与修复计划。
- 修复发布前不公开细节；修复后将你加入致谢（如你同意）。

## 高敏区域提示

本项目接入多个 IM 平台，以下位置的问题请**优先私下报告**：

- 凭据存储与读取（`$PASEO_HOME/plugin-data/paseo-im/secrets/`、`server/secret-store.ts`）
- 第三方 IM 输入的信任边界（所有入站内容视为不可信输入：`server/bridge.ts`、各渠道 `normalize.ts`）
- 微信/iMessage 等非公开协议通道（`server/channels/weixin/`、`imessage/`）
- 脱敏守卫绕过（`scripts/check-redaction.mjs` 的规则缺口）

## 范围外

- 非官方协议渠道（微信/WhatsApp/iMessage）的平台侧账号风险（封号、限流）不属于安全漏洞。
- 依赖包自身的漏洞请同步上游，同时欢迎告知我们以便升级。
