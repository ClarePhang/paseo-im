# CLAUDE.md

Claude Code / agent 会话在本仓库工作时的操作约定：完整规则见 [AGENTS.md](./AGENTS.md)（门禁铁律、边界、知识回路、协作边界、披露五节）。

速查：

- 门禁：`npm run typecheck && npm test`（变更前后各跑一次，全绿才算完成）
- 状态：`docs/status.md`（改完同步状态表/修复索引）
- 踩坑：`docs/gotchas.md`（实机 bug 必须沉淀对应节）
- 脱敏守卫：`node scripts/check-redaction.mjs`（文档/脚本不得含真实 ID 与本机路径）
