# paseo-im 全量实施规划

状态：规划定稿（v1，2026-11-07）。前置：`docs/design.md`。

## 1. 里程碑与任务分解

### M0 核心框架（先行，契约冻结）
| 任务 | 产物 | 验证 |
| --- | --- | --- |
| 清单/构建 | `paseo-plugin.json`（id=paseo-im, name, icon, media, requirements>=0.15.2, build npm ci）、package.json deps/devDeps、tsconfig 双份（server NodeNext / client RN 无 node types） | `npm run typecheck` 绿 |
| 基础设施 | `server/paths.ts`、`server/files.ts`（atomicWrite 0700/0600 + 断言 mode 的测试） | files.test.ts |
| 配置模型 | `shared/model.ts`（NormalizedMessage/Capabilities/Outbound）、`shared/config.ts`（12 渠道判别联合 + Target/Policy）、`shared/contracts.ts`（11 RPC + ConnectionView/Choices） | config.test.ts（每渠道 schema 解析/拒绝） |
| 持久化 | `server/registry.ts`（乐观锁、≤50 连接）、`server/secret-store.ts`、`server/state-store.ts`（jobs/conversations/cursor/pendingGroups） | registry/state 单测（并发写、revision 冲突） |
| 守卫 | `scripts/check-package.mjs`（发布字段/入口/依赖白名单）、`scripts/check-runtime.mjs`（client 无 node 导入、root 无代码模块） | node --test |
| 管理面 | `server/manager.ts`（start/stop/activate、exclusive 串行）、`server/daemon.ts`（connectLocalDaemon 移植，clientId=paseo-im）、`index.server.ts`（11 RPC + addHook permission.after） | manager 单测（fake DaemonLink） |

### M1 桥接引擎（渠道无关，全渠道共享）
| 任务 | 产物 | 验证 |
| --- | --- | --- |
| 准入 | `server/policy.ts`（dm/group × owner/operators/admins/everyone × invocation；admins 校验留 adapter 钩子） | policy.test.ts 矩阵用例 |
| 路由 | `server/routing.ts`（会话键、convKey 覆盖、target 解析、agent 绑定标签、workspace 下建 agent） | routing.test.ts |
| 队列 | `server/queue.ts`（conversation + agent 两级串行、question 回复绕行） | queue.test.ts（顺序断言） |
| 桥接 | `server/bridge.ts`（handle→authorize→questionMatch→dispatch receipt→waitForFinish→deliver；FALLBACK/failure 物化） | bridge.test.ts（fake adapter + fake PaseoApi，断言 receipt 恢复不重发） |
| 进度 | `server/progress.ts`（ProgressSink：edit/stream/typing/none；8s 节流、finish 原位替换） | progress.test.ts |
| 提问 | `server/questions.ts`（permission.before→IM、回复匹配→respondToPermission、permission.after 清理） | questions.test.ts |
| 补发 | `server/deferred.ts`（超时登记、完成后补发、≤3 次且不确定不重试） | deferred.test.ts |
| 格式 | `server/format/`：marked AST → telegram entities / mrkdwn / discord md / whatsapp / plain；fence-aware 分块 | format/*.test.ts |

### M2 渠道适配器（每渠道 = 独立 slice：`server/channels/<ch>/`（api+adapter+normalize+format 挂钩）+ `tests/channels/<ch>.test.ts`）
验收统一标准：① schema 往返解析；② normalize 用例（dsh-im 渠道卡里的真实事件形状各 ≥1：dm/group/mention/reply/media）；③ 发送分块按矩阵上限；④ Recorder mock 传输下 connect→收→发全链路单测；⑤ testConnection。

| 波次 | 渠道 | 依赖 | 备注 |
| --- | --- | --- | --- |
| W1 | telegram | 无外部 SDK | 最薄参照 paseo-telegram server/telegram.ts + dsh runtime 语义（topic、groupTriggers、命令菜单） |
| W1 | slack | ws | Socket Mode ACK、startStream、三步上传 |
| W1 | discord | ws | Gateway v10 hello/identify/heartbeat、thread 创建、intents 校验 |
| W2 | feishu | @larksuiteoapi/node-sdk | 先装依赖验证；WSClient 事件 + OpenAPI 发送；卡片流式首版用 post 分步（cardkit 权限门槛高，列为可选开关） |
| W2 | dingtalk | dingtalk-stream | TOPIC_ROBOT + sessionWebhook 回复 + AI Card 进度（首版可用 typing 文案占位卡） |
| W2 | qq | @tencent-connect/qqbot-nodejs | markdown→回退纯文本；被动窗口/分片上限执行 |
| W2 | wecom | @wecom/aibot-node-sdk | 官方扫码（qc/generate+query_result）+ 手填双路径；replyStream |
| W2 | wecom-app | @wecom/aibot-node-sdk（crypto） | 回调服务器复用单实例（多 bot 路由）；公网基址校验 |
| W3 | whatsapp | @whiskeysockets/baileys | QR 配对流（pairing RPC）；auth 目录 0700/0600；self-chat 默认 |
| W3 | matrix | 无（fetch） | sync 长轮询 + next_batch sidecar；m.replace 进度；E2EE 不做（明示） |
| W3 | weixin | 无（fetch+qrcode） | 移植 dsh-im weixin-api.mjs（MIT 注明）；仅私聊；typing |
| W3 | imessage | 无 | darwin 门禁；chat.db 轮询 + osascript；非 darwin 恒 unsupported |

### M3 Client UI
- `client/channels.ts` 渠道注册表；`client/screen.tsx`（列表/表单/详情/QR/审批/路由编辑/全局设置，全 RN 原语 + theme/layout）；`index.client.tsx` 三件套。
- 验证：`scripts/check-runtime.mjs` 零 DOM 命中；jsdom 渲染冒烟（paseo-telegram tests/ui 模式）。

### M4 全局设置与文档
- `shared/settings.ts`（defineSettings：图片限制 30MB/5MB/20MB/20 张、忙时 queue|steer、附件 TTL 168h 默认）+ server 读取接线。
- `OVERVIEW.md`、`README.md`（中文，含各渠道开通指引与风险）、CHANGELOG。

### M5 集成验收
见 §3。

## 2. 并行执行安排

- M0+M1 由主 agent 完成（契约与共享代码不外包）。
- M2 三波 fan-out：W1（3 渠道）、W2（5 渠道）、W3（4 渠道）各为一批子代理任务；每个任务给足：设计文档路径 + dsh-im 渠道卡事实（文件:行号）+ paseo-telegram 对应参照 + 统一验收标准；约定接口只依赖 `server/channels/types.ts` 与 `shared/`。
- M3 主 agent 完成（跨渠道 UI 聚合，需全局视角）。
- 每波合入后统一 typecheck + test + 修复（主 agent），再放行下一波。

## 3. 集成验收清单（"完成"的定义）

1. `npm run typecheck`（server+client 两 tsconfig）零错误。
2. `npm test`（node --test）全绿：框架单测 + 12 渠道单测 + 静态守卫。
3. `paseo plugin install /home/clare/clare_oss/paseo/paseo-im && paseo plugin ls` → `running`，无 load error。
4. `paseo plugin logs paseo-im` 无崩溃输出。
5. Paseo 应用内：sidebar 出现 IM 入口；screen 打开渲染渠道列表（宽窗口 + compact + 深浅主题）。
6. 每渠道：testConnection 单测路径绿（mock）；**QQ 渠道已完成真实凭据端到端验收**（在线/入站/去重/准入/绑定 workspace/agent 回复发回 QQ，见 webui-acceptance.md QQ 节）；其余渠道待凭据逐渠道联调。
7. RPC 错误路径：connect 传非法 token → UserFacingError 文案直达 UI。

## 4. 明确范围声明

- 首版交付 12 渠道适配器 + 桥接 + UI；**真实凭据端到端联调**（真实 bot token 收发）需用户提供各平台凭据，不在本次自动化验收内——验收 §3.6 如实标注。
- bots profile / git worktree 同步 / Matrix E2EE / 语音转写 / dsh-im 扫码建应用（钉钉、飞书、QQ 私有配对后端）不在首版（design.md §1）。
