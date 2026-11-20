## 变更说明

<!-- 一段话说清「改了什么 + 为什么」。实机 bug 修复需附三件套：代码 + 回归测试 + gotchas 沉淀节。 -->

## 门禁

- [ ] `npm run typecheck && npm test` 全绿（运行时行为变更建议双跑）
- [ ] `node scripts/check-redaction.mjs` 通过

## 范围

- [ ] 渠道行为变更已更新 `docs/channel-test-matrix.md` 对应项
- [ ] 实机验收记录已写入 `docs/webui-acceptance.md`
- [ ] 状态/索引已同步（`docs/status.md` / `docs/gotchas.md`）

## Agent 参与披露（强制）

按 Harness Model 格式填写；无 agent 参与选 none 并删除模型行。

- Agent participation: `none` / `assisted (human-reviewed)` / `autonomous (human-reviewed)`
- Model: `<provider/model>`（如 `omp/acme-inc/model-x`）
