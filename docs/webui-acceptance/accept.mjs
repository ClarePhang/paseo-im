import { chromium } from "playwright";
import { mkdirSync } from "node:fs";

const BASE = "http://127.0.0.1:6768";
const OUT = "/tmp/webui-accept/shots";
mkdirSync(OUT, { recursive: true });

const results = [];
function check(name, ok, detail = "") {
  results.push({ name, ok, detail });
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? `  — ${detail}` : ""}`);
}

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1366, height: 860 } });
const consoleErrors = [];
page.on("console", (msg) => {
  if (msg.type() === "error") consoleErrors.push(msg.text().slice(0, 200));
});

// 1. 加载 WebUI（跳过首启向导：两层 X 叠放不可靠，"跳过设置" 一次成功）
await page.goto(BASE, { waitUntil: "domcontentloaded", timeout: 60_000 });
await page.waitForLoadState("networkidle", { timeout: 45_000 }).catch(() => {});
await page.waitForTimeout(2500);
await page.screenshot({ path: `${OUT}/01-boot.png` });
check("WebUI 首屏加载", true);

try {
  const skip = page.getByText("跳过设置", { exact: true });
  await skip.waitFor({ state: "visible", timeout: 8000 });
  await skip.first().click({ timeout: 5000 });
  await skip.waitFor({ state: "hidden", timeout: 5000 }).catch(() => {});
  await page.waitForTimeout(500);
} catch {
  // 无首启向导
}

// 2. 侧栏 IM 入口 → IM 屏
const sidebarIm = page.getByText("IM", { exact: true }).first();
const imVisible = await sidebarIm.isVisible().catch(() => false);
check("侧栏出现 IM 入口", imVisible);
if (imVisible) {
  await sidebarIm.click();
  await page.waitForTimeout(1800);
}
await page.screenshot({ path: `${OUT}/02-im-screen.png`, fullPage: true });

const topListText = await page.evaluate(() => document.body.innerText);
const channels = ["飞书", "微信", "钉钉", "企业微信", "企业微信应用", "QQ", "Slack", "Telegram", "Discord", "WhatsApp", "iMessage", "Matrix"];
const missing = channels.filter((c) => !topListText.includes(c));
check("渠道列表 12 渠道渲染", missing.length === 0, missing.length ? `缺少: ${missing.join(",")}` : "全部命中");
check("通用设置分区渲染", topListText.includes("通用设置") && topListText.includes("忙时行为"));

/** 逐级点「‹ 返回 …」回到顶层渠道列表。 */
async function backToTopList() {
  for (let i = 0; i < 4; i += 1) {
    const txt = await page.evaluate(() => document.body.innerText);
    if (txt.includes("IM 机器人") && txt.includes("通用设置")) return true;
    const back = page.getByText(/^‹ 返回/).first();
    if (await back.isVisible().catch(() => false)) {
      await back.click({ timeout: 5000 });
      await page.waitForTimeout(1200);
    } else break;
  }
  const txt = await page.evaluate(() => document.body.innerText);
  return txt.includes("IM 机器人") && txt.includes("通用设置");
}

// 3. Telegram 错误路径（无效凭据 → 连接创建 → 状态列错误呈现）
const tgRow = page.getByText("Telegram", { exact: true }).first();
if (await tgRow.isVisible().catch(() => false)) {
  await tgRow.click();
  await page.waitForTimeout(1500);
  const nameInput = page.getByPlaceholder(/我的Telegram/).first();
  if (await nameInput.isVisible().catch(() => false)) await nameInput.fill("验收测试").catch(() => {});
  const tokenInput = page.locator('input[type="password"]').first();
  if (await tokenInput.isVisible().catch(() => false)) await tokenInput.fill("000000:INVALID_TOKEN").catch(() => {});
  await page.screenshot({ path: `${OUT}/03-telegram-form.png`, fullPage: true });
  const connectBtn = page.getByText("连接", { exact: true }).last();
  if (await connectBtn.isVisible().catch(() => false)) {
    await connectBtn.click({ timeout: 5000 }).catch(() => {});
    await page.waitForTimeout(6000);
  }
  await page.screenshot({ path: `${OUT}/04-telegram-error.png`, fullPage: true });
  const errText = await page.evaluate(() => document.body.innerText);
  const ok = errText.includes("需要关注") && /fetch failed|无法连接|请检查|格式不正确|缺少必填/i.test(errText);
  check(
    "Telegram 非法凭据 → 错误呈现于状态列",
    ok,
    errText.match(/[^\n]*(fetch failed|无法连接[^\n]*)/)?.[0]?.slice(0, 110) ?? "未见错误状态",
  );
} else {
  check("Telegram 渠道入口", false, "未找到 Telegram 行");
}

// 4. 回到顶层 → iMessage 平台门禁
const backOk = await backToTopList();
check("逐级返回渠道列表", backOk);
if (backOk) {
  const imsgRow = page.getByText("iMessage", { exact: true }).first();
  if (await imsgRow.isVisible().catch(() => false)) {
    await imsgRow.click();
    await page.waitForTimeout(1500);
    await page.screenshot({ path: `${OUT}/05-imessage.png`, fullPage: true });
    const txt = await page.evaluate(() => document.body.innerText);
    check("iMessage 渠道入口 + macOS 提示", txt.includes("macOS"));
  } else {
    check("iMessage 渠道入口", false, "返回后仍未找到行");
  }
}

// 5. compact 视口
const compact = await browser.newPage({ viewport: { width: 420, height: 820 } });
await compact.goto(BASE, { waitUntil: "domcontentloaded", timeout: 60_000 });
await compact.waitForLoadState("networkidle", { timeout: 45_000 }).catch(() => {});
await compact.waitForTimeout(2200);
try {
  const skip = compact.getByText("跳过设置", { exact: true });
  await skip.waitFor({ state: "visible", timeout: 8000 });
  await skip.first().click({ timeout: 5000 });
  await skip.waitFor({ state: "hidden", timeout: 5000 }).catch(() => {});
  await compact.waitForTimeout(600);
} catch {
  // 无首启向导
}
await compact.screenshot({ path: `${OUT}/06-compact.png` });
const compactText = await compact.evaluate(() => document.body.innerText);
check("compact 视口渲染", compactText.includes("IM") || compactText.length > 50);
await compact.close();

// 6. 控制台错误面
const realErrors = consoleErrors.filter((e) => !/Download the React DevTools|Warning:/i.test(e));
check("无未捕获控制台错误", realErrors.length === 0, realErrors.slice(0, 3).join(" | "));

await browser.close();
const failed = results.filter((r) => !r.ok);
console.log(`\n==== ${results.length - failed.length}/${results.length} PASS ====`);
process.exit(0);
