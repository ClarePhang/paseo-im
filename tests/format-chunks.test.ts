import assert from "node:assert/strict";
import { test } from "node:test";
import { splitChunks } from "../server/format/chunks.ts";

test("短文本不分块", () => {
  assert.deepEqual(splitChunks("hello", 100), ["hello"]);
});

test("limit<=0 不分块", () => {
  const text = "a".repeat(500);
  assert.deepEqual(splitChunks(text, 0), [text]);
});

test("段落边界优先", () => {
  const text = `${"a".repeat(60)}\n\n${"b".repeat(60)}`;
  const chunks = splitChunks(text, 100);
  assert.equal(chunks.length, 2);
  assert.ok(chunks[0]!.startsWith("a"));
  assert.ok(chunks[1]!.startsWith("b"));
});

test("代码围栏整体不被切断", () => {
  const text = `${"a".repeat(30)}\n\`\`\`\ncode\n\`\`\`\n${"b".repeat(30)}`;
  const chunks = splitChunks(text, 50);
  assert.ok(chunks.length >= 2);
  const fence = chunks.find((chunk) => chunk.includes("```"));
  assert.ok(fence?.includes("code"));
});

test("单个超长围栏块硬切分", () => {
  const text = `\`\`\`\n${"x".repeat(250)}\n\`\`\``;
  const chunks = splitChunks(text, 100);
  assert.ok(chunks.length >= 3);
  assert.ok(chunks.every((chunk) => chunk.length <= 100 + 8));
});

test("无空行超长文本按字符硬切", () => {
  const text = "x".repeat(250);
  const chunks = splitChunks(text, 100);
  assert.deepEqual(chunks, ["x".repeat(100), "x".repeat(100), "x".repeat(50)]);
});

test("空串返回单空块", () => {
  assert.deepEqual(splitChunks("", 100), [""]);
});
