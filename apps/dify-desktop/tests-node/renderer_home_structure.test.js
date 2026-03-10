const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

test("home renderer references external app script without inline blocks", () => {
  const file = path.resolve(__dirname, "..", "renderer", "index.html");
  const html = fs.readFileSync(file, "utf8");
  assert.match(html, /<script\s+src="\.\/home-app\.js"><\/script>/i);
  const inlineScripts = html.match(/<script>([\s\S]*?)<\/script>/gi) || [];
  assert.equal(inlineScripts.length, 0);
  assert.match(html, /script-src 'self'/i);
});
