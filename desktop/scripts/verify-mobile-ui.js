const fs = require("fs");
const html = fs.readFileSync("peer/ui/v2/index.html", "utf8");
const must = [
  "get('surface') === 'mobile'",
  "mobileSurface",
  "html.mobile-surface",
  'x-show="!mobileSurface"',
  "dashboardTab: defaultTab",
  "sidebarCollapsed: mobileSurface ? true : false",
  "loadChatModels()",
  "chat-composer",
];
const missing = must.filter((s) => !html.includes(s));
if (missing.length) {
  console.error("Missing mobile surface markers:", missing);
  process.exit(1);
}
console.log("mobile surface UI markers: OK (" + must.length + ")");
