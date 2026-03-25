const appearanceByClass = (cls) => {
  const c = String(cls || "");
  if (/\bdanger\b/.test(c)) return "accent";
  if (/\bsecondary\b/.test(c)) return "neutral";
  return "accent";
};

document.addEventListener("DOMContentLoaded", () => {
  const body = document.body;
  const root = document.documentElement;
  const isWorkflow = /workflow/i.test(document.title) || location.pathname.toLowerCase().includes("workflow");
  body.classList.add(isWorkflow ? "page-workflow" : "page-home");
  const updateInputMode = () => {
    const coarse = !!(window.matchMedia && window.matchMedia("(pointer: coarse)").matches) || Number(navigator.maxTouchPoints || 0) > 0;
    const reducedMotion = !!(window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches);
    const width = Number(window.innerWidth || 0);
    const dpr = Math.max(1, Math.min(Number(window.devicePixelRatio || 1), 3));
    body.classList.toggle("input-coarse", coarse);
    body.classList.toggle("input-touch", coarse);
    body.classList.toggle("reduced-motion", reducedMotion);
    body.classList.toggle("high-dpi", dpr > 1.25);
    body.classList.toggle("dpi-hidpi", dpr >= 1.5);
    body.classList.toggle("dpi-retina", dpr >= 2);
    body.classList.toggle("window-compact", width > 0 && width < 960);
    body.classList.toggle("window-medium", width >= 960 && width < 1440);
    body.classList.toggle("window-wide", width >= 1440);
    root.style.setProperty("--app-dpr", dpr.toFixed(2));
    root.style.setProperty("--viewport-width", `${Math.max(width, 320)}px`);
    body.style.setProperty("--device-pixel-ratio", String(dpr));
  };
  updateInputMode();
  const search = new URLSearchParams(location.search || "");
  const embeddedMode = search.get("embedded") === "1";
  const compatAdminMode = search.get("legacyAdmin") === "1";
  const savedDevMode = (() => {
    try { return localStorage.getItem("aiwf_dev_mode") === "1"; } catch { return false; }
  })();
  const devMode = search.get("devtools") === "1" || savedDevMode || navigator.webdriver === true || /playwright/i.test(navigator.userAgent || "");
  body.classList.toggle("dev-mode", devMode);
  body.classList.toggle("embedded-mode", embeddedMode);
  body.classList.toggle("compat-admin-mode", compatAdminMode);

  const badge = document.querySelector(".hero div");
  if (badge) {
    badge.classList.add("hero-badge");
    badge.textContent = devMode ? "Fluent 2 | Win11 Shell | Dev" : "Fluent 2 | Win11 Shell";
  }

  document.querySelectorAll(".card").forEach((card) => {
    card.classList.add("reveal");
  });

  document.querySelectorAll("fluent-button").forEach((btn) => {
    btn.setAttribute("appearance", appearanceByClass(btn.className));
    if (!btn.hasAttribute("shape")) btn.setAttribute("shape", "rounded");
  });

  const hero = document.querySelector(".hero");
  if (hero) {
    hero.addEventListener("pointermove", (evt) => {
      if (body.classList.contains("input-coarse") || body.classList.contains("reduced-motion")) return;
      if (String(evt.pointerType || "").toLowerCase() === "touch") return;
      const rect = hero.getBoundingClientRect();
      const rx = ((evt.clientX - rect.left) / Math.max(rect.width, 1)) - 0.5;
      const ry = ((evt.clientY - rect.top) / Math.max(rect.height, 1)) - 0.5;
      hero.style.transform = `perspective(900px) rotateX(${(-ry * 1.6).toFixed(2)}deg) rotateY(${(rx * 1.6).toFixed(2)}deg)`;
    });
    hero.addEventListener("pointerleave", () => {
      hero.style.transform = "";
    });
  }

  window.addEventListener("resize", updateInputMode, { passive: true });
  window.visualViewport?.addEventListener?.("resize", updateInputMode, { passive: true });
});
