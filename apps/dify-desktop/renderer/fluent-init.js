const appearanceByClass = (cls) => {
  const c = String(cls || "");
  if (/\bdanger\b/.test(c)) return "accent";
  if (/\bsecondary\b/.test(c)) return "neutral";
  return "accent";
};

document.addEventListener("DOMContentLoaded", () => {
  const body = document.body;
  const isWorkflow = /workflow/i.test(document.title) || location.pathname.toLowerCase().includes("workflow");
  body.classList.add(isWorkflow ? "page-workflow" : "page-home");
  const search = new URLSearchParams(location.search || "");
  const savedDevMode = (() => {
    try { return localStorage.getItem("aiwf_dev_mode") === "1"; } catch { return false; }
  })();
  const devMode = search.get("devtools") === "1" || savedDevMode || navigator.webdriver === true || /playwright/i.test(navigator.userAgent || "");
  body.classList.toggle("dev-mode", devMode);

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
      const rect = hero.getBoundingClientRect();
      const rx = ((evt.clientX - rect.left) / Math.max(rect.width, 1)) - 0.5;
      const ry = ((evt.clientY - rect.top) / Math.max(rect.height, 1)) - 0.5;
      hero.style.transform = `perspective(900px) rotateX(${(-ry * 1.6).toFixed(2)}deg) rotateY(${(rx * 1.6).toFixed(2)}deg)`;
    });
    hero.addEventListener("pointerleave", () => {
      hero.style.transform = "";
    });
  }
});
