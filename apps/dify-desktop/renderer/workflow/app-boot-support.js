import {
  CANVAS_BINDING_KEYS,
  EDITOR_BINDING_KEYS,
  STARTUP_KEYS,
  TOOLBAR_BINDING_KEYS,
} from "./app-boot-support-keys.js";

function pickKeys(source = {}, keys = []) {
  const out = {};
  keys.forEach((key) => {
    if (Object.prototype.hasOwnProperty.call(source, key)) out[key] = source[key];
  });
  return out;
}

function buildToolbarBindingDeps(ctx = {}) {
  return pickKeys(ctx, TOOLBAR_BINDING_KEYS);
}

function buildCanvasBindingDeps(ctx = {}) {
  return pickKeys(ctx, CANVAS_BINDING_KEYS);
}

function buildEditorBindingDeps(ctx = {}) {
  return pickKeys(ctx, EDITOR_BINDING_KEYS);
}

function buildStartupDeps(ctx = {}) {
  return pickKeys(ctx, STARTUP_KEYS);
}

export {
  buildCanvasBindingDeps,
  buildEditorBindingDeps,
  buildStartupDeps,
  buildToolbarBindingDeps,
};
