import { DESKTOP_RUST_OPERATOR_TYPES } from "./rust_operator_manifest.generated.js";
import { RUST_OPERATOR_PRESENTATIONS } from "./rust-operator-presentations.js";
import {
  buildRustOperatorPalettePolicy,
} from "./rust-operator-palette-policy.js";
import {
  DESKTOP_RUST_OPERATOR_METADATA,
} from "./rust_operator_manifest.generated.js";
import { LOCAL_NODE_PRESENTATIONS } from "./local-node-presentations.js";
import {
  buildLocalNodePalettePolicy,
} from "./local-node-palette-policy.js";

const localPalettePolicy = buildLocalNodePalettePolicy(LOCAL_NODE_PRESENTATIONS);
if (!localPalettePolicy.ok) {
  throw new Error(localPalettePolicy.errors.join("; "));
}
export const LOCAL_NODE_CATALOG = Object.freeze(localPalettePolicy.entries);

const palettePolicy = buildRustOperatorPalettePolicy(DESKTOP_RUST_OPERATOR_METADATA, RUST_OPERATOR_PRESENTATIONS);
if (!palettePolicy.ok) {
  throw new Error(palettePolicy.errors.join("; "));
}
export const RUST_NODE_CATALOG = Object.freeze(palettePolicy.entries);

export const NODE_CATALOG = Object.freeze([
  ...LOCAL_NODE_CATALOG,
  ...RUST_NODE_CATALOG,
]);
