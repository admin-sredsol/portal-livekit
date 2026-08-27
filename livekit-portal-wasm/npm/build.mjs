// Copyright 2026 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Builds the publishable package into `dist/`:
 *
 *   dist/web/       wasm-bindgen `--target web` artifact (browsers, bundlers)
 *   dist/node/      wasm-bindgen `--target nodejs` artifact (Node, smoke test)
 *   dist/transport/ compiled LiveKitJsTransport (ESM, .mjs + .d.ts)
 *
 * The package deliberately declares no `"type"`: the wasm-bindgen `nodejs`
 * artifact is CommonJS (`exports.X = ...`) while the `web` artifact is ESM.
 * Node loads dist/node as CJS and ESM importers get named exports via
 * standard interop; bundlers load dist/web by syntax. The transport adapter
 * is compiled with `--module esnext` and renamed to `.mjs` so it parses as
 * ESM in every runtime.
 *
 * Requires: rustup toolchain from rust-toolchain.toml, the
 * `wasm32-unknown-unknown` target, `wasm-bindgen-cli` at the same version as
 * the crate's `wasm-bindgen` dependency (0.2.127 — the CLI and crate must
 * match exactly), and npm installs in BOTH this directory (provides the
 * `tsc` binary) and `../ts/` (provides the `livekit-client` types the
 * adapter source resolves against).
 */

import { spawnSync } from "node:child_process";
import { mkdirSync, renameSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const npmDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(npmDir, "..", "..");
const crateDir = join(repoRoot, "livekit-portal-wasm");
const dist = join(npmDir, "dist");

function run(cmd, args, opts = {}) {
  const result = spawnSync(cmd, args, { stdio: "inherit", ...opts });
  if (result.status !== 0) {
    throw new Error(`${cmd} ${args.join(" ")} failed with status ${result.status}`);
  }
}

// 1. Release cdylib for wasm32. The crate's non-wasm deps compile to empty
// stubs, so no native SDK (libwebrtc) is touched on this target.
run("cargo", [
  "build",
  "-p",
  "livekit-portal-wasm",
  "--target",
  "wasm32-unknown-unknown",
  "--release",
], { cwd: repoRoot });

const wasm = join(
  repoRoot,
  "target",
  "wasm32-unknown-unknown",
  "release",
  "livekit_portal_wasm.wasm",
);

// 2. wasm-bindgen for both runtimes. The generated .d.ts is the package's
// type surface (hand-built JsValue conversions still produce full typings).
rmSync(dist, { recursive: true, force: true });
mkdirSync(dist, { recursive: true });
for (const target of ["web", "nodejs"]) {
  run("wasm-bindgen", [
    wasm,
    `--target=${target}`,
    "--out-dir",
    join(dist, target === "web" ? "web" : "node"),
    "--out-name",
    "livekit_portal_wasm",
  ]);
}

// 3. Compile the LiveKitJsTransport adapter (source lives next to the crate
// in ../ts/). livekit-client types come from this package's devDependencies.
// Renamed to .mjs after compilation: the package has no `"type"` field (see
// the header comment), so a plain .js would parse as CJS in Node.
mkdirSync(join(dist, "transport"), { recursive: true });
run("./node_modules/.bin/tsc", [
  join(npmDir, "..", "ts", "livekit-js-transport.ts"),
  "--outDir",
  join(dist, "transport"),
  "--module",
  "esnext",
  "--target",
  "es2022",
  "--moduleResolution",
  "bundler",
  "--lib",
  "es2022,dom",
  "--declaration",
  "--skipLibCheck",
], { cwd: npmDir });
const transportDir = join(dist, "transport");
renameSync(
  join(transportDir, "livekit-js-transport.js"),
  join(transportDir, "livekit-js-transport.mjs"),
);

console.log("package built into", dist);