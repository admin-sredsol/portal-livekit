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

//! Browser binding for LiveKit Portal: the wasm-bindgen mirror of the
//! UniFFI surface.
//!
//! The transport-agnostic protocol core ([`livekit_portal_core`]) compiles
//! to wasm unchanged; this crate exposes it to JavaScript. The native
//! build links libwebrtc; a browser build cannot, so the LiveKit room
//! connection is supplied by JavaScript instead:
//!
//! * **Outbound** — the embedder constructs a JS object implementing the
//!   `JsTransport` contract ([`transport`]) over `livekit-js` (room
//!   connect, data publish, byte streams, RPC, attributes) and hands it to
//!   [`portal::WasmPortal::connect`]. Portal calls it through a Rust
//!   adapter implementing the crate-agnostic [`PortalTransport`] seam.
//! * **Inbound** — the JS transport forwards room activity into the
//!   [`sink::PortalEventSink`] it received at connect time; events flow
//!   through the same channel the native transport pumps, so every
//!   protocol path in core is shared.
//! * **Video** — the browser decodes frames itself (canvas / WebCodecs)
//!   and pushes decoded RGB through [`portal::WasmPortal::ingest_video_frame`],
//!   which routes them through the same slots / sync-buffer / observation
//!   pipeline native receivers feed. Frame-video byte streams still cross
//!   as events; core decodes them.
//!
//! Everything else — role setup, state/action publishing, chunk and
//! frame-video byte streams, RPC routing, the multi-controller layer, and
//! the sync pipeline — is core, identical to native.
//!
//! This crate is wasm-only: every module (and every dependency) is gated
//! on `target_arch = "wasm32"`, so native workspace builds see an empty
//! stub.

#[cfg(target_arch = "wasm32")]
pub mod config;
#[cfg(target_arch = "wasm32")]
pub mod portal;
#[cfg(target_arch = "wasm32")]
pub mod sink;
#[cfg(target_arch = "wasm32")]
pub mod transport;

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::wasm_bindgen;

/// Module setup: panic hook (panics reach the devtools console instead of
/// vanishing with `RuntimeError: unreachable`) and a console logger so
/// core's `log` output is visible while debugging.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(start)]
fn start() {
    std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    wasm_logger::init(wasm_logger::Config::default());
}