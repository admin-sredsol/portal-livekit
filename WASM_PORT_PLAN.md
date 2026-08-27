# Portal → WebAssembly: Feasibility & Port Plan

Assessment of this fork (clean clone of livekit/portal @ `999c118`) for compiling the Rust
core to WebAssembly and making Portal usable from a browser operator UI.

> **Status (updated after Phase 4).** All four phases — 0 through 4 — are
> **done**. The browser port is complete: `npm run build` in
> `livekit-portal-wasm/npm` produces the publishable package, and the
> `wasm` GitHub Actions workflow builds it and runs the smoke test on every
> push/PR.
>
> - *Phases 0–1* moved the protocol core into `livekit-portal-core` with the native
>   `LiveKitRustTransport` inside it behind a `native` cargo feature; the parent
>   `livekit-portal` crate is a thin facade (FFI untouched), and a wasm build is
>   simply "core with default features".
> - *Phase 2* added three things to core: executor-agnostic task spawning
>   (`task.rs` — `tokio::spawn` natively, `spawn_local` + `CancelToken` on wasm32),
>   `Portal::ingest_video_frame` so a browser pushes decoded RGB frames through the
>   same slots/sync-buffer/observation pipeline as the native receiver, and
>   target-gated future bounds — `TransportFuture`/`RpcHandlerFuture` are `+ Send`
>   only off-wasm (JS event loop is single-threaded and `JsFuture` isn't `Send`).
>   New crate `livekit-portal-wasm` (cdylib + rlib, empty stub on native targets)
>   exposes the JS seam: `WasmPortalConfig` builder mirroring the UniFFI surface,
>   `WasmPortal` (connect/send_*/get_*/metrics/callbacks, all values as plain JS
>   objects via `serde_wasm_bindgen`), `PortalEventSink` (JS→Rust inbound events +
>   `invokeRpcMethod` promise dispatch), and the `JsTransport` contract the
>   TypeScript adapter implements (13 camelCase methods mirroring `PortalTransport`).
> - Verified: native `cargo check --workspace` clean; 117 core tests pass with
>   `--features native` (incl. 6 new `ingest_video_frame` tests on a `FakeTransport`);
>   `cargo check` + `cargo clippy` for `livekit-portal-core` +
>   `livekit-portal-wasm` on `wasm32-unknown-unknown` clean.
> - *Phase 3* added `livekit-portal-wasm/ts/livekit-js-transport.ts` — the
>   reference `LiveKitJsTransport` implementing the `JsTransport` contract over
>   livekit-js: byte streams via per-topic `registerByteStreamHandler`, RPC via
>   `room.registerRpcMethod` (rejections carry `RpcError`'s
>   `{code, message, data}`), canvas-capture publishing for WebRTC video,
>   and embedder-driven decode into `ingestVideoFrame` on the receive side.
> - Verified: `tsc --noEmit` strict against livekit-client's own type
>   declarations (TS 5.9); wasm crate doc-only change after.
> - *Phase 4* added the npm package (`livekit-portal-wasm/npm/`) and CI. A
>   Node end-to-end smoke test (`npm/test/smoke.mjs`) drives two wasm
>   `WasmPortal`s — a robot and an operator — through a mock `JsTransport`
>   pair: role classification via attributes, state publish → receive,
>   RPC both directions with the error wire shape, frame-video byte
>   streams, `ingestVideoFrame`, and metrics. The smoke test caught four
>   real bugs pre-commit (see Phase 4 section). The published surface
>   keeps wasm-bindgen's generated `.d.ts`; all u64 values (timestamps,
>   RTT, counters) cross as f64 Numbers, not BigInt.
> - Verified: smoke test green; native `cargo check --workspace` + 117
>   core tests + wasm32 clippy (`-D warnings`) still clean after the
>   fixes; full `npm run build` → `npm test` → `require()` path exercised.
> - CI: `.github/workflows/wasm.yml` — wasm32 target, wasm-bindgen-cli
>   version-locked to `Cargo.lock`, package build, wasm32 clippy, adapter
>   typecheck, and the smoke test, pinned to the same action SHAs as
>   `tests.yml`.

## 1. Verdict

| Question | Answer |
|---|---|
| Can the whole workspace compile to `wasm32-unknown-unknown` as-is? | **No.** Fails at the `livekit` SDK (native libwebrtc C++), `yuv-sys` (libyuv C++), `tokio/rt-multi-thread`, and UniFFI (no JS backend). |
| Can the core library be made WASM-buildable? | **Yes.** ~65% of `livekit-portal` compiles to wasm unchanged. The rest needs one refactor: a transport seam replacing direct `livekit::` calls. |
| Is a browser-friendly Portal realistic? | Yes — WASM core + TypeScript transport built on livekit-js. The browser becomes a first-class operator speaking the identical wire protocol; robots keep running the native SDK unchanged. |

## 2. Verified module inventory

Derived from grepping every source file for `livekit::`, `tokio::time/spawn`, `yuv_sys`,
and `std::net/fs/thread` usage.

### Tier 1 — compiles to wasm today (no changes)

| Module | Size | Notes |
|---|---|---|
| `sync_buffer.rs` | 57.7 KB | The fusion core (O(N+M) two-pointer matching). std-only + crate types. |
| `serialization.rs` | 24.8 KB | Wire protocol, schema/action fingerprints. std-only. |
| `dtype.rs` | 8.7 KB | |
| `types.rs` | 13.3 KB | `bytes::Bytes` — wasm-safe. |
| `metrics.rs` | 22.6 KB | parking_lot + atomics — wasm-safe. |
| `codec.rs` | 15.0 KB | PNG/JPEG via `image` (pure-Rust codecs) — wasm-safe. |
| `config.rs` | 22.1 KB | |
| `config_file.rs` | 22.3 KB | One `std::fs::read_to_string` — gate behind `not(target_arch = "wasm32")`; `from_yaml_str` works everywhere. |
| `error.rs`, `lib.rs` | small | |
| `rpc.rs` | 3.4 KB | ~90% portable; only the `From<livekit::prelude::RpcError>` conversions touch the SDK — split trivially. |

Dependencies of these modules: `serde`, `serde-saphyr`, `thiserror`, `parking_lot`,
`bytes`, `image (png,jpeg)` — all pure Rust, all wasm-safe. `std::time::Instant`
(used by SyncBuffer) is implemented on wasm32-unknown-unknown.

### Tier 2 — reusable logic behind a transport seam

| Module | Size | Coupling found | What stays / what moves |
|---|---|---|---|
| `portal.rs` | 77.4 KB | `Room::connect`, `RoomOptions` + E2EE key provider, `RoomEvent` dispatch loop (8 event kinds), `LocalParticipant` (publish/set_attributes/RPC/byte streams), `tokio::spawn`, one `tokio::time::sleep` | All session/controller/callback logic stays. Room/event/participant calls move behind a `Transport` trait. |
| `data.rs` | 36.7 KB | `StreamByteOptions`, `prelude` (`publish_data`), `tokio::spawn` | DataPublisher/ChunkPublisher framing & sequencing stays; the send call becomes a trait method. |
| `frame_video.rs` | 20.0 KB | `StreamByteOptions`, `prelude`, `tokio::spawn` | Same as data.rs. PNG/MJPEG encode already lives in portable `codec.rs`. |
| `rtt.rs` | 4.8 KB | `prelude` (publish), `tokio::time::interval`, `tokio::spawn` | Ping/pong logic stays; interval → pluggable timer; publish → trait. |
| `video.rs` | 20.6 KB | `NativeVideoSource/Stream`, `TrackPublishOptions`, `FrameMetadata`, `yuv_sys` (2 unsafe calls) | Publish/receive slots + metadata logic stays. I420 conversion and track I/O are browser-provided. |

### Tier 3 — replaced, not compiled, on web

| Dependency | Why blocked | Browser replacement |
|---|---|---|
| `livekit` crate → `libwebrtc`/`webrtc-sys` | Prebuilt native C++ WebRTC; no wasm target, no web-sys/js-sys anywhere in rust-sdks | livekit-js (client-sdk-js) — LiveKit's intended browser SDK |
| `yuv-sys` | ~85K lines of C++ built natively | Canvas / WebCodecs (browsers convert RGB↔I420 natively) |
| `tokio` `rt-multi-thread`, `tokio::time` | Doesn't exist on wasm; timers unreliable | Current-thread semantics via wasm-bindgen-futures; `gloo-timers`/`futures-timer` |
| UniFFI + `livekit-portal-ffi` | UniFFI targets Python/Swift/Kotlin, not JS; also `ctor`, `env_logger` | wasm-bindgen + tsify (new crate; FFI crate untouched) |

## 3. Target architecture

```
┌─────────────────────────── Browser (Operator) ───────────────────────────┐
│  your teleop UI / policy loop (TS/JS)                                    │
│      │                                                                   │
│      ▼                                                                   │
│  @livekit/portal-web  (wasm-bindgen generated, tsify types)              │
│  ┌──────────────────────────────────────────────────────────────┐        │
│  │ WASM: livekit-portal-core                                    │        │
│  │  Portal (connect/send_*/on_*/metrics)                        │        │
│  │  SyncBuffer · serialization · codec · metrics · rpc types    │        │
│  │  ── Transport trait ─────────────────────────────────────    │        │
│  └──────────────────────────────┬───────────────────────────────┘        │
│                                 │ implemented by                         │
│                    TS adapter: LiveKitJsTransport                        │
│                                 ▼                                        │
│                       livekit-js (WebRTC + signalling)                   │
└──────────────────────────────────┬───────────────────────────────────────┘
                                   │  LiveKit room (unchanged wire protocol,
                                   ▼  lk.portal.* attributes, topics)
              Robot host: livekit-portal (native Rust) + Python bindings
```

The contract that makes this work already exists and is documented:
`docs/reference/wire-protocol.md` (403 lines — topics, payload layouts, fingerprints),
plus the `lk.portal.role` / `lk.portal.active_operator` attribute protocol and
`portal.set_active_operator` RPC defined in `portal.rs`.

## 4. The Transport trait (Phase 1 — the real work)

Verified surface portal.rs/data.rs/video.rs/rtt.rs/rpc.rs use from the SDK, which the
trait must cover:

```rust
// livekit-portal-core/src/transport.rs
#[async_trait::async_trait]
pub trait PortalTransport: Send + Sync {
    // outbound
    async fn publish_data(&self, payload: Vec<u8>, topic: Option<String>,
                          reliable: bool, destinations: Vec<String>) -> Result<()>;
    async fn open_byte_stream(&self, topic: String,
                              opts: ByteStreamOptions) -> Result<ByteStreamSink>;
    async fn set_attributes(&self, attrs: HashMap<String, String>) -> Result<()>;
    async fn perform_rpc(&self, dest: &str, method: &str,
                         payload: String, timeout: Duration) -> Result<String>;
    // inbound registration
    fn on_data(&self, cb: Box<dyn Fn(DataPacketEvent) + Send + Sync>);
    fn on_byte_stream(&self, cb: Box<dyn Fn(ByteStreamEvent) + Send + Sync>);
    fn on_participant_connected(&self, cb: IdentityCb);
    fn on_participant_disconnected(&self, cb: IdentityCb);
    fn on_attributes_changed(&self, cb: Box<dyn Fn(ParticipantAttrsEvent) + Send + Sync>);
}
// Video track publish/subscribe is browser-side on web; native impl uses the
// livekit SDK. Frame exchange with the core stays `VideoFrameData { rgb, w, h, ts }`.
```

Notes from the code:

- `rpc.rs` already isolates `RpcInvocationData`/`RpcError` as portable types with thin
  `From<livekit::…>` conversions — exactly the pattern to replicate.
- `data.rs`'s `DataSlot`/`ChunkSlot` callback plumbing and `ChunkPublisher` sequencing
  are transport-agnostic once the byte-stream sink is a trait object.
- `portal.rs`'s `ControllerState` (active-operator pointer, operator set), the
  `set_active_operator` RPC handler, callback registries, and metrics are all portable.
- Only `connect()` (RoomOptions/E2EE/event-loop wiring) and `video.rs`'s
  Native* plumbing are SDK-specific.

This refactor pays for itself twice: it also enables transport-mocked integration tests
with no LiveKit server.

## 5. Phased plan

**Phase 0 — prove Tier 1 compiles (≈ a day).**
New workspace member `livekit-portal-core` (move the 10 Tier-1 files + rpc types).
`cargo build -p livekit-portal-core --target wasm32-unknown-unknown` should pass with
only the `config_file.rs` fs-gate. Existing crate re-exports from core, so the Python
stack is untouched.

**Phase 1 — introduce `PortalTransport` (bulk of the effort).**
Rework `portal.rs`, `data.rs`, `frame_video.rs`, `rtt.rs`, `video.rs` to program against
the trait (as sketched above). Keep the native `LiveKitRustTransport` implementing it —
native behavior must not change. Gate tokio: `rt` (current-thread) + `sync` + `macros`
for wasm builds; `rt-multi-thread` only in native builds (one `cfg` in the workspace
dep table). Replace direct `tokio::time` in `rtt.rs`/`portal.rs` with an injected
timer abstraction (native impl = tokio; wasm impl = futures-timer/gloo).

**Phase 2 — wasm-bindgen crate (moderate). ✅ Done** (crate `livekit-portal-wasm`;
hand-built JS conversions instead of tsify; callbacks registered as `js_sys::Function`
closures; `PortalEventSink` carries inbound events + RPC dispatch to JS).

**Phase 3 — TS transport adapter (few hundred lines). ✅ Done**
(`livekit-portal-wasm/ts/livekit-js-transport.ts`; typechecks against
livekit-client's own types, strict). `LiveKitJsTransport implements JsTransport`
over livekit-js: `publishData` (reliability + topic), `sendBytes` on declared
byte-stream topics via `room.registerByteStreamHandler` (one finished stream =
one payload, accumulated chunks concatenated), `performRpc` (rejects carry
livekit's `RpcError` — same `{code, message, data}` shape the Rust adapter
reads), inbound RPC via `room.registerRpcMethod` wrapping
`sink.invokeRpcMethod`, `setAttributes`, and the event hooks Portal needs.
Video: on send, a lazily published canvas-backed track per track name
(`captureStream(0)` + `requestFrame` per frame, sender timestamp ignored —
WebRTC stamps its own clock); on receive, the transport records subscribed
tracks and the embedder decodes into `WasmPortal.ingestVideoFrame`. No libyuv
anywhere — the browser handles I420 inside its WebRTC pipeline. Two subtleties
worth knowing: RPC handlers registered before `connect` (and Portal's
reconnect re-application) are buffered/de-duplicated because livekit throws
on duplicate registration; and `startVideoReceiver` must NOT re-notify the
sink — core already dispatched the `VideoTrackSubscribed` event before
calling it, so a re-notify would loop the event channel.

**Phase 4 — packaging & CI. ✅ Done.**
The plan's `wasm-pack` was swapped for plain `cargo build` + `wasm-bindgen`
(same output; `wasm-pack`'s packing step fights a cargo workspace and adds
nothing here) — `livekit-portal-wasm/npm/` holds the publishable package:
`package.json` (no `"type"` field on purpose: the wasm-bindgen `nodejs`
artifact is CommonJS while `web` is ESM, so dist/node loads as CJS with
standard named-export interop and bundlers take dist/web by syntax; the
transport adapter is compiled to `.mjs` to stay ESM everywhere),
`build.mjs` (release cdylib → wasm-bindgen `--target web`/`--target nodejs`
→ `tsc` compile of `../ts/livekit-js-transport.ts`; requires npm installs
in both `npm/` and `../ts/` — the latter provides the livekit-client types
the adapter resolves), and `test/smoke.mjs`. The smoke test runs two wasm
`WasmPortal`s (robot + operator) through a mock `JsTransport` room in one
Node process and caught four real bugs pre-commit: the JS surface was
snake_case (missing `js_name` attrs), `serde_wasm_bindgen` serialized
`setAttributes` maps as JS `Map`s where the contract (and livekit-js)
wants plain objects, `SystemTime::now()` panics on wasm32 (core's
`now_us` is now `js_sys::Date::now()` there), and every u64 crossed as a
BigInt (timestamps/RTT/counters are f64 Numbers now). CI:
`.github/workflows/wasm.yml` runs the wasm32 build, wasm-bindgen-cli
pinned to the `Cargo.lock` version, wasm32 clippy with `-D warnings`,
adapter typecheck, and the smoke test — the "browser smoke test" option
of running two synthetic peers over LiveKit Cloud stays available later;
the mock transport already exercises the identical seam without network
flakiness.

## 6. Browser-friendly concerns & risks

1. **Threading model.** Current code `tokio::spawn`s background tasks (event loop,
   RTT pings, chunk senders, frame drain). On wasm these become spawn_local tasks or
   plain callbacks — mechanical, but every `JoinHandle::abort` on Drop (data.rs, rtt.rs,
   frame_video.rs, portal.rs) needs a web equivalent (CancelToken or storing closures).
2. **Parity risk (E2EE + video codec).** Native path uses libwebrtc + libyuv with
   `VideoCodec`/`VideoEncoding` publish options; browser path uses livekit-js's
   publisher. Pixel-exact frame-video (RAW/PNG/MJPEG over data channels) is transport
   independent and fully preserved; WebRTC-track video quality/bandwidth behavior may
   differ slightly between SDKs.
3. **Clocks.** SyncBuffer relies on the sender's monotonic `performance.timeOrigin +
   performance.now()` — same monotonic-clock assumption the native side makes
   (`now_us`); document that operators must not skew-adjust.
4. **Bundle size.** Core wasm (sync + serialization + codec + metrics) should land
   well under 1 MB gz; the `image` codecs are the biggest chunk — feature-gate if only
   RAW frame-video is used.
5. **`send_video_frame` ergonomics.** On web, callers pass `Uint8Array` RGB24 into
   wasm; zero-copy via `js_sys::Uint8Array` view into `Bytes` is possible.
6. **Upstream drift.** The fork tracks livekit/portal; Phases 0–1 are additive
   (core split + trait), so rebasing stays cheap. Consider upstreaming the Transport
   trait — LiveKit itself just moved this direction (`livekit-net`'s
   "pluggable, host-providable network transport").

## 7. Alternative worth naming

Skip WASM: reimplement the documented wire protocol + SyncBuffer in TypeScript
(wire-protocol.md is the spec; SyncBuffer is the only algorithmically subtle part).
Lower ceiling on fidelity guarantees, but zero build tooling. Choose WASM if you want
guaranteed behavioral parity with the native SDK — which is the usual reason to do
this at all.