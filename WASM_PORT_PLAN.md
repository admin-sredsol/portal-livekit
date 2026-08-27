# Portal → WebAssembly: Feasibility & Port Plan

Assessment of this fork (clean clone of livekit/portal @ `999c118`) for compiling the Rust
core to WebAssembly and making Portal usable from a browser operator UI.

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

**Phase 2 — wasm-bindgen crate (moderate).**
New crate `livekit-portal-wasm` (cdylib, `wasm-pack build --target web`):
- Mirror the UniFFI surface 1:1 — it's already the right shape: `PortalConfig`
  builder (`add_video`, `add_state_typed`, `add_action_typed`, `add_action_chunk`,
  `set_*`), `Portal` (`connect`, `send_video_frame`, `send_state`, `send_action`,
  `send_action_chunk`, `set_active_operator`, `perform_rpc`, `get_*`, `metrics`).
- `PortalCallbacks` (9 methods) becomes JS closures (`Closure<dyn FnMut(_)>`); tsify
  emits TS types for `Observation`, `State`, `Action`, `ActionChunk`,
  `VideoFrameData`, `PortalMetrics`.
- Runtime: wasm-bindgen-futures; spawn_local or callback-driven tasks; single-threaded
  is fine (the browser JS event loop replaces the tokio reactor; there is no
  rt-multi-thread on wasm and none is needed).
- E2EE: keep the shared-key GCM config in the core config; enforcement moves to
  livekit-js (which supports E2EE) — note the parity risk below.

**Phase 3 — TS transport adapter (few hundred lines).**
`LiveKitJsTransport implements PortalTransport` over livekit-js:
`publishData` (with reliability + destination_identities), `streamText`/byte-stream
options, `performRpc`, participant attributes + the five event hooks Portal needs.
Video: on send, camera frames go straight out as a livekit-js track (or frame-video
path: `canvas.toBlob` PNG/JPEG → wasm chunker); on receive, `<video>`/canvas →
`VideoFrameData` bytes → wasm SyncBuffer. No libyuv anywhere — the browser handles
I420 inside its WebRTC pipeline.

**Phase 4 — packaging & CI.**
`wasm-pack` → npm package (ESM, web target); GitHub Actions job adding
`wasm32-unknown-unknown` build + a browser smoke test (two synthetic Portal peers via
LiveKit Cloud sandbox or a mock transport).

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