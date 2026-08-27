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

//! The transport seam between Portal's orchestration logic and the LiveKit
//! room.
//!
//! [`Portal`](crate::portal::Portal) contains all of Portal's protocol logic —
//! role setup, state/action publishing, chunk and frame-video byte streams,
//! RPC routing, the multi-controller layer, and the sync pipeline — but never
//! touches the LiveKit SDK directly. Everything the SDK would otherwise do is
//! expressed as a [`PortalTransport`] method, and inbound room activity
//! arrives as [`TransportEvent`]s on a channel the Portal drains.
//!
//! The native LiveKit SDK transport lives in [`crate::native::LiveKitRustTransport`]
//! behind the crate's `native` cargo feature. A browser build compiles the
//! same Portal logic to WebAssembly with a JS-backed transport that
//! implements this trait over LiveKit's JS SDK — Portal code is identical on
//! both sides of the seam.
//!
//! # Threading
//!
//! On native targets, futures returned by the trait are `Send + 'static`
//! so the Portal can drive them from `tokio::spawn` on multi-threaded
//! runtimes. On `wasm32` everything runs on the single-threaded JS event
//! loop — `crate::task::spawn` maps to `spawn_local` there and needs no
//! `Send` — and JS-backed futures (`JsFuture`) are not `Send` anyway, so
//! [`TransportFuture`] drops the bound on that target (mirroring
//! `RpcHandlerFuture`).

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::mpsc::UnboundedSender;

use crate::error::PortalResult;
use crate::metrics::TrackMetrics;
use crate::portal::ObservationSink;
use crate::rpc::{RpcError, RpcHandler};
use crate::sync_buffer::SyncBuffer;
use crate::types::VideoTrackSlots;

/// Boxed future returned by every async transport method. Boxed (rather
/// than `impl Future`) because `PortalTransport` is used as
/// `Arc<dyn PortalTransport>`, which requires object safety. `Send` on
/// native targets (see the Threading section); plain on wasm32, where the
/// JS event loop is single-threaded and JS-backed futures are not `Send`.
#[cfg(not(target_arch = "wasm32"))]
pub type TransportFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

#[cfg(target_arch = "wasm32")]
pub type TransportFuture<T> = Pin<Box<dyn Future<Output = T> + 'static>>;

/// Parameters for [`PortalTransport::connect`]. The Portal passes its event
/// sink and the byte-stream topics it will consume at connect time so the
/// transport needs no set-up ordering beyond "connect once".
pub struct TransportConnect<'a> {
    /// Signal/room URL, as taken by `Portal::connect`.
    pub url: &'a str,
    /// Access token minted for this participant.
    pub token: &'a str,
    /// Inbound event channel. The transport forwards every room event it
    /// translates into this sender; the Portal drains the receiving side on
    /// its own task. Closed when the Portal drops its receiver, which is the
    /// signal for the pump to stop.
    pub events: UnboundedSender<TransportEvent>,
    /// Byte-stream topics the Portal will consume. Streams opened on other
    /// topics are dropped without being read — mirroring the SDK's
    /// take-or-drop reader semantics, and keeping the receive hot path free
    /// for peers sharing the room.
    pub byte_stream_topics: HashSet<String>,
}

/// An inbound room event, translated out of the SDK's own event enum into
/// transport-agnostic data.
#[derive(Debug)]
pub enum TransportEvent {
    /// A data packet arrived with a topic. Packets without a topic are not
    /// forwarded (Portal's protocol never sends one). `sender` is the
    /// publishing participant's identity, `None` when the SDK did not
    /// attribute the packet.
    DataReceived {
        payload: Bytes,
        topic: String,
        sender: Option<String>,
    },
    /// A byte stream opened on a subscribed topic finished reading. One
    /// stream is one payload (Portal sends chunk/frame payloads as
    /// single-stream sends).
    ByteStream {
        topic: String,
        sender: String,
        payload: Bytes,
    },
    /// A remote video track was subscribed. The transport is expected to
    /// have remembered the track; the Portal decides (by role/config)
    /// whether to call [`PortalTransport::start_video_receiver`] for it.
    VideoTrackSubscribed { track_name: String },
    /// A participant joined the room.
    ParticipantConnected(ParticipantInfo),
    /// A participant's attributes changed.
    ParticipantAttributesChanged(ParticipantInfo),
    /// A participant left the room.
    ParticipantDisconnected { identity: String },
    /// The underlying connection reconnected. Portal clears stale receive
    /// state on this event.
    Reconnected,
}

/// Identity + attribute snapshot of a room participant. The transport's
/// view of the SDK's participant object, reduced to what Portal's protocol
/// logic reads.
#[derive(Debug, Clone)]
pub struct ParticipantInfo {
    pub identity: String,
    pub attributes: HashMap<String, String>,
}

/// An outbound RPC invocation, translated out of the SDK's `PerformRpcData`.
#[derive(Debug, Clone)]
pub struct TransportRpcRequest {
    pub destination: String,
    pub method: String,
    pub payload: String,
    /// `None` lets the transport pick its default timeout.
    pub response_timeout: Option<Duration>,
}

/// The core-side sinks a video receiver feeds decoded frames into: the
/// freshest-frame slots, the sync buffer, the observation dispatcher, and
/// the track's metrics. Bundled so a transport implements one spawn method
/// instead of receiving five loose arguments.
pub struct VideoSink {
    pub track_name: String,
    pub sync_buffer: Arc<Mutex<SyncBuffer>>,
    pub slots: Arc<VideoTrackSlots>,
    // `ObservationSink` is crate-internal, so this field is too. If a
    // future transport lives outside this crate and needs to dispatch
    // observations itself, promote `ObservationSink` behind a
    // doc(hidden) re-export instead of widening this field.
    // Only the native transport reads it today; a wasm build (no `native`
    // feature, JS transport not yet wired) has no reader.
    #[cfg_attr(not(feature = "native"), allow(dead_code))]
    pub(crate) obs_sink: Arc<ObservationSink>,
    pub metrics: Arc<TrackMetrics>,
}

/// Handle for aborting a video receiver the transport spawned. Transports
/// that cannot abort (single-threaded JS runtimes) may hold no-op handles.
///
/// The transport remains responsible for tearing its receivers down on
/// disconnect; this is only a convenience for early teardown.
pub trait VideoReceiverHandle: Send + Sync {
    fn abort(&self);
}

/// The room-facing transport Portal is programmed against.
///
/// All async methods return boxed [`Send`] futures (see the module docs).
/// The transport is reusable: `disconnect` must release the underlying
/// connection so a subsequent `connect` starts fresh, and Portal relies on
/// `connect` cleaning up its own partial state when it fails part-way.
pub trait PortalTransport: Send + Sync + 'static {
    /// Establish the underlying room connection and begin forwarding
    /// inbound events into `params.events`. On success the transport must
    /// be ready to accept publish calls, RPC registration, and identity
    /// queries.
    fn connect(&self, params: TransportConnect<'_>) -> TransportFuture<PortalResult<()>>;

    /// Close the underlying connection, stop forwarding events, and tear
    /// down anything spawned for the session (event pump, video receivers,
    /// per-track publishers). Must be safe to call when never connected.
    fn disconnect(&self) -> TransportFuture<PortalResult<()>>;

    /// Publish a data packet (state / action / RTT) to the room.
    fn publish_data(
        &self,
        payload: Vec<u8>,
        topic: Option<String>,
        reliable: bool,
    ) -> TransportFuture<PortalResult<()>>;

    /// Send one byte stream on `topic` (action chunks, frame-video frames).
    fn send_bytes(&self, payload: Vec<u8>, topic: &str) -> TransportFuture<PortalResult<()>>;

    /// Update this participant's attributes.
    fn set_attributes(
        &self,
        attrs: HashMap<String, String>,
    ) -> TransportFuture<PortalResult<()>>;

    /// Invoke a registered RPC method on a remote participant.
    fn perform_rpc(
        &self,
        request: TransportRpcRequest,
    ) -> TransportFuture<Result<String, RpcError>>;

    /// Register an inbound RPC method handler. May be called before or
    /// after `connect`; handlers registered while connected take effect
    /// immediately, and Portal re-applies stored handlers on reconnect.
    fn register_rpc_method(&self, method: String, handler: RpcHandler);

    /// Remove a previously registered RPC method handler.
    fn unregister_rpc_method(&self, method: &str);

    /// This Portal's own identity once connected; `None` before.
    fn local_identity(&self) -> Option<String>;

    /// This participant's current attributes (used to seed mirrored state
    /// the token may have pre-populated).
    fn local_attributes(&self) -> HashMap<String, String>;

    /// Snapshot of remote participants, for connect-time classification and
    /// peer resolution.
    fn remote_participants(&self) -> Vec<ParticipantInfo>;

    /// Start consuming a subscribed remote video track, feeding decoded
    /// frames into `sink`. Returns an optional abort handle; the transport
    /// remains responsible for tearing the receiver down on disconnect.
    fn start_video_receiver(
        &self,
        track_name: &str,
        sink: VideoSink,
    ) -> Option<Box<dyn VideoReceiverHandle>>;

    /// Publish one frame on an RTC-published video track (the WebRTC media
    /// path, as opposed to the frame-video byte-stream path which Portal
    /// drives itself). Errors if the track is not declared/published.
    fn publish_video_frame(
        &self,
        track_name: &str,
        rgb: &[u8],
        width: u32,
        height: u32,
        timestamp_us: Option<u64>,
    ) -> PortalResult<()>;

    /// Yield for `duration` on the transport's runtime. Portal never touches
    /// executor-specific timers directly, so a JS transport can back this
    /// with browser timers instead of tokio's clock.
    fn sleep(&self, duration: Duration) -> TransportFuture<()>;
}