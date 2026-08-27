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

//! The native LiveKit SDK transport: [`PortalTransport`] implemented over
//! the `livekit` Rust SDK (libwebrtc + tokio). Only compiled with the
//! `native` cargo feature — never on wasm32, where a JS transport implements
//! the same trait instead.
//!
//! Everything the old monolithic `Portal` did *because* it held a
//! `LocalParticipant` lives here now:
//!
//! * Room options (SDK identity header, auto-subscribe, optional E2EE).
//! * WebRTC-path video track publication (robot-side) and frame encoding on
//!   the media path (`publish_video_frame`).
//! * The event pump: LiveKit `RoomEvent`s are translated into
//!   transport-agnostic [`TransportEvent`]s and forwarded to core, which
//!   owns all protocol semantics.
//! * RPC method registration against the `LocalParticipant`, including the
//!   runtime-`enter` guard so handlers can be registered from foreign
//!   threads (a binding's asyncio loop) without panicking on `tokio::spawn`.
//! * Native video receive: `NativeVideoStream` + `VideoReceiver`.
//!
//! # Ownership contract
//!
//! `Portal` stores `Arc<dyn PortalTransport>` only after `connect()`
//! resolves. Any failure *inside* `connect()` must therefore clean the room
//! up itself — core never obtained the slot and will not call
//! `disconnect()` for it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use livekit::prelude::*;
use livekit::webrtc::video_stream::native::NativeVideoStream;
use livekit::StreamByteOptions;
use parking_lot::Mutex;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::PortalConfig;
use crate::error::{PortalError, PortalResult};
use crate::metrics::MetricsRegistry;
use crate::rpc::{RpcError, RpcHandler, RpcInvocationData};
use crate::transport::{
    ParticipantInfo, PortalTransport, TransportConnect, TransportEvent, TransportFuture,
    TransportRpcRequest, VideoSink,
};
use crate::video::{VideoPublisher, VideoReceiver};

/// SDK-facing state guarded by one mutex. Everything the transport touches
/// post-connect lives here so `disconnect` can tear the session down
/// atomically and a failed `connect` leaves no residue.
struct NativeState {
    room: Option<Room>,
    lp: Option<LocalParticipant>,
    /// Runtime `connect` ran on, so `register_rpc_method` can be called
    /// from a foreign thread without panicking on the SDK's internal spawn.
    runtime_handle: Option<Handle>,
    /// WebRTC-path video publishers (robot-side), keyed by track name.
    video_publishers: HashMap<String, Arc<VideoPublisher>>,
    /// Native video receivers spawned on demand (operator-side), keyed by
    /// track name. Matches the old Portal's map semantics.
    video_receivers: HashMap<String, VideoReceiver>,
    /// Event pump task translating `RoomEvent` -> `TransportEvent`.
    pump_task: Option<JoinHandle<()>>,
    /// Remote video tracks observed by the pump, keyed by publication name,
    /// shared with the pump so `start_video_receiver` can resolve the track
    /// core asked about.
    video_tracks: Arc<Mutex<HashMap<String, RemoteVideoTrack>>>,
}

/// The LiveKit Rust SDK behind [`PortalTransport`]. Constructed by
/// `Portal::connect` on native targets; a wasm build uses a JS transport
/// implementing the same trait and never links this type.
pub struct LiveKitRustTransport {
    config: PortalConfig,
    metrics: Arc<MetricsRegistry>,
    state: Arc<Mutex<NativeState>>,
}

impl LiveKitRustTransport {
    pub fn new(config: PortalConfig, metrics: Arc<MetricsRegistry>) -> Self {
        Self {
            config,
            metrics,
            state: Arc::new(Mutex::new(NativeState {
                room: None,
                lp: None,
                runtime_handle: None,
                video_publishers: HashMap::new(),
                video_receivers: HashMap::new(),
                pump_task: None,
                video_tracks: Arc::new(Mutex::new(HashMap::new())),
            })),
        }
    }
}

impl PortalTransport for LiveKitRustTransport {
    fn connect(&self, params: TransportConnect<'_>) -> TransportFuture<PortalResult<()>> {
        let config = self.config.clone();
        let metrics = self.metrics.clone();
        let state = Arc::clone(&self.state);
        let url = params.url.to_string();
        let token = params.token.to_string();
        let events_tx = params.events.clone();
        let byte_stream_topics = params.byte_stream_topics.clone();
        Box::pin(async move {
            static OTHER_SDKS_VALUE: &str = concat!("portal:", env!("CARGO_PKG_VERSION"));

            let mut options = RoomOptions::default();
            options.sdk_options.other_sdks = Some(OTHER_SDKS_VALUE.to_string());
            options.auto_subscribe = true;
            if let Some(key) = &config.shared_key {
                use livekit::E2eeOptions;
                use livekit::e2ee::{
                    EncryptionType,
                    key_provider::{KeyProvider, KeyProviderOptions},
                };
                let key_provider =
                    KeyProvider::with_shared_key(KeyProviderOptions::default(), key.clone());
                options.encryption =
                    Some(E2eeOptions { key_provider, encryption_type: EncryptionType::Gcm });
            }

            log::info!("[{}] connecting to {}", config.session, url);

            let (room, events) = Room::connect(&url, &token, options)
                .await
                .map_err(|e| PortalError::Room(e.to_string()))?;

            let lp = room.local_participant();

            // Publish WebRTC-path video tracks (robot-side; config drives the
            // list). The old Portal did this inside `setup_robot` after the
            // role attribute landed; the transport now owns the media path, so
            // publication happens here. If any publish fails, close the room —
            // core never stored the transport slot, so it will not call
            // `disconnect()` for us.
            let mut video_publishers = HashMap::new();
            for spec in &config.video_tracks {
                let track_metrics = metrics
                    .track(&spec.name)
                    .expect("track metrics registered at construction");
                let publisher = VideoPublisher::new(
                    &spec.name,
                    track_metrics,
                    config.fps,
                    spec.codec,
                    spec.max_bitrate_kbps,
                    spec.simulcast,
                    spec.screencast,
                );
                if let Err(e) = publisher.publish(&lp).await {
                    let _ = room.close().await;
                    return Err(e);
                }
                log::info!("[{}] published video track '{}'", config.session, spec.name);
                video_publishers.insert(spec.name.clone(), Arc::new(publisher));
            }

            // Spawn the event pump before returning: core expects events to
            // flow as soon as `connect` resolves.
            let video_tracks = Arc::new(Mutex::new(HashMap::new()));
            let pump_task = spawn_event_pump(
                config.session.clone(),
                events,
                events_tx,
                byte_stream_topics,
                Arc::clone(&video_tracks),
            );

            let runtime_handle = Handle::current();
            *state.lock() = NativeState {
                room: Some(room),
                lp: Some(lp),
                runtime_handle: Some(runtime_handle),
                video_publishers,
                video_receivers: HashMap::new(),
                pump_task: Some(pump_task),
                video_tracks,
            };

            log::info!("[{}] transport connected", config.session);
            Ok(())
        })
    }

    fn disconnect(&self) -> TransportFuture<PortalResult<()>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            // Take everything out under the lock, release the lock, then
            // await on close. Holding a parking_lot guard across `.await`
            // would make the future !Send.
            let (room, lp, pump_task, receivers) = {
                let mut state = state.lock();
                let pump_task = state.pump_task.take();
                let receivers = std::mem::take(&mut state.video_receivers);
                state.video_publishers.clear();
                state.video_tracks.lock().clear();
                (state.room.take(), state.lp.take(), pump_task, receivers)
            };
            let _ = lp; // dropping the LocalParticipant releases its handle
            for receiver in receivers.values() {
                receiver.abort();
            }
            if let Some(task) = pump_task {
                task.abort();
            }

            log::info!("transport disconnecting");
            // close() is best-effort; cleanup above already happened, and the
            // error (if any) is surfaced to the caller, mirroring the old
            // Portal contract.
            match room {
                Some(room) => room.close().await.map_err(|e| PortalError::Room(e.to_string())),
                None => Ok(()),
            }
        })
    }

    fn publish_data(
        &self,
        payload: Vec<u8>,
        topic: Option<String>,
        reliable: bool,
    ) -> TransportFuture<PortalResult<()>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let lp = state.lock().lp.clone();
            let Some(lp) = lp else { return Err(PortalError::NotConnected) };
            let packet = DataPacket {
                payload,
                topic,
                reliable,
                destination_identities: Vec::new(),
            };
            lp.publish_data(packet).await.map_err(|e| PortalError::Room(e.to_string()))
        })
    }

    fn send_bytes(&self, payload: Vec<u8>, topic: &str) -> TransportFuture<PortalResult<()>> {
        let state = Arc::clone(&self.state);
        let topic = topic.to_string();
        Box::pin(async move {
            let lp = state.lock().lp.clone();
            let Some(lp) = lp else { return Err(PortalError::NotConnected) };
            let options = StreamByteOptions::new_with_topic(topic);
            lp.send_bytes(payload, options)
                .await
                .map(|_info| ())
                .map_err(|e| PortalError::Room(e.to_string()))
        })
    }

    fn set_attributes(
        &self,
        attrs: HashMap<String, String>,
    ) -> TransportFuture<PortalResult<()>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let lp = state.lock().lp.clone();
            let Some(lp) = lp else { return Err(PortalError::NotConnected) };
            lp.set_attributes(attrs).await.map_err(|e| PortalError::Room(e.to_string()))
        })
    }

    fn perform_rpc(&self, request: TransportRpcRequest) -> TransportFuture<Result<String, RpcError>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let lp = state.lock().lp.clone();
            let Some(lp) = lp else {
                // Portal never routes an RPC while disconnected (the
                // transport slot is absent), so this is a defensive path.
                return Err(RpcError {
                    code: 2001,
                    message: "transport not connected".to_string(),
                    data: None,
                });
            };
            let mut data = PerformRpcData {
                destination_identity: request.destination,
                method: request.method,
                payload: request.payload,
                ..Default::default()
            };
            if let Some(t) = request.response_timeout {
                data.response_timeout = t;
            }
            lp.perform_rpc(data).await.map_err(rpc_error_from_sdk)
        })
    }

    fn register_rpc_method(&self, method: String, handler: RpcHandler) {
        let state = self.state.lock();
        let Some(lp) = &state.lp else { return };
        // The SDK's `register_rpc_method` kicks off publisher negotiation,
        // which `tokio::spawn`s internally. If we were called from a foreign
        // thread with no runtime context (a binding's asyncio loop), that
        // spawn panics. Enter the runtime `connect()` ran on so the spawn
        // lands on it. The handle is always present here (an LP exists only
        // after a successful connect set it), but fall back to a bare call
        // rather than panicking if it somehow isn't.
        match &state.runtime_handle {
            Some(handle) => {
                let _guard = handle.enter();
                register_handler_on(lp, method, handler);
            }
            None => register_handler_on(lp, method, handler),
        }
    }

    fn unregister_rpc_method(&self, method: &str) {
        let state = self.state.lock();
        if let Some(lp) = &state.lp {
            lp.unregister_rpc_method(method.to_string());
        }
    }

    fn local_identity(&self) -> Option<String> {
        let state = self.state.lock();
        state.lp.as_ref().map(|lp| lp.identity().as_str().to_string())
    }

    fn local_attributes(&self) -> HashMap<String, String> {
        let state = self.state.lock();
        state.lp.as_ref().map(|lp| lp.attributes()).unwrap_or_default()
    }

    fn remote_participants(&self) -> Vec<ParticipantInfo> {
        let state = self.state.lock();
        let Some(room) = &state.room else { return Vec::new() };
        room.remote_participants().values().map(remote_participant_info).collect()
    }

    fn start_video_receiver(
        &self,
        track_name: &str,
        sink: VideoSink,
    ) -> Option<Box<dyn crate::transport::VideoReceiverHandle>> {
        let mut state = self.state.lock();
        let Some(track) = state.video_tracks.lock().get(track_name).cloned() else {
            log::warn!(
                "[unknown-track] start_video_receiver: track '{track_name}' never seen on the \
                 transport, receiver skipped"
            );
            return None;
        };
        let stream = NativeVideoStream::new(track.rtc_track());
        let receiver = VideoReceiver::spawn(
            track_name.to_string(),
            stream,
            sink.sync_buffer,
            sink.slots,
            sink.obs_sink,
            sink.metrics,
        );
        // Keyed by track name, matching the old Portal's map semantics.
        state.video_receivers.insert(track_name.to_string(), receiver);
        // The transport owns receiver lifecycle (aborted in `disconnect`);
        // no handle is returned.
        None
    }

    fn publish_video_frame(
        &self,
        track_name: &str,
        rgb: &[u8],
        width: u32,
        height: u32,
        timestamp_us: Option<u64>,
    ) -> PortalResult<()> {
        let state = self.state.lock();
        let Some(publisher) = state.video_publishers.get(track_name) else {
            return Err(PortalError::UnknownVideoTrack { name: track_name.to_string() });
        };
        publisher.send_frame(rgb, width, height, timestamp_us)
    }

    fn sleep(&self, duration: Duration) -> TransportFuture<()> {
        Box::pin(async move { tokio::time::sleep(duration).await })
    }
}

// --- Event pump ---

/// Translate the SDK's room-event stream into core's [`TransportEvent`]
/// stream. The pump runs for the lifetime of the session: it exits when the
/// room closes (`events` yields `None`) or when core drops its receiver
/// (sends fail), and `disconnect` aborts it outright.
fn spawn_event_pump(
    session: String,
    mut events: mpsc::UnboundedReceiver<RoomEvent>,
    events_tx: mpsc::UnboundedSender<TransportEvent>,
    byte_stream_topics: HashSet<String>,
    video_tracks: Arc<Mutex<HashMap<String, RemoteVideoTrack>>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            let translated: Option<TransportEvent> = match event {
                RoomEvent::TrackSubscribed { track: RemoteTrack::Video(video_track), publication, .. } => {
                    let track_name = publication.name();
                    log::info!("[{session}] subscribed to video track '{track_name}'");
                    video_tracks.lock().insert(track_name.clone(), video_track);
                    Some(TransportEvent::VideoTrackSubscribed { track_name })
                }
                RoomEvent::TrackSubscribed { .. } => None,
                RoomEvent::DataReceived { payload, topic, participant, .. } => {
                    // Portal's protocol never sends topicless packets; the
                    // old Portal's `topic: Some(topic)` match arm dropped
                    // them, so the pump filters the same way.
                    //
                    // The SDK hands over `Arc<Vec<u8>>`; a copy into `Bytes`
                    // keeps the core-side event shape uniform (data packets
                    // are at most 15 KB, so the copy is negligible).
                    topic.map(|topic| TransportEvent::DataReceived {
                        payload: Bytes::copy_from_slice(&payload),
                        topic,
                        sender: participant.map(|p| p.identity().as_str().to_string()),
                    })
                }
                RoomEvent::ByteStreamOpened { reader, topic, participant_identity } => {
                    // Only consume the byte-stream topics core declared at
                    // connect. `byte_stream_topics` already encodes the
                    // role's ownership (action chunks for the robot and
                    // subscribed operators, frame video for frame-video
                    // operators), so streams on other topics — including
                    // this role's non-owned Portal topic — are dropped
                    // without being read.
                    if !byte_stream_topics.contains(&topic) {
                        continue;
                    }
                    let Some(reader) = reader.take() else { continue };
                    let tx = events_tx.clone();
                    let sender = participant_identity.as_str().to_string();
                    tokio::spawn(async move {
                        use livekit::StreamReader;
                        match reader.read_all().await {
                            Ok(payload) => {
                                let _ = tx.send(TransportEvent::ByteStream {
                                    topic: topic.clone(),
                                    sender,
                                    payload: Bytes::from_owner(payload),
                                });
                            }
                            Err(e) => log::warn!(
                                "[bad-payload] failed to read byte stream on '{topic}': {e}"
                            ),
                        }
                    });
                    None
                }
                RoomEvent::ParticipantConnected(participant) => {
                    Some(TransportEvent::ParticipantConnected(remote_participant_info(
                        &participant,
                    )))
                }
                RoomEvent::ParticipantAttributesChanged { participant, .. } => {
                    // Note: `participant` here is the SDK's `Participant`
                    // enum, which can be the local participant — core skips
                    // its own identity when classifying.
                    Some(TransportEvent::ParticipantAttributesChanged(ParticipantInfo {
                        identity: participant.identity().as_str().to_string(),
                        attributes: participant.attributes(),
                    }))
                }
                RoomEvent::ParticipantDisconnected(participant) => {
                    Some(TransportEvent::ParticipantDisconnected {
                        identity: participant.identity().as_str().to_string(),
                    })
                }
                RoomEvent::Reconnected => Some(TransportEvent::Reconnected),
                _ => None,
            };
            if let Some(event) = translated {
                // Core's receive loop is gone (disconnect / drop): stop
                // pumping instead of spinning on failed sends.
                if events_tx.send(event).is_err() {
                    break;
                }
            }
        }
    })
}

fn remote_participant_info(p: &RemoteParticipant) -> ParticipantInfo {
    ParticipantInfo {
        identity: p.identity().as_str().to_string(),
        attributes: p.attributes(),
    }
}

fn register_handler_on(lp: &LocalParticipant, method: String, handler: RpcHandler) {
    lp.register_rpc_method(method, move |data| {
        let handler = handler.clone();
        Box::pin(async move {
            let core_data = rpc_invocation_from_sdk(data);
            handler(core_data).await.map_err(rpc_error_to_sdk)
        })
    });
}

/// Conversions between the SDK's RPC types and the transport-agnostic core
/// types, applied at the room boundary. These are free functions rather
/// than `From` impls because neither side of the conversion is local to
/// this crate (the SDK vs. `crate::rpc`).
fn rpc_invocation_from_sdk(d: livekit::prelude::RpcInvocationData) -> RpcInvocationData {
    RpcInvocationData {
        request_id: d.request_id,
        caller_identity: d.caller_identity.as_str().to_string(),
        payload: d.payload,
        response_timeout: d.response_timeout,
    }
}

fn rpc_error_from_sdk(e: livekit::prelude::RpcError) -> RpcError {
    RpcError { code: e.code, message: e.message, data: e.data }
}

fn rpc_error_to_sdk(e: RpcError) -> livekit::prelude::RpcError {
    livekit::prelude::RpcError::new(e.code, e.message, e.data)
}