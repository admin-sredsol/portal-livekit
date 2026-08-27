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

//! Portal's orchestration: role setup, the publish paths (state, action,
//! chunk, video), the receive pipeline (data packets, byte streams, video
//! tracks), RPC routing, and the v0.2 multi-controller layer.
//!
//! Everything here is programmed against the [`PortalTransport`] trait, so
//! the identical logic runs against the native LiveKit SDK transport
//! (`LiveKitRustTransport`, behind the `native` feature) and, on wasm, a
//! JS-backed transport. SDK-specific concerns — room options, E2EE, the
//! event pump, RPC wire encoding — live in the transport, not here.

use std::collections::{HashMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use crate::task::Task;

use crate::config::{ChunkSpec, FieldSpec, PortalConfig};
use crate::data::{
    ACTION_CHUNK_TOPIC, ACTION_TOPIC, ActionSlot, ChunkPublisher, ChunkSlot, DataPublisher,
    STATE_TOPIC, StateSlot, dispatch_chunk_payload, handle_data_received,
};
use crate::error::{PortalError, PortalResult};
use crate::frame_video::{
    FRAME_VIDEO_TOPIC, FrameVideoPublisher, FrameVideoTrackEntry, dispatch_frame_payload,
};
use crate::metrics::{DataStream, MetricsRegistry, PortalMetrics};
use crate::rpc::{RpcError, RpcHandler, RpcInvocationData};
use crate::rtt::RttService;
use crate::serialization::{action_fingerprint, schema_fingerprint};
use crate::sync_buffer::{SyncBuffer, SyncOutput};
use crate::time::now_us;
use crate::transport::{
    PortalTransport, TransportConnect, TransportEvent, TransportRpcRequest, VideoSink,
};
use crate::types::*;

/// Participant attribute keys used by Portal for role discovery and the
/// multi-controller pointer. Namespaced to avoid colliding with any
/// application-level attributes the user may also be setting.
pub const ROLE_ATTR_KEY: &str = "lk.portal.role";
pub const ACTIVE_OPERATOR_ATTR_KEY: &str = "lk.portal.active_operator";
const ROLE_VALUE_ROBOT: &str = "robot";
const ROLE_VALUE_OPERATOR: &str = "operator";
/// RPC method registered by Robot-side Portals so any participant can request
/// a change to the active operator pointer. Payload is the new identity, or
/// the empty string to clear. Result is the empty string on success.
pub const SET_ACTIVE_OPERATOR_RPC: &str = "portal.set_active_operator";

/// App-level RPC error code (outside the transport's 1001-1999 reserved
/// range) returned when the robot has not yet finished setting up its
/// connection.
const RPC_NOT_CONNECTED: u32 = 2001;
/// App-level RPC error code returned when `set_attributes` fails while the
/// robot is processing a `set_active_operator` request.
const RPC_SET_ATTRIBUTES_FAILED: u32 = 2002;

type ObservationCb = Box<dyn Fn(&Observation) + Send + Sync>;
type DropCb = Box<dyn Fn(Vec<HashMap<String, TypedValue>>) + Send + Sync>;
type IdentityCb = Box<dyn Fn(&str) + Send + Sync>;
type OptIdentityCb = Box<dyn Fn(Option<&str>) + Send + Sync>;

/// State for the v0.2 multi-controller layer. Lives in an `Arc` so the room
/// event handler and the `set_active_operator` RPC handler can share it
/// without copying.
pub(crate) struct ControllerState {
    /// On Robot side: source of truth, mirrored as own `lk.portal.active_operator`
    /// attribute. On Operator side: a mirror of the robot's attribute, updated
    /// from `ParticipantAttributesChanged` events.
    pub(crate) active_operator: Mutex<Option<String>>,
    /// Identities of currently-connected operators (excluding self), populated
    /// from `ParticipantConnected` plus the initial remote-participants
    /// snapshot.
    pub(crate) operators: Mutex<HashSet<String>>,
    /// The robot's identity, discovered by reading the `lk.portal.role`
    /// attribute. Operators use this to address `set_active_operator` RPCs.
    pub(crate) robot_identity: Mutex<Option<String>>,

    on_operator_joined: Mutex<Option<IdentityCb>>,
    on_operator_left: Mutex<Option<IdentityCb>>,
    on_active_operator_changed: Mutex<Option<OptIdentityCb>>,
}

impl ControllerState {
    fn new() -> Self {
        Self {
            active_operator: Mutex::new(None),
            operators: Mutex::new(HashSet::new()),
            robot_identity: Mutex::new(None),
            on_operator_joined: Mutex::new(None),
            on_operator_left: Mutex::new(None),
            on_active_operator_changed: Mutex::new(None),
        }
    }

    fn fire_op_joined(&self, identity: &str) {
        if let Some(cb) = self.on_operator_joined.lock().as_ref() {
            let result = catch_unwind(AssertUnwindSafe(|| cb(identity)));
            if result.is_err() {
                log::error!("[callback-panic] on_operator_joined callback panicked");
            }
        }
    }

    fn fire_op_left(&self, identity: &str) {
        if let Some(cb) = self.on_operator_left.lock().as_ref() {
            let result = catch_unwind(AssertUnwindSafe(|| cb(identity)));
            if result.is_err() {
                log::error!("[callback-panic] on_operator_left callback panicked");
            }
        }
    }

    fn fire_active_changed(&self, identity: Option<&str>) {
        if let Some(cb) = self.on_active_operator_changed.lock().as_ref() {
            let result = catch_unwind(AssertUnwindSafe(|| cb(identity)));
            if result.is_err() {
                log::error!("[callback-panic] on_active_operator_changed callback panicked");
            }
        }
    }

    fn clear(&self) {
        *self.active_operator.lock() = None;
        self.operators.lock().clear();
        *self.robot_identity.lock() = None;
    }

    /// Partial clear used on reconnect. Drops the per-room rosters
    /// (`operators`, `robot_identity`) so post-reconnect
    /// `ParticipantConnected` events can rebuild them from scratch, but
    /// keeps `active_operator` pinned. Two reasons:
    ///
    /// * **Robot side.** The robot's `active_operator` is the source of
    ///   truth (mirrored as its own `lk.portal.active_operator` attribute).
    ///   The robot ignores attribute events on its local identity (see
    ///   the `ParticipantAttributesChanged` filter), and the SDK never fires
    ///   `ParticipantConnected` for self, so a full clear here would leave
    ///   the gate stuck at `None` until something explicitly re-set it —
    ///   silently halting control across a transient reconnect.
    /// * **Operator side.** The mirror is reseeded by the next
    ///   `ParticipantConnected` for the robot (via `classify_and_update`).
    ///   `classify_and_update` only fires `on_active_operator_changed` on
    ///   a value change, so retaining a stale value across the reconnect
    ///   does not produce a spurious callback when it gets re-read.
    fn clear_for_reconnect(&self) {
        self.operators.lock().clear();
        *self.robot_identity.lock() = None;
    }
}

/// Classify a participant by their `lk.portal.role` attribute. Returns
/// `None` if the attribute is absent or has an unknown value.
fn classify_role(attrs: &HashMap<String, String>) -> Option<Role> {
    match attrs.get(ROLE_ATTR_KEY).map(String::as_str) {
        Some(ROLE_VALUE_ROBOT) => Some(Role::Robot),
        Some(ROLE_VALUE_OPERATOR) => Some(Role::Operator),
        _ => None,
    }
}

/// Drains the buffers returned by `SyncBuffer::push_*` and dispatches them to
/// the user — callback first (by reference, no clone), then into the pull-based
/// observation buffer. Kept separate from `SyncBuffer` so callbacks run with no
/// sync-buffer lock held.
pub(crate) struct ObservationSink {
    observation_cb: Mutex<Option<ObservationCb>>,
    drop_cb: Mutex<Option<DropCb>>,
    // Latest-wins slot. Consumers peek via `get()` (clone). Consumers that
    // want history register `on_observation` and buffer on their own side.
    latest: Mutex<Option<Observation>>,
}

impl ObservationSink {
    pub(crate) fn new() -> Self {
        Self {
            observation_cb: Mutex::new(None),
            drop_cb: Mutex::new(None),
            latest: Mutex::new(None),
        }
    }

    pub(crate) fn dispatch(&self, output: SyncOutput) {
        let SyncOutput { observations, drops } = output;

        // User callbacks run on the task dispatching room events.
        // A panic here would abort the whole event loop, so we catch and
        // log and keep going.
        if !observations.is_empty() {
            {
                let cb_slot = self.observation_cb.lock();
                if let Some(cb) = cb_slot.as_ref() {
                    for obs in &observations {
                        let result = catch_unwind(AssertUnwindSafe(|| cb(obs)));
                        if result.is_err() {
                            log::error!(
                                "[callback-panic] observation callback panicked, event loop continues"
                            );
                        }
                    }
                }
            }
            // Latest-wins: only the final observation needs to reach the pull
            // slot — intermediates are discarded either way.
            if let Some(last_obs) = observations.into_iter().last() {
                *self.latest.lock() = Some(last_obs);
            }
        }

        if !drops.is_empty()
            && let Some(cb) = self.drop_cb.lock().as_ref()
        {
            let result = catch_unwind(AssertUnwindSafe(|| cb(drops)));
            if result.is_err() {
                log::error!("[callback-panic] drop callback panicked, event loop continues");
            }
        }
    }

    pub(crate) fn get(&self) -> Option<Observation> {
        self.latest.lock().clone()
    }

    pub(crate) fn clear(&self) {
        *self.latest.lock() = None;
    }

    pub(crate) fn set_observation_cb(&self, cb: ObservationCb) {
        *self.observation_cb.lock() = Some(cb);
    }

    pub(crate) fn set_drop_cb(&self, cb: DropCb) {
        *self.drop_cb.lock() = Some(cb);
    }
}

struct ConnectionState {
    event_task: Option<Task>,
    rtt: Option<Arc<RttService>>,
}

pub struct Portal {
    config: PortalConfig,

    // Serializes connect()/disconnect() so a disconnect() yielding on
    // close().await can't be overtaken by a concurrent connect()
    // whose newly-populated state would then be clobbered by the
    // disconnect's cleanup path.
    lifecycle: tokio::sync::Mutex<()>,

    // Lifecycle state (connect/disconnect).
    conn: Mutex<ConnectionState>,

    // The transport backing this session. Set as soon as the transport's
    // room is up (before role attributes and publishers are written), so a
    // concurrent `register_rpc_method` forwards immediately; cleared on
    // disconnect and failed connects.
    transport: Mutex<Option<Arc<dyn PortalTransport>>>,

    // Hot-path publishers. Each is guarded by its own mutex so send methods
    // can clone the Arc out and drop the lock before doing any IO. WebRTC
    /// media-path video publishers are owned by the transport itself (they
    /// are SDK-coupled); Portal only drives the frame-video publisher.
    /// Robot-side: one publisher per declared frame-video track. Frame-video
    /// frames travel as byte streams (per-frame RGB encode), bypassing the
    /// WebRTC media path.
    frame_video_publishers: Mutex<HashMap<String, Arc<FrameVideoPublisher>>>,
    state_publisher: Mutex<Option<Arc<DataPublisher>>>,
    action_publisher: Mutex<Option<Arc<DataPublisher>>>,
    /// Operator-side: one publisher per declared action chunk.
    chunk_publishers: Mutex<HashMap<String, Arc<ChunkPublisher>>>,

    // Operator-side sync + dispatch.
    sync_buffer: Mutex<Option<Arc<Mutex<SyncBuffer>>>>,
    obs_sink: Arc<ObservationSink>,

    // Push callback + pull latest-wins slot, bundled per stream.
    action: Arc<ActionSlot>,
    state: Arc<StateSlot>,
    /// Robot-side: one slot per declared action chunk. Fixed at construction
    /// (keyed by chunk name) so the receive path doesn't lock the map.
    chunk_slots: HashMap<String, Arc<ChunkSlot>>,
    /// Rate-limit set for unknown chunk fingerprints — the byte-stream
    /// equivalent of `DataSlot::warned_mismatches`, but lives at the
    /// dispatcher level because no slot owns "unknown" packets.
    unknown_chunk_fp_warns: Arc<Mutex<HashSet<u32>>>,
    // Fixed at construction (keyed by declared video_tracks) — no lock on the map itself.
    video_tracks: HashMap<String, Arc<VideoTrackSlots>>,
    /// Names of all video tracks (WebRTC + frame video) in declaration
    /// order. Used by `setup_operator` to size the sync buffer over the
    /// union of transports. Computed once at `Portal::new` so the connect
    /// hot path doesn't re-walk the config.
    all_track_names: Vec<String>,
    /// Per-track frame-video entries (spec + slots + metrics fused). Fixed
    /// at construction and shared as an `Arc<HashMap>` so the receive
    /// dispatch can fan out into spawn tasks via a refcount bump
    /// instead of cloning the map (which would allocate one `String`
    /// per declared track per received frame).
    frame_video_entries: Arc<HashMap<String, Arc<FrameVideoTrackEntry>>>,

    metrics: Arc<MetricsRegistry>,

    // RPC methods the caller has registered. Applied to the transport
    // on connect(); survives disconnect so reconnects reapply them.
    rpc_handlers: Arc<Mutex<HashMap<String, RpcHandler>>>,

    // Multi-controller state (v0.2). Shared with the room event handler so
    // attribute-change and participant-connect events can update operators,
    // robot_identity, and active_operator without taking a Portal-level lock.
    controller: Arc<ControllerState>,
}

impl Portal {
    pub fn new(config: PortalConfig) -> Self {
        // Slots and metrics cover both transports. Frame-video and WebRTC
        // tracks share the same VideoFrameData / VideoTrackSlots / sync
        // buffer, so the consumer-facing API is identical.
        let all_track_names = combined_track_names(&config);
        let video_tracks: HashMap<String, Arc<VideoTrackSlots>> = all_track_names
            .iter()
            .map(|name| (name.clone(), Arc::new(VideoTrackSlots::new())))
            .collect();

        let metrics = Arc::new(MetricsRegistry::new(&all_track_names));
        let obs_sink = Arc::new(ObservationSink::new());

        // Build chunk slots once at construction so the dispatch table is
        // immutable for the Portal's lifetime — `handle_room_event` reads
        // them without taking any Portal-level lock.
        let chunk_slots: HashMap<String, Arc<ChunkSlot>> = config
            .action_chunks
            .iter()
            .map(|spec| (spec.name.clone(), Arc::new(ChunkSlot::new(spec.clone()))))
            .collect();

        // Same idea for frame-video entries: the dispatch path reads them
        // per packet, so freezing the map at construction lets the hot path
        // skip a Portal-level lock and the per-connect rebuild. Each entry
        // bundles spec + slots + metrics so dispatch is a single lookup.
        // Wrapped in `Arc<HashMap>` so per-frame fan-out is a refcount bump
        // rather than a `String`-cloning map clone.
        let frame_video_entries: Arc<HashMap<String, Arc<FrameVideoTrackEntry>>> = Arc::new(
            config
                .frame_video_tracks
                .iter()
                .map(|spec| {
                    let slots = video_tracks
                        .get(&spec.name)
                        .expect("video_tracks contains every frame-video name")
                        .clone();
                    let track_metrics = metrics
                        .track(&spec.name)
                        .expect("track metrics registered for every frame-video name");
                    (
                        spec.name.clone(),
                        Arc::new(FrameVideoTrackEntry {
                            spec: spec.clone(),
                            metrics: track_metrics,
                            slots,
                        }),
                    )
                })
                .collect(),
        );

        Self {
            config,
            lifecycle: tokio::sync::Mutex::new(()),
            conn: Mutex::new(ConnectionState { event_task: None, rtt: None }),
            transport: Mutex::new(None),
            frame_video_publishers: Mutex::new(HashMap::new()),
            state_publisher: Mutex::new(None),
            action_publisher: Mutex::new(None),
            chunk_publishers: Mutex::new(HashMap::new()),
            sync_buffer: Mutex::new(None),
            obs_sink,
            action: Arc::new(ActionSlot::new()),
            state: Arc::new(StateSlot::new()),
            chunk_slots,
            unknown_chunk_fp_warns: Arc::new(Mutex::new(HashSet::new())),
            video_tracks,
            all_track_names,
            frame_video_entries,
            metrics,
            rpc_handlers: Arc::new(Mutex::new(HashMap::new())),
            controller: Arc::new(ControllerState::new()),
        }
    }

    /// Connect using the native LiveKit SDK transport. This is the standard
    /// entry point on native platforms; it exists behind the crate's
    /// `native` cargo feature and is a thin wrapper that constructs
    /// [`crate::native::LiveKitRustTransport`] and delegates to
    /// [`Portal::connect_with_transport`].
    #[cfg(feature = "native")]
    pub async fn connect(&self, url: &str, token: &str) -> PortalResult<()> {
        let transport =
            Arc::new(crate::native::LiveKitRustTransport::new(self.config.clone(), self.metrics.clone()));
        self.connect_with_transport(transport, url, token).await
    }

    /// Connect using a caller-provided transport. Room setup that the SDK
    /// would do (signal/WebRTC connect, E2EE, WebRTC-path video track
    /// publishing) happens inside `transport.connect`; everything Portal
    /// owns — role attributes, the multi-controller layer, data and
    /// byte-stream publishers, RPC routing, the event pipeline — runs here,
    /// transport-agnostic.
    ///
    /// The transport must be reusable: `disconnect()` releases its
    /// connection so a later `connect_with_transport` on the same instance
    /// starts fresh.
    pub async fn connect_with_transport(
        &self,
        transport: Arc<dyn PortalTransport>,
        url: &str,
        token: &str,
    ) -> PortalResult<()> {
        let _lifecycle = self.lifecycle.lock().await;
        if self.conn.lock().event_task.is_some() {
            return Err(PortalError::AlreadyConnected);
        }

        log::info!("[{}] connecting as {:?} to {}", self.config.session, self.config.role, url);

        // Robot-side: register the built-in `set_active_operator` RPC. The
        // handler clones the transport and the controller Arc so the closure
        // can update both the attribute and the local mirror without holding
        // any Portal-level lock. Registration lands in the handler map and
        // is applied to the room below, alongside caller-registered methods.
        if self.config.role == Role::Robot {
            let transport_for_rpc = transport.clone();
            let controller = self.controller.clone();
            let handler: RpcHandler = Arc::new(move |data: RpcInvocationData| {
                let transport_for_rpc = transport_for_rpc.clone();
                let controller = controller.clone();
                Box::pin(async move {
                    set_active_operator_rpc_impl(&*transport_for_rpc, &controller, data).await
                })
            });
            self.register_rpc_method(SET_ACTIVE_OPERATOR_RPC, handler);
        }

        // Connect the room. The transport receives its event sink and the
        // byte-stream topics this Portal consumes up front; everything it
        // translates lands on the channel and is drained by the event loop
        // spawned below.
        let (events_tx, mut events_rx) = tokio::sync::mpsc::unbounded_channel();
        let connect_params = TransportConnect {
            url,
            token,
            events: events_tx,
            byte_stream_topics: self.byte_stream_topics(),
        };
        transport.connect(connect_params).await?;

        // Store the transport before applying handlers so a concurrent
        // `register_rpc_method` either (a) inserts before we iterate and gets
        // picked up, or (b) inserts after we've stored it and forwards the
        // handler itself. Overlap is idempotent — the transport-side handler
        // map is last-writer-wins.
        *self.transport.lock() = Some(transport.clone());
        self.apply_rpc_handlers();

        // Self-set the role attribute so other participants can discover us.
        // Token-mint may also have set this key; in that case `set_attributes`
        // is effectively a no-op for the same value.
        let role_value = match self.config.role {
            Role::Robot => ROLE_VALUE_ROBOT,
            Role::Operator => ROLE_VALUE_OPERATOR,
        };
        let mut role_attrs = HashMap::new();
        role_attrs.insert(ROLE_ATTR_KEY.to_string(), role_value.to_string());
        if let Err(e) = transport.set_attributes(role_attrs).await {
            // Most common cause: the token grant did not include
            // `canUpdateOwnMetadata`. Surface a clear error so callers fix
            // their token-mint script rather than silently leaving the
            // participant unidentified. Roll back the partial state we
            // already wrote (transport slot, RPC handler bindings) so a
            // retry starts from a clean slate.
            self.rollback_partial_connect();
            let _ = transport.disconnect().await;
            return Err(PortalError::Room(format!(
                "failed to publish role attribute (token may be missing canUpdateOwnMetadata): {e}"
            )));
        }

        // Robot-side: if the token seeded `lk.portal.active_operator`,
        // mirror it locally so the action gate sees the configured pointer
        // before anyone calls `set_active_operator`.
        if self.config.role == Role::Robot {
            let attrs = transport.local_attributes();
            if let Some(seed) = attrs.get(ACTIVE_OPERATOR_ATTR_KEY) {
                let value = if seed.is_empty() { None } else { Some(seed.clone()) };
                *self.controller.active_operator.lock() = value;
            }
        }

        // Walk the room snapshot once at connect so any participant that
        // joined before us is already in `operators` / `robot_identity`. New
        // joiners get added by the `ParticipantConnected` event handler.
        for participant in transport.remote_participants() {
            classify_and_update(
                &self.controller,
                self.config.role,
                &participant.identity,
                &participant.attributes,
            );
        }

        let setup_result = match self.config.role {
            Role::Robot => self.setup_robot(&transport),
            Role::Operator => {
                self.setup_operator(&transport);
                Ok(())
            }
        };
        if let Err(e) = setup_result {
            // Setup can fail mid-way through building publishers. Their own
            // constructors already clean up their partial maps on drop; we
            // still need to undo the transport slot, controller mirror, and
            // any other state written above before bailing.
            self.rollback_partial_connect();
            let _ = transport.disconnect().await;
            return Err(e);
        }

        let rtt = Arc::new(RttService::spawn(
            transport.clone(),
            self.config.ping_ms,
            self.metrics.clone(),
        ));

        log::info!("[{}] connected as {:?}", self.config.session, self.config.role);

        // Event dispatch runs off a snapshot of the fields it touches, not the
        // whole Portal, so it doesn't need any outer lock.
        let action_schema_fp = action_fingerprint(&self.config.action_schema);
        let state_schema_fp = schema_fingerprint(&self.config.state_schema);
        // The dispatch path needs a slice for fingerprint lookup; the map
        // form is for `get_action_chunk` / `on_action_chunk` name lookups.
        // Build the slice once per connect so the event loop iterates a
        // plain Vec, not a HashMap.
        let chunk_slots_for_dispatch: Vec<Arc<ChunkSlot>> =
            self.chunk_slots.values().cloned().collect();
        let local_identity = transport
            .local_identity()
            .expect("transport reports a local identity after a successful connect");
        let ctx = EventContext {
            config: self.config.clone(),
            action_schema_fp,
            state_schema_fp,
            sync_buffer: self.sync_buffer.lock().clone(),
            obs_sink: self.obs_sink.clone(),
            action: self.action.clone(),
            state: self.state.clone(),
            chunk_slots: chunk_slots_for_dispatch,
            unknown_chunk_fp_warns: self.unknown_chunk_fp_warns.clone(),
            video_tracks: self.video_tracks.clone(),
            frame_video_entries: self.frame_video_entries.clone(),
            metrics: self.metrics.clone(),
            rtt: rtt.clone(),
            controller: self.controller.clone(),
            local_identity,
            transport: transport.clone(),
        };
        let event_handle = crate::task::spawn(async move {
            while let Some(event) = events_rx.recv().await {
                handle_room_event(&ctx, event);
            }
        });

        let mut state = self.conn.lock();
        state.event_task = Some(event_handle);
        state.rtt = Some(rtt);
        Ok(())
    }

    pub fn send_video_frame(
        &self,
        track_name: &str,
        rgb_data: &[u8],
        width: u32,
        height: u32,
        timestamp_us: Option<u64>,
    ) -> PortalResult<()> {
        // Two transports, one user-facing method. WebRTC-path publishers
        // live in the native transport (populated at connect from the
        // config's `video_tracks`); frame-video publishers are built here in
        // `setup_robot`. Names are unique across both, so a track lives in
        // exactly one place.
        let transport = self.transport.lock().clone();
        if let Some(transport) = &transport {
            if self.config.role == Role::Robot
                && self.config.video_tracks.iter().any(|s| s.name == track_name)
            {
                return transport.publish_video_frame(
                    track_name,
                    rgb_data,
                    width,
                    height,
                    timestamp_us,
                );
            }
            if let Some(publisher) = self.frame_video_publishers.lock().get(track_name).cloned() {
                return publisher.send_frame(rgb_data, width, height, timestamp_us);
            }
        }
        // Distinguish wrong-role (track is declared but no publisher exists
        // because send is operator-side) from genuinely unknown-track. The
        // operator never spawns video publishers, so a declared name with
        // no publisher means "wrong role" — same shape as `send_state` /
        // `send_action_chunk`.
        if self.config.role != Role::Robot
            && (self.config.video_tracks.iter().any(|s| s.name == track_name)
                || self.config.frame_video_tracks.iter().any(|s| s.name == track_name))
        {
            return Err(PortalError::WrongRole(self.config.role));
        }
        Err(PortalError::UnknownVideoTrack { name: track_name.to_string() })
    }

    /// Publish a state sample (robot only). Values are typed — build the
    /// map with `TypedValue::Bool(true)`, `0.5f32.into()`, etc. The
    /// pipeline internally widens to `f64` for carry-forward and casts
    /// back to the declared dtype at the wire boundary.
    pub fn send_state(
        &self,
        values: &HashMap<String, TypedValue>,
        timestamp_us: Option<u64>,
    ) -> PortalResult<()> {
        let publisher =
            self.state_publisher.lock().clone().ok_or(PortalError::WrongRole(Role::Operator))?;
        // State has no echo path; drop the wire-values vector that
        // `send_map` returns for action callers.
        publisher.send_map(values, timestamp_us, None).map(|_| ())
    }

    /// Publish an action (operator only).
    ///
    /// `in_reply_to_ts_us` is the timestamp of the observation this action
    /// was produced from — pass `Some(obs.timestamp_us)` to give the
    /// receiver the data it needs to compute true end-to-end policy
    /// latency (`metrics.policy.e2e_us_*`). Pass `None` for unsolicited
    /// publishes (teleop, idle commands).
    pub fn send_action(
        &self,
        values: &HashMap<String, TypedValue>,
        timestamp_us: Option<u64>,
        in_reply_to_ts_us: Option<u64>,
    ) -> PortalResult<()> {
        let publisher =
            self.action_publisher.lock().clone().ok_or(PortalError::WrongRole(Role::Robot))?;
        // Resolve the actual send timestamp the publisher will stamp onto
        // the wire so the local echo (if any) sees the same value the
        // robot sees. `send_map` would default `None` to `now_us()` and
        // we'd pick a slightly later timestamp here.
        let send_ts = timestamp_us.unwrap_or_else(now_us);
        let wire_values = publisher.send_map(values, Some(send_ts), in_reply_to_ts_us)?;
        // Echo path. The room does not fan out a publisher's own data
        // packets, so without this an active operator would never see its
        // own action through `on_action`. We only echo when subscription
        // is on AND we are the active operator: otherwise this would just
        // be local noise that nobody else in the room sees either.
        //
        // `wire_values` is what the receiver will reconstruct after decode:
        // post-carry-forward (so omitted fields keep their last-sent value
        // rather than reading as 0.0) and post-saturation (so out-of-range
        // inputs match the clipped wire bytes). Building the echo from the
        // caller's input map directly would silently diverge whenever a
        // partial update or saturating value is involved.
        if self.config.action_subscription && self.is_self_active() {
            // We only echo when self is the active operator, which means
            // we are connected and have a local identity. Unwrap is safe.
            let local_id =
                self.local_identity().expect("local_identity is Some when self == active_operator");
            let action = crate::data::build_action(
                send_ts,
                in_reply_to_ts_us,
                &self.config.action_schema,
                &wire_values,
                local_id,
            );
            self.action.deliver(action);
        }
        Ok(())
    }

    /// Publish an action chunk on the named chunk schema (operator only).
    ///
    /// `data` is `field -> column of length horizon`. Columns shorter than
    /// `horizon` are zero-padded, longer columns are truncated, both with a
    /// warn-once, and unknown keys are warned-and-ignored once each. Use
    /// `in_reply_to_ts_us` the same way as `send_action` to feed
    /// `metrics.policy.e2e_us_*`.
    ///
    /// A column built with `ChunkColumn::typed` is checked against the
    /// declared field dtype and returns `PortalError::DtypeMismatch` on
    /// disagreement, the same rejection `send_action` gives a `TypedValue`
    /// of the wrong variant. `ChunkColumn::untyped` (and the `From<Vec<f64>>`
    /// conversion) waives the check and coerces.
    pub fn send_action_chunk(
        &self,
        chunk_name: &str,
        data: &HashMap<String, ChunkColumn>,
        timestamp_us: Option<u64>,
        in_reply_to_ts_us: Option<u64>,
    ) -> PortalResult<()> {
        let publisher = {
            let map = self.chunk_publishers.lock();
            map.get(chunk_name).cloned()
        };
        let Some(publisher) = publisher else {
            // No publisher resolves to one of three precise errors so the
            // caller sees the actual mistake instead of a generic refusal:
            // wrong role, undeclared chunk name, or operator-but-not-yet
            // connected (publishers are spawned in `setup_operator`).
            return if self.config.role != Role::Operator {
                Err(PortalError::WrongRole(Role::Robot))
            } else if !self.chunk_slots.contains_key(chunk_name) {
                Err(PortalError::UnknownChunk { name: chunk_name.to_string() })
            } else {
                Err(PortalError::NotConnected)
            };
        };
        let send_ts = timestamp_us.unwrap_or_else(now_us);
        publisher.send(data, Some(send_ts), in_reply_to_ts_us)?;
        // Echo path: same conditions as `send_action`. Unlike scalar
        // actions where we rebuild the typed values, chunks already carry
        // raw `f64` columns — we hand the same `data` map straight to the
        // slot, padded/truncated to the declared horizon to match what
        // the wire path emits.
        if self.config.action_subscription
            && self.is_self_active()
            && let Some(slot) = self.chunk_slots.get(chunk_name)
        {
            let local_id =
                self.local_identity().expect("local_identity is Some when self == active_operator");
            let horizon = slot.spec.horizon as usize;
            let normalized: HashMap<String, Vec<f64>> = slot
                .spec
                .fields
                .iter()
                .map(|f| {
                    let mut col = data.get(&f.name).map(|c| c.values.clone()).unwrap_or_default();
                    if col.len() < horizon {
                        col.resize(horizon, 0.0);
                    } else if col.len() > horizon {
                        col.truncate(horizon);
                    }
                    (f.name.clone(), col)
                })
                .collect();
            slot.deliver(ActionChunk {
                name: slot.spec.name.clone(),
                horizon: slot.spec.horizon,
                data: normalized,
                timestamp_us: send_ts,
                in_reply_to_ts_us,
                sender: local_id,
            });
        }
        Ok(())
    }

    // --- RPC ---

    /// Declared state schema (field names + dtypes), in declaration order.
    /// Bindings mirror this snapshot internally; reading from the Portal
    /// keeps the snapshot single-sourced.
    pub fn state_schema(&self) -> &[FieldSpec] {
        self.config.state_schema()
    }

    /// Declared action schema, same semantics as `state_schema`.
    pub fn action_schema(&self) -> &[FieldSpec] {
        self.config.action_schema()
    }

    // --- Multi-controller surface (v0.2) ---

    /// This Portal's own identity once connected. Reads from the transport.
    /// `None` before `connect()` succeeds.
    pub fn local_identity(&self) -> Option<String> {
        let transport = self.transport.lock().clone()?;
        transport.local_identity()
    }

    /// Identity of the operator the robot is currently listening to, or
    /// `None` if no operator is selected. On Robot side this is the local
    /// pointer (also broadcast as the `lk.portal.active_operator` attribute).
    /// On Operator side it is a mirror of the robot's attribute.
    pub fn active_operator(&self) -> Option<String> {
        self.controller.active_operator.lock().clone()
    }

    /// `true` iff this Portal's local identity is the current
    /// `active_operator`. Used internally by the echo path; exposed so
    /// callers can decide whether to record their own outgoing actions.
    fn is_self_active(&self) -> bool {
        let local = self.local_identity();
        let active = self.controller.active_operator.lock().clone();
        match (local, active) {
            (Some(local), Some(active)) => local == active,
            _ => false,
        }
    }

    /// Set the active operator. On Robot side this updates the local pointer
    /// and broadcasts via the robot's own attributes. On Operator side this
    /// dispatches a `portal.set_active_operator` RPC to the robot.
    ///
    /// Pass `None` to clear and drop all incoming actions.
    pub async fn set_active_operator(&self, identity: Option<String>) -> PortalResult<()> {
        match self.config.role {
            Role::Robot => {
                let transport =
                    self.transport.lock().clone().ok_or(PortalError::NotConnected)?;
                let prev = self.controller.active_operator.lock().clone();
                let mut attrs = HashMap::new();
                attrs.insert(
                    ACTIVE_OPERATOR_ATTR_KEY.to_string(),
                    identity.clone().unwrap_or_default(),
                );
                transport.set_attributes(attrs).await?;
                *self.controller.active_operator.lock() = identity.clone();
                if prev != identity {
                    self.controller.fire_active_changed(identity.as_deref());
                }
                Ok(())
            }
            Role::Operator => {
                // Cached robot identity (populated by attribute events) is
                // the fast path. The common pattern is:
                //
                //   await op.connect(...)
                //   await op.set_active_operator(op.local_identity())
                //
                // immediately after connect, before the room has surfaced the
                // robot's attributes via `ParticipantAttributesChanged`. To
                // make that work without forcing every caller to manually
                // wait, we scan the room snapshot and, if still empty,
                // poll briefly. Bounded at ~1.5 s — long enough for the
                // initial attribute event on a healthy LAN, short enough to
                // surface NoPeer quickly when there really is no robot.
                let robot = self.resolve_robot_identity().await?;
                let payload = identity.unwrap_or_default();
                self.perform_rpc(Some(&robot), SET_ACTIVE_OPERATOR_RPC, payload, None).await?;
                Ok(())
            }
        }
    }

    /// Currently-connected operator identities (excluding self).
    pub fn operators(&self) -> Vec<String> {
        let mut v: Vec<String> = self.controller.operators.lock().iter().cloned().collect();
        v.sort();
        v
    }

    /// Identity of the robot in the room, or `None` if none has been seen.
    /// Operator-side helper, derived from the robot's `lk.portal.role`
    /// attribute.
    pub fn robot_identity(&self) -> Option<String> {
        self.controller.robot_identity.lock().clone()
    }

    /// Fire when an operator joins the room. Identity is the new operator's
    /// participant identity. Only one callback is stored; subsequent calls
    /// overwrite.
    pub fn on_operator_joined(&self, callback: impl Fn(&str) + Send + Sync + 'static) {
        *self.controller.on_operator_joined.lock() = Some(Box::new(callback));
    }

    /// Fire when an operator leaves the room. The robot's `active_operator`
    /// attribute is **not** auto-cleared on disconnect; the pointer stays
    /// pinned so a reconnect with the same identity resumes control.
    pub fn on_operator_left(&self, callback: impl Fn(&str) + Send + Sync + 'static) {
        *self.controller.on_operator_left.lock() = Some(Box::new(callback));
    }

    /// Fire when the robot's `active_operator` attribute changes (or, on the
    /// Robot side, when the local pointer is updated via `set_active_operator`
    /// or the RPC handler). The argument is the new identity, or `None` if
    /// the pointer was cleared.
    pub fn on_active_operator_changed(
        &self,
        callback: impl Fn(Option<&str>) + Send + Sync + 'static,
    ) {
        *self.controller.on_active_operator_changed.lock() = Some(Box::new(callback));
    }

    /// Register an RPC method handler. Handlers can be registered before or
    /// after `connect()`; stored handlers are (re)applied to the transport
    /// on each connect.
    pub fn register_rpc_method(&self, method: &str, handler: RpcHandler) {
        {
            let mut map = self.rpc_handlers.lock();
            map.insert(method.to_string(), handler.clone());
        }
        if let Some(transport) = self.transport.lock().clone() {
            transport.register_rpc_method(method.to_string(), handler);
        }
    }

    /// Remove a previously registered RPC method handler.
    pub fn unregister_rpc_method(&self, method: &str) {
        self.rpc_handlers.lock().remove(method);
        if let Some(transport) = self.transport.lock().clone() {
            transport.unregister_rpc_method(method);
        }
    }

    /// Invoke a registered method on the peer. `destination` is optional;
    /// when omitted, the call is routed to the obvious counterpart — robot
    /// for an Operator, the active operator for a Robot — falling back to
    /// the single remote participant if neither pointer is set yet. Errors
    /// with `NoPeer` or `AmbiguousPeer` when no unique destination
    /// resolves.
    pub async fn perform_rpc(
        &self,
        destination: Option<&str>,
        method: &str,
        payload: String,
        response_timeout: Option<Duration>,
    ) -> PortalResult<String> {
        let destination = match destination {
            Some(id) => id.to_string(),
            None => self.resolve_peer()?,
        };
        let transport = self.transport.lock().clone().ok_or(PortalError::NotConnected)?;
        transport
            .perform_rpc(TransportRpcRequest {
                destination,
                method: method.to_string(),
                payload,
                response_timeout,
            })
            .await
            .map_err(PortalError::Rpc)
    }

    /// Byte-stream topics this Portal consumes, computed from the role and
    /// config. Passed to the transport at connect so streams opened on any
    /// other topic are dropped without being read — the transport-level
    /// equivalent of the take-or-drop decision the event handler used to
    /// make inline, and it keeps the receive hot path free for peers
    /// sharing the room.
    fn byte_stream_topics(&self) -> HashSet<String> {
        let mut topics = HashSet::new();
        match self.config.role {
            Role::Robot => {
                // Robot always consumes action chunks.
                topics.insert(ACTION_CHUNK_TOPIC.to_string());
            }
            Role::Operator => {
                // Operators consume chunks only with subscription on (HITL
                // recording, shadow eval); frame video whenever any
                // frame-video track is declared.
                if self.config.action_subscription {
                    topics.insert(ACTION_CHUNK_TOPIC.to_string());
                }
                if !self.config.frame_video_tracks.is_empty() {
                    topics.insert(FRAME_VIDEO_TOPIC.to_string());
                }
            }
        }
        topics
    }

    /// Walk the transport's participant snapshot looking for one whose
    /// attributes declare `role=robot`. Synchronous one-shot lookup.
    fn find_robot_in_room(&self) -> Option<String> {
        let transport = self.transport.lock().clone()?;
        for participant in transport.remote_participants() {
            if classify_role(&participant.attributes) == Some(Role::Robot) {
                // Cache for subsequent calls so the slow path runs at most
                // once per session.
                *self.controller.robot_identity.lock() = Some(participant.identity.clone());
                return Some(participant.identity);
            }
        }
        None
    }

    /// Resolve the robot's identity for an operator-side RPC. Tries the
    /// cached value first (populated by attribute events), then a synchronous
    /// scan of the room snapshot, then a short polling loop so
    /// `set_active_operator` works immediately after `connect()` without
    /// racing the initial attribute-propagation event. Returns `NoPeer`
    /// after the timeout if no participant with `role=robot` ever appears.
    async fn resolve_robot_identity(&self) -> PortalResult<String> {
        if let Some(id) = self.controller.robot_identity.lock().clone() {
            return Ok(id);
        }
        if let Some(id) = self.find_robot_in_room() {
            return Ok(id);
        }
        // Not connected — there is no room to find a robot in.
        let Some(transport) = self.transport.lock().clone() else {
            return Err(PortalError::NoPeer);
        };
        // Poll for ~1.5s in 50ms ticks. On a healthy LAN the first
        // ParticipantAttributesChanged event lands well within this window.
        for _ in 0..30 {
            transport.sleep(Duration::from_millis(50)).await;
            if let Some(id) = self.controller.robot_identity.lock().clone() {
                return Ok(id);
            }
            if let Some(id) = self.find_robot_in_room() {
                return Ok(id);
            }
        }
        Err(PortalError::NoPeer)
    }

    /// Resolve a default destination for `perform_rpc(None, ...)`. Reads
    /// the multi-controller mirrors first (operator → robot, robot →
    /// active operator), then falls back to a single-remote-participant
    /// snapshot for setups that haven't designated control yet.
    fn resolve_peer(&self) -> PortalResult<String> {
        match self.config.role {
            Role::Operator => {
                if let Some(id) = self.controller.robot_identity.lock().clone() {
                    return Ok(id);
                }
            }
            Role::Robot => {
                if let Some(id) = self.controller.active_operator.lock().clone() {
                    return Ok(id);
                }
            }
        }
        let transport = self.transport.lock().clone().ok_or(PortalError::NotConnected)?;
        let remotes = transport.remote_participants();
        match remotes.len() {
            0 => Err(PortalError::NoPeer),
            1 => Ok(remotes.into_iter().next().expect("remotes has one entry").identity),
            _ => Err(PortalError::AmbiguousPeer),
        }
    }

    /// Apply every stored handler to the freshly-connected transport.
    /// Called once from `connect_with_transport` after the room is up.
    fn apply_rpc_handlers(&self) {
        let Some(transport) = self.transport.lock().clone() else {
            return;
        };
        let handlers = self.rpc_handlers.lock().clone();
        for (method, handler) in handlers {
            transport.register_rpc_method(method, handler);
        }
    }

    /// Reset Portal-side state written during a `connect()` that failed
    /// before reaching the final commit (where `conn.event_task` would be
    /// stored). Mirrors the cleanup `disconnect()` does, except it
    /// (a) doesn't take the lifecycle lock — `connect()` already holds it —
    /// and (b) leaves the transport for the caller to close, since the
    /// failing connect path owns it. Without this, a failed connect would
    /// leave a stale transport slot, RPC handler bindings on a dead room,
    /// and partial publisher maps that the next `connect()` (or any
    /// pre-connect getter) would still see.
    fn rollback_partial_connect(&self) {
        *self.transport.lock() = None;
        self.controller.clear();
        self.frame_video_publishers.lock().clear();
        *self.state_publisher.lock() = None;
        *self.action_publisher.lock() = None;
        self.chunk_publishers.lock().clear();
        if let Some(sb) = self.sync_buffer.lock().take() {
            sb.lock().clear();
        }
        self.obs_sink.clear();
        self.action.clear();
        self.state.clear();
        for slot in self.chunk_slots.values() {
            slot.clear();
        }
        for slots in self.video_tracks.values() {
            slots.clear();
        }
    }

    pub async fn disconnect(&self) -> PortalResult<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let transport = self.transport.lock().take();
        log::info!("disconnecting");

        // disconnect() is best-effort; cleanup must happen even if it
        // errors, otherwise the Portal would be half-disconnected (no
        // transport but tasks/publishers still running) and the next
        // connect() would race.
        let disconnect_result = match transport {
            Some(transport) => transport.disconnect().await,
            None => Ok(()),
        };

        {
            let mut state = self.conn.lock();
            if let Some(task) = state.event_task.take() {
                task.abort();
            }
            state.rtt = None;
        }
        // Multi-controller state (operators, robot_identity, active_operator
        // mirror) is per-connection and cleared so a subsequent connect()
        // starts from a clean slate.
        self.controller.clear();

        self.frame_video_publishers.lock().clear();
        *self.state_publisher.lock() = None;
        *self.action_publisher.lock() = None;
        self.chunk_publishers.lock().clear();

        if let Some(sb) = self.sync_buffer.lock().take() {
            sb.lock().clear();
        }
        self.obs_sink.clear();
        self.action.clear();
        self.state.clear();
        for slot in self.chunk_slots.values() {
            slot.clear();
        }
        for slots in self.video_tracks.values() {
            slots.clear();
        }

        disconnect_result
    }

    // --- Pull API (latest-wins, peek semantics) ---

    /// Clone of the latest observation, or `None` if none received yet.
    /// Consumers wanting a history of observations should register
    /// `on_observation` and buffer on their own side.
    pub fn get_observation(&self) -> Option<Observation> {
        self.obs_sink.get()
    }

    /// Clone of the latest action received (Robot side), or `None`.
    /// `.values` holds typed values per the declared schema; `.raw_values`
    /// is the lossless `f64` view.
    pub fn get_action(&self) -> Option<Action> {
        self.action.get()
    }

    /// Clone of the latest state received (Operator side), or `None`.
    /// Typed per the declared schema.
    pub fn get_state(&self) -> Option<State> {
        self.state.get()
    }

    /// Clone of the latest frame received for `track_name`, or `None`.
    pub fn get_video_frame(&self, track_name: &str) -> Option<VideoFrameData> {
        self.video_tracks.get(track_name).and_then(|s| s.latest.lock().clone())
    }

    /// Clone of the latest chunk received for `chunk_name`, or `None` if
    /// none received yet (or the chunk wasn't declared).
    pub fn get_action_chunk(&self, chunk_name: &str) -> Option<ActionChunk> {
        self.chunk_slots.get(chunk_name).and_then(|s| s.get())
    }

    /// All declared action chunk schemas, in declaration order.
    pub fn action_chunks(&self) -> &[ChunkSpec] {
        self.config.action_chunks()
    }

    // --- Callback registration (push API) ---

    /// Fire on every received action. The `Action` record exposes typed
    /// values per the declared schema plus `raw_values` for the lossless
    /// `f64` view.
    pub fn on_action(&self, callback: impl Fn(&Action) + Send + Sync + 'static) {
        *self.action.cb.lock() = Some(Box::new(callback));
    }

    /// Fire on every received chunk for the named declaration. Only one
    /// callback per chunk; calling twice overwrites. Unknown names are
    /// logged and ignored — they aren't a hard error because the chunk
    /// schema may have been intentionally omitted on this peer.
    pub fn on_action_chunk(
        &self,
        chunk_name: &str,
        callback: impl Fn(&ActionChunk) + Send + Sync + 'static,
    ) {
        match self.chunk_slots.get(chunk_name) {
            Some(slot) => slot.set_callback(Box::new(callback)),
            None => log::warn!(
                "[unknown-chunk] on_action_chunk: chunk '{chunk_name}' not declared, callback ignored"
            ),
        }
    }

    pub fn on_observation(&self, callback: impl Fn(&Observation) + Send + Sync + 'static) {
        self.obs_sink.set_observation_cb(Box::new(callback));
    }

    /// Fire on every received state. Semantics mirror `on_action`.
    pub fn on_state(&self, callback: impl Fn(&State) + Send + Sync + 'static) {
        *self.state.cb.lock() = Some(Box::new(callback));
    }

    pub fn on_video_frame(
        &self,
        track_name: &str,
        callback: impl Fn(&str, &VideoFrameData) + Send + Sync + 'static,
    ) {
        match self.video_tracks.get(track_name) {
            Some(slots) => *slots.cb.lock() = Some(Box::new(callback)),
            None => log::warn!(
                "[unknown-track] on_video_frame: track '{track_name}' not registered, callback ignored"
            ),
        }
    }

    /// Operator-side, browser video path: push one decoded RGB frame into the
    /// same slots / sync-buffer / observation pipeline the native receivers
    /// feed.
    ///
    /// Native transports own the whole receive pipeline — libwebrtc decodes,
    /// yuv converts, a receiver task pushes. In the browser there is no
    /// libwebrtc; the JS side subscribes with livekit-js, decodes frames
    /// itself (canvas / WebCodecs), and hands the RGB here. Portal routes it
    /// exactly like a native receiver would: per-frame callback, latest-wins
    /// slot for `get_video_frame`, sync-buffer push (which fires
    /// `on_observation` when state pairs with the frame), and track metrics.
    ///
    /// Frame-video byte-stream arrivals do not go through this method — the
    /// JS transport forwards finished streams as `TransportEvent::ByteStream`
    /// and `dispatch_frame_payload` decodes them in core. This entry point is
    /// for WebRTC media-path tracks, declared under either `video_tracks` or
    /// `frame_video_tracks` (a robot may publish a declared frame-video track
    /// as real media; the codec in its spec is irrelevant to this path).
    ///
    /// `rgb` is packed RGB24 (`width * height * 3` bytes). Requires a
    /// connected operator; the sync buffer does not exist before connect.
    pub fn ingest_video_frame(
        &self,
        track_name: &str,
        rgb: Vec<u8>,
        width: u32,
        height: u32,
        timestamp_us: u64,
    ) -> PortalResult<()> {
        if self.config.role != Role::Operator {
            return Err(PortalError::WrongRole(self.config.role));
        }

        // Buffer-length check with overflow-safe math: a u32 product can
        // overflow a 32-bit `usize` on wasm32 (checked math saturates the
        // pathological case into `InvalidFrameDimensions`).
        let expected = (width as usize)
            .checked_mul(height as usize)
            .and_then(|px| px.checked_mul(3));
        let expected = match expected {
            Some(n) => n,
            None => return Err(PortalError::InvalidFrameDimensions { width, height }),
        };
        if rgb.len() != expected {
            return Err(PortalError::WrongFrameSize { expected, got: rgb.len() });
        }

        // `video_tracks` holds slots for every declared track name — WebRTC
        // and frame-video alike (see `Portal::new`) — so one lookup resolves
        // either kind, and metrics are registered for the same name set.
        let slots = self
            .video_tracks
            .get(track_name)
            .ok_or_else(|| PortalError::UnknownVideoTrack { name: track_name.to_string() })?
            .clone();
        let metrics =
            self.metrics.track(track_name).expect("track metrics registered at construction");

        let sync_buffer =
            self.sync_buffer.lock().clone().ok_or(PortalError::NotConnected)?;

        let frame = Arc::new(VideoFrameData {
            width,
            height,
            data: Bytes::from(rgb),
            timestamp_us,
        });
        metrics.record_received_bytes(timestamp_us, now_us(), frame.data.len());

        // Everything below mirrors the native receive paths' tail (video.rs
        // processing task / `dispatch_frame_payload`): panic-isolated
        // callback, latest-wins slot, sync-buffer push + observation
        // dispatch. Synchronous — the JS side calls this once per decoded
        // frame, so no queue or drainer task is needed.
        if let Some(cb) = slots.cb.lock().as_ref() {
            let result = catch_unwind(AssertUnwindSafe(|| cb(track_name, &frame)));
            if result.is_err() {
                log::error!(
                    "[callback-panic] video frame callback panicked on track '{track_name}', \
                     receive loop continues"
                );
            }
        }
        *slots.latest.lock() = Some((*frame).clone());

        let output = sync_buffer.lock().push_frame(track_name, frame);
        if !output.is_empty() {
            self.obs_sink.dispatch(output);
        }
        Ok(())
    }

    /// Fire on every batch of state samples that couldn't be matched to a
    /// video frame. Each entry is the typed state payload (same shape as
    /// `Observation.state`).
    pub fn on_drop(
        &self,
        callback: impl Fn(Vec<HashMap<String, TypedValue>>) + Send + Sync + 'static,
    ) {
        self.obs_sink.set_drop_cb(Box::new(callback));
    }

    // --- Internal ---

    /// Wire up the Robot-side publishers that don't depend on the WebRTC
    /// media path. WebRTC-path video tracks are published by the transport
    /// itself during `connect`; frame-video tracks and state data travel
    /// through transport-agnostic publishers built here.
    fn setup_robot(&self, transport: &Arc<dyn PortalTransport>) -> PortalResult<()> {
        // Frame-video publishers emit one byte stream per frame. No async
        // setup — just build the publisher (which spawns its drainer task).
        for spec in &self.config.frame_video_tracks {
            let track_metrics =
                self.metrics.track(&spec.name).expect("track metrics registered at construction");
            let publisher = FrameVideoPublisher::new(spec.clone(), transport.clone(), track_metrics);
            log::info!(
                "[{}] ready to publish frame-video track '{}' via byte stream (codec={:?}, quality={})",
                self.config.session,
                spec.name,
                spec.codec,
                spec.quality
            );
            self.frame_video_publishers.lock().insert(spec.name.clone(), Arc::new(publisher));
        }

        if !self.config.state_schema.is_empty() {
            let publisher = DataPublisher::new(
                &self.config.state_schema,
                STATE_TOPIC,
                self.config.state_reliable,
                transport.clone(),
                self.metrics.clone(),
                DataStream::State,
            );
            let mode = if self.config.state_reliable { "reliable" } else { "unreliable" };
            log::info!(
                "[{}] ready to publish state via {mode} data ({} fields)",
                self.config.session,
                self.config.state_schema.len()
            );
            *self.state_publisher.lock() = Some(Arc::new(publisher));
        }

        Ok(())
    }

    fn setup_operator(&self, transport: &Arc<dyn PortalTransport>) {
        // Sync buffer treats both transports the same way — it tracks frame
        // arrivals by name, regardless of whether they came from a WebRTC
        // RTP track or a frame-video byte stream. `all_track_names` was
        // computed once at construction.
        let sync_buffer = Arc::new(Mutex::new(SyncBuffer::new(
            &self.all_track_names,
            self.config.state_schema.clone(),
            self.config.sync_config(),
            self.metrics.clone(),
        )));
        *self.sync_buffer.lock() = Some(sync_buffer);

        if !self.config.action_schema.is_empty() {
            let mode = if self.config.action_reliable { "reliable" } else { "unreliable" };
            log::info!(
                "[{}] ready to publish action via {mode} data ({} fields)",
                self.config.session,
                self.config.action_schema.len()
            );
            let publisher = DataPublisher::new(
                &self.config.action_schema,
                ACTION_TOPIC,
                self.config.action_reliable,
                transport.clone(),
                self.metrics.clone(),
                DataStream::Action,
            );
            *self.action_publisher.lock() = Some(Arc::new(publisher));
        }

        if !self.config.action_chunks.is_empty() {
            for spec in &self.config.action_chunks {
                log::info!(
                    "[{}] ready to publish chunk '{}' via byte stream (horizon={}, {} fields)",
                    self.config.session,
                    spec.name,
                    spec.horizon,
                    spec.fields.len()
                );
                let publisher = ChunkPublisher::new(spec.clone(), transport.clone(), self.metrics.clone());
                self.chunk_publishers.lock().insert(spec.name.clone(), Arc::new(publisher));
            }
        }
    }

    /// Snapshot of metrics since construction or the last `reset_metrics()`.
    pub fn metrics(&self) -> PortalMetrics {
        let (video_fill, state_fill) = match self.sync_buffer.lock().as_ref() {
            Some(sb) => {
                let sb = sb.lock();
                (sb.video_fill_snapshot(), sb.state_fill())
            }
            None => (HashMap::new(), 0),
        };
        self.metrics.snapshot(video_fill, state_fill)
    }

    pub fn reset_metrics(&self) {
        self.metrics.reset();
    }
}

/// Names of every video track on a config, regardless of transport. Used
/// when registering metrics and sync-buffer slots, since the consumer-facing
/// API doesn't distinguish WebRTC and frame-video tracks.
fn combined_track_names(config: &PortalConfig) -> Vec<String> {
    let mut names: Vec<String> = config.video_tracks.iter().map(|s| s.name.clone()).collect();
    names.extend(config.frame_video_tracks.iter().map(|s| s.name.clone()));
    names
}

/// Snapshot of the fields the room event loop needs, so it doesn't take any
/// Portal-level lock on the hot path.
struct EventContext {
    config: PortalConfig,
    /// Cached schema fingerprints so the receive hot path doesn't recompute
    /// them per packet. Matches the peer's fingerprint when schemas agree;
    /// a mismatch logs once per offending value and drops the packet.
    action_schema_fp: u32,
    state_schema_fp: u32,
    sync_buffer: Option<Arc<Mutex<SyncBuffer>>>,
    obs_sink: Arc<ObservationSink>,
    action: Arc<ActionSlot>,
    state: Arc<StateSlot>,
    chunk_slots: Vec<Arc<ChunkSlot>>,
    unknown_chunk_fp_warns: Arc<Mutex<HashSet<u32>>>,
    video_tracks: HashMap<String, Arc<VideoTrackSlots>>,
    /// Frame-video entries (spec + slots + metrics fused) keyed by track
    /// name. Shared as `Arc<HashMap>` so per-frame fan-out into spawn
    /// tasks bumps a refcount instead of cloning the map.
    frame_video_entries: Arc<HashMap<String, Arc<FrameVideoTrackEntry>>>,
    metrics: Arc<MetricsRegistry>,
    rtt: Arc<RttService>,
    /// Multi-controller state, shared with `Portal` so attribute and
    /// participant lifecycle events can update it directly without going
    /// through the Portal struct.
    controller: Arc<ControllerState>,
    /// Cached at connect time. Used to skip self when classifying participants
    /// observed via `ParticipantConnected` / `ParticipantAttributesChanged` —
    /// our own attribute updates also fire these events on the local participant.
    local_identity: String,
    /// The transport the event stream came from; the receiver path uses it
    /// to spawn video receivers on subscribed WebRTC tracks.
    transport: Arc<dyn PortalTransport>,
}

/// Classify a remote participant by their `lk.portal.role` attribute and
/// reconcile controller state. Idempotent: re-observing the same participant
/// does not re-fire `on_operator_joined`. Used by the connect-time snapshot
/// and the ongoing `ParticipantConnected` / `ParticipantAttributesChanged`
/// handlers.
fn classify_and_update(
    controller: &ControllerState,
    self_role: Role,
    identity: &str,
    attrs: &HashMap<String, String>,
) {
    let id = identity.to_string();
    match classify_role(attrs) {
        Some(Role::Robot) => {
            {
                let mut slot = controller.robot_identity.lock();
                if slot.as_deref() != Some(id.as_str()) {
                    *slot = Some(id.clone());
                }
            }
            // Operator-side: mirror the robot's `active_operator` attribute.
            if self_role == Role::Operator {
                let new_value = attrs
                    .get(ACTIVE_OPERATOR_ATTR_KEY)
                    .and_then(|v| if v.is_empty() { None } else { Some(v.clone()) });
                let mut slot = controller.active_operator.lock();
                if *slot != new_value {
                    *slot = new_value.clone();
                    drop(slot);
                    controller.fire_active_changed(new_value.as_deref());
                }
            }
        }
        Some(Role::Operator) => {
            let inserted = controller.operators.lock().insert(id.clone());
            if inserted {
                controller.fire_op_joined(&id);
            }
        }
        None => {
            // Role attribute not yet visible; wait for a follow-up
            // ParticipantAttributesChanged event.
        }
    }
}

/// Implementation of the `portal.set_active_operator` RPC, registered on the
/// Robot side at connect. Anyone in the room may call this; payload is the
/// new identity (or empty string to clear). The handler updates the local
/// pointer and the broadcast attribute, then fires
/// `on_active_operator_changed` if the value actually moved.
async fn set_active_operator_rpc_impl(
    transport: &dyn PortalTransport,
    controller: &ControllerState,
    data: RpcInvocationData,
) -> Result<String, RpcError> {
    let identity = if data.payload.is_empty() { None } else { Some(data.payload.clone()) };
    if transport.local_identity().is_none() {
        return Err(RpcError::new(RPC_NOT_CONNECTED, "robot not connected", None));
    }
    let prev = controller.active_operator.lock().clone();
    let mut attrs = HashMap::new();
    attrs.insert(ACTIVE_OPERATOR_ATTR_KEY.to_string(), identity.clone().unwrap_or_default());
    if let Err(e) = transport.set_attributes(attrs).await {
        return Err(RpcError::new(
            RPC_SET_ATTRIBUTES_FAILED,
            format!("set_attributes failed: {e}"),
            None,
        ));
    }
    *controller.active_operator.lock() = identity.clone();
    if prev != identity {
        controller.fire_active_changed(identity.as_deref());
    }
    Ok(String::new())
}

fn handle_room_event(ctx: &EventContext, event: TransportEvent) {
    match event {
        TransportEvent::VideoTrackSubscribed { track_name } => {
            if ctx.config.role != Role::Operator {
                return;
            }
            if ctx.config.video_tracks.iter().any(|s| s.name == track_name) {
                log::info!("[{}] subscribed to video track '{track_name}'", ctx.config.session);
                if let Some(sync_buffer) = &ctx.sync_buffer {
                    let slots = ctx
                        .video_tracks
                        .get(track_name.as_str())
                        .cloned()
                        .unwrap_or_else(|| Arc::new(VideoTrackSlots::new()));
                    let track_metrics = ctx
                        .metrics
                        .track(track_name.as_str())
                        .expect("track metrics registered at construction");

                    ctx.transport.start_video_receiver(
                        &track_name,
                        VideoSink {
                            track_name: track_name.clone(),
                            sync_buffer: sync_buffer.clone(),
                            slots,
                            obs_sink: ctx.obs_sink.clone(),
                            metrics: track_metrics,
                        },
                    );
                }
            }
        }
        TransportEvent::DataReceived { payload, topic, sender } => {
            // Active-operator gate. Drop incoming actions whose sender does
            // not match `active_operator`. Applies to both the robot (always
            // processes ACTION_TOPIC) and operators with subscription on
            // (recorders, shadow eval, live monitoring). Operators without
            // subscription short-circuit before the deserialize so the
            // receive hot path costs nothing for the common controller-only
            // case. Non-action topics (state, RTT) bypass the gate and pass
            // an empty sender — those records don't carry a sender field.
            let gate_sender: String = match (ctx.config.role, topic.as_str()) {
                (Role::Robot, ACTION_TOPIC) => {
                    let Some(sender) = sender else {
                        return;
                    };
                    let active = ctx.controller.active_operator.lock().clone();
                    if active.as_deref() != Some(sender.as_str()) {
                        return;
                    }
                    sender
                }
                (Role::Operator, ACTION_TOPIC) => {
                    if !ctx.config.action_subscription {
                        return;
                    }
                    let Some(sender) = sender else {
                        return;
                    };
                    let active = ctx.controller.active_operator.lock().clone();
                    if active.as_deref() != Some(sender.as_str()) {
                        return;
                    }
                    sender
                }
                _ => String::new(),
            };
            let output = handle_data_received(
                &payload,
                &topic,
                ctx.config.role,
                &ctx.config.action_schema,
                ctx.action_schema_fp,
                &ctx.config.state_schema,
                ctx.state_schema_fp,
                &ctx.action,
                &ctx.state,
                ctx.sync_buffer.as_ref(),
                &ctx.metrics,
                &ctx.rtt,
                gate_sender,
            );
            if !output.is_empty() {
                ctx.obs_sink.dispatch(output);
            }
        }
        TransportEvent::ByteStream { topic, sender, payload } => {
            // Two Portal byte-stream topics, each owned by a different role:
            //   * `portal_action_chunk` — operator → robot. Action chunks
            //     too big to fit in a 15 KB data packet.
            //   * `portal_frame_video`  — robot → operator. Per-frame
            //     RGB/PNG/MJPEG payloads that bypass the WebRTC media path.
            // The transport has already filtered by the topics Portal
            // declared at connect (`byte_stream_topics`), and has read each
            // stream to completion before forwarding it here — a finished
            // stream is one payload.
            match (ctx.config.role, topic.as_str()) {
                (_, ACTION_CHUNK_TOPIC) => {
                    // Robot always consumes chunks. Operators only consume
                    // when subscription is on (HITL recording, shadow eval).
                    // The topic filter keeps non-subscribed operators from
                    // ever seeing the stream; this check is defense in depth.
                    if matches!(ctx.config.role, Role::Operator) && !ctx.config.action_subscription
                    {
                        return;
                    }
                    // Apply the active-operator gate at delivery time.
                    // Sender at delivery wins; a chunk started under one
                    // operator and finishing under another is dropped if the
                    // new active is different.
                    let active = ctx.controller.active_operator.lock().clone();
                    if active.as_deref() != Some(sender.as_str()) {
                        return;
                    }
                    dispatch_chunk_payload(
                        &payload,
                        &ctx.chunk_slots,
                        &ctx.unknown_chunk_fp_warns,
                        &ctx.metrics,
                        sender,
                    );
                }
                (Role::Operator, FRAME_VIDEO_TOPIC) => {
                    // Operator-side: each byte stream carries one frame for
                    // some declared frame-video track. The header in the
                    // payload routes it to the right entry (spec + slots
                    // + metrics fused; one HashMap lookup at dispatch).
                    if ctx.frame_video_entries.is_empty() {
                        return;
                    }
                    let Some(sync_buffer) = ctx.sync_buffer.clone() else {
                        return;
                    };
                    let _ = sender;
                    // `payload` is consumed as `Bytes` so the `Raw` codec
                    // gets a zero-copy view of the wire payload all the way
                    // to `VideoFrameData.data`.
                    dispatch_frame_payload(
                        payload,
                        &ctx.frame_video_entries,
                        &sync_buffer,
                        &ctx.obs_sink,
                    );
                }
                _ => {}
            }
        }
        TransportEvent::ParticipantConnected(info) => {
            // Snapshot the peer's attributes once they are visible. We may
            // observe an empty attribute map if the new participant has not
            // yet completed their `set_attributes` call; the
            // `ParticipantAttributesChanged` event will reclassify them when
            // the role attribute lands.
            classify_and_update(&ctx.controller, ctx.config.role, &info.identity, &info.attributes);
        }
        TransportEvent::ParticipantAttributesChanged(info) => {
            // Skip our own attribute updates: when we self-set `role` /
            // `active_operator`, the room echoes the change back through this
            // event for the local participant.
            if info.identity == ctx.local_identity {
                return;
            }
            classify_and_update(&ctx.controller, ctx.config.role, &info.identity, &info.attributes);
        }
        TransportEvent::ParticipantDisconnected { identity } => {
            // Multi-controller bookkeeping. The `active_operator` pointer
            // stays pinned by design (see spec.md §Defaults: "stays pinned");
            // a same-identity reconnect resumes control, a different operator
            // claims explicitly via `set_active_operator`.
            let id_str = identity;
            log::info!("[{}] participant '{}' disconnected", ctx.config.session, id_str);
            if ctx.controller.operators.lock().remove(&id_str) {
                ctx.controller.fire_op_left(&id_str);
            }
            let mut robot_slot = ctx.controller.robot_identity.lock();
            if robot_slot.as_deref() == Some(id_str.as_str()) {
                *robot_slot = None;
            }
        }
        TransportEvent::Reconnected => {
            log::info!(
                "[{}] reconnected, clearing sync buffers and latest slots",
                ctx.config.session
            );
            if let Some(sb) = &ctx.sync_buffer {
                sb.lock().clear();
            }
            // Pre-reconnect data is stale by definition; consumers calling
            // get_* after a reconnect should see None until fresh packets
            // arrive, matching the semantics already applied to sync_buffer.
            ctx.obs_sink.clear();
            ctx.action.clear();
            ctx.state.clear();
            for slot in &ctx.chunk_slots {
                slot.clear();
            }
            for slots in ctx.video_tracks.values() {
                slots.clear();
            }
            // Reset the per-room rosters but keep `active_operator` pinned —
            // the robot has no self-event to re-read its own attribute, and
            // the operator-side mirror gets reseeded by the post-reconnect
            // `ParticipantConnected` for the robot (idempotent on equal
            // values). Clearing it here would silently stall control on the
            // robot side across any transient reconnect.
            ctx.controller.clear_for_reconnect();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::Codec;
    use crate::config::{FrameVideoSpec, VideoTrackSpec};
    use crate::transport::{
        PortalTransport, ParticipantInfo, TransportConnect, TransportFuture, TransportRpcRequest,
        VideoReceiverHandle, VideoSink,
    };
    use std::time::Duration;

    /// Minimal in-crate transport: every method is a no-op that succeeds.
    /// Enough to drive `connect_with_transport` through an operator's setup
    /// (no publishers to build) so `ingest_video_frame`'s happy path is
    /// reachable in tests; inbound events never fire because the fake never
    /// sends any.
    struct FakeTransport;

    impl PortalTransport for FakeTransport {
        fn connect(&self, _params: TransportConnect<'_>) -> TransportFuture<PortalResult<()>> {
            Box::pin(std::future::ready(Ok(())))
        }

        fn disconnect(&self) -> TransportFuture<PortalResult<()>> {
            Box::pin(std::future::ready(Ok(())))
        }

        fn publish_data(
            &self,
            _payload: Vec<u8>,
            _topic: Option<String>,
            _reliable: bool,
        ) -> TransportFuture<PortalResult<()>> {
            Box::pin(std::future::ready(Ok(())))
        }

        fn send_bytes(&self, _payload: Vec<u8>, _topic: &str) -> TransportFuture<PortalResult<()>> {
            Box::pin(std::future::ready(Ok(())))
        }

        fn set_attributes(
            &self,
            _attrs: HashMap<String, String>,
        ) -> TransportFuture<PortalResult<()>> {
            Box::pin(std::future::ready(Ok(())))
        }

        fn perform_rpc(
            &self,
            _request: TransportRpcRequest,
        ) -> TransportFuture<Result<String, RpcError>> {
            Box::pin(std::future::ready(Err(RpcError::new(1, "fake: no rpc", None))))
        }

        fn register_rpc_method(&self, _method: String, _handler: RpcHandler) {}

        fn unregister_rpc_method(&self, _method: &str) {}

        fn local_identity(&self) -> Option<String> {
            Some("op-1".to_string())
        }

        fn local_attributes(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        fn remote_participants(&self) -> Vec<ParticipantInfo> {
            Vec::new()
        }

        fn start_video_receiver(
            &self,
            _track_name: &str,
            _sink: VideoSink,
        ) -> Option<Box<dyn VideoReceiverHandle>> {
            None
        }

        fn publish_video_frame(
            &self,
            _track_name: &str,
            _rgb: &[u8],
            _width: u32,
            _height: u32,
            _timestamp_us: Option<u64>,
        ) -> PortalResult<()> {
            Ok(())
        }

        fn sleep(&self, _duration: Duration) -> TransportFuture<()> {
            Box::pin(std::future::ready(()))
        }
    }

    fn operator_config() -> PortalConfig {
        let mut config = PortalConfig::new("test", Role::Operator);
        // No RTT pinger: the fake's `sleep` is always-ready, and a ping loop
        // against it would spin the single-threaded test runtime.
        config.ping_ms = 0;
        config
    }

    fn rgb(w: u32, h: u32) -> Vec<u8> {
        vec![0u8; (w * h * 3) as usize]
    }

    #[tokio::test]
    async fn ingest_routes_frame_through_slots_and_callback() {
        let mut config = operator_config();
        config.video_tracks.push(VideoTrackSpec::new("cam", Codec::Raw, None, false, false));
        let portal = Portal::new(config);

        let got = Arc::new(Mutex::new(None::<VideoFrameData>));
        let got_cb = got.clone();
        portal.on_video_frame("cam", move |name, frame| {
            assert_eq!(name, "cam");
            *got_cb.lock() = Some(frame.clone());
        });

        portal
            .connect_with_transport(Arc::new(FakeTransport), "ws://test", "tok")
            .await
            .unwrap();
        portal.ingest_video_frame("cam", rgb(8, 8), 8, 8, 12_345).unwrap();

        let latest = portal.get_video_frame("cam").expect("latest slot updated");
        assert_eq!((latest.width, latest.height), (8, 8));
        assert_eq!(latest.timestamp_us, 12_345);
        assert_eq!(latest.data.len(), 8 * 8 * 3);
        let cb_frame = got.lock().clone().expect("callback fired");
        assert_eq!(cb_frame.timestamp_us, 12_345);
    }

    #[tokio::test]
    async fn ingest_accepts_frame_video_track_names() {
        let mut config = operator_config();
        config.frame_video_tracks.push(FrameVideoSpec::new("fv", Codec::Raw, 0));
        let portal = Portal::new(config);

        portal
            .connect_with_transport(Arc::new(FakeTransport), "ws://test", "tok")
            .await
            .unwrap();
        portal.ingest_video_frame("fv", rgb(4, 4), 4, 4, 7).unwrap();
        assert_eq!(portal.get_video_frame("fv").unwrap().timestamp_us, 7);
    }

    #[tokio::test]
    async fn ingest_rejects_wrong_role() {
        let mut config = PortalConfig::new("test", Role::Robot);
        config.ping_ms = 0;
        config.video_tracks.push(VideoTrackSpec::new("cam", Codec::Raw, None, false, false));
        let portal = Portal::new(config);
        let err = portal.ingest_video_frame("cam", rgb(8, 8), 8, 8, 0).unwrap_err();
        assert!(matches!(err, PortalError::WrongRole(Role::Robot)));
    }

    #[tokio::test]
    async fn ingest_rejects_wrong_frame_size() {
        let mut config = operator_config();
        config.video_tracks.push(VideoTrackSpec::new("cam", Codec::Raw, None, false, false));
        let portal = Portal::new(config);
        let err = portal.ingest_video_frame("cam", vec![0u8; 10], 8, 8, 0).unwrap_err();
        assert!(matches!(err, PortalError::WrongFrameSize { expected: 192, got: 10 }));
    }

    #[tokio::test]
    async fn ingest_rejects_unknown_track() {
        let portal = Portal::new(operator_config());
        let err = portal.ingest_video_frame("nope", rgb(8, 8), 8, 8, 0).unwrap_err();
        assert!(matches!(err, PortalError::UnknownVideoTrack { ref name } if name == "nope"));
    }

    #[tokio::test]
    async fn ingest_requires_connect() {
        let mut config = operator_config();
        config.video_tracks.push(VideoTrackSpec::new("cam", Codec::Raw, None, false, false));
        let portal = Portal::new(config);
        let err = portal.ingest_video_frame("cam", rgb(8, 8), 8, 8, 0).unwrap_err();
        assert!(matches!(err, PortalError::NotConnected));
    }
}