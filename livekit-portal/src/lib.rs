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

//! SDK-facing Portal crate: the native LiveKit transport wired onto the
//! transport-agnostic [`livekit-portal-core`](livekit_portal_core) crate.
//!
//! All of Portal's logic lives in the core crate; everything is re-exported
//! here unchanged, so every existing `livekit_portal::…` import path keeps
//! working. This crate's only job is to enable the core's `native` feature —
//! the LiveKit Rust SDK transport (libwebrtc, yuv) for native targets.
//! Browser builds depend on `livekit-portal-core` with default features
//! instead and supply a JS transport implementing `PortalTransport`.

pub use livekit_portal_core::{
    codec, config, config_file, dtype, error, metrics, rpc, serialization, sync_buffer, time,
    transport, types,
};

pub use codec::Codec;
pub use config::{
    ChunkSpec, DEFAULT_H264_MAX_BITRATE_KBPS, FieldSpec, FrameVideoSpec, PortalConfig,
    VideoTrackSpec,
};
pub use config_file::ConfigFileError;
pub use dtype::DType;
pub use error::{PortalError, PortalResult};
pub use livekit_portal_core::{
    BYTE_STREAM_CHUNK_SIZE, LiveKitRustTransport, ParticipantInfo, PortalTransport,
    TransportConnect, TransportEvent, TransportFuture, TransportRpcRequest, VideoReceiverHandle,
    VideoSink, now_us,
};
pub use metrics::{
    BufferMetrics, PolicyMetrics, PortalMetrics, RttMetrics, SyncMetrics, TransportMetrics,
};
pub use livekit_portal_core::{
    ACTIVE_OPERATOR_ATTR_KEY, Portal, ROLE_ATTR_KEY, SET_ACTIVE_OPERATOR_RPC,
};
pub use rpc::{RpcError, RpcHandler, RpcInvocationData};
pub use types::{
    Action, ActionChunk, ChunkColumn, Observation, Role, State, SyncConfig, TypedValue,
    VideoFrameData, VideoTrackSlots,
};