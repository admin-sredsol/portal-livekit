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

//! Transport-agnostic core of LiveKit Portal.
//!
//! All of Portal's protocol logic — role setup, state/action publishing,
//! chunk and frame-video byte streams, RPC routing, the multi-controller
//! layer, and the sync pipeline — lives here, programmed against the
//! [`transport::PortalTransport`] trait instead of the LiveKit SDK. The
//! crate therefore builds for `wasm32-unknown-unknown` as well as all
//! native targets.
//!
//! Two transports implement the seam:
//!
//! * `LiveKitRustTransport` ([`native`]), behind the crate's `native`
//!   cargo feature: the LiveKit Rust SDK (libwebrtc + yuv) for native
//!   targets.
//! * A JS-backed transport (Phase 3 of the WASM port): the same Portal
//!   logic compiled to wasm, driving the LiveKit JS SDK from the browser.
//!
//! The `livekit-portal` crate is a thin facade that enables `native` and
//! re-exports everything, so downstream import paths are unchanged.

pub mod codec;
pub mod config;
pub mod config_file;
pub mod dtype;
pub mod error;
pub mod metrics;
pub mod rpc;
pub mod serialization;
pub mod sync_buffer;
pub mod time;
pub mod transport;
pub mod types;

// Orchestration + the byte-stream/RTT services. Private modules; their
// public items are re-exported below.
mod data;
mod frame_video;
pub mod portal;
mod rtt;

// SDK- and libwebrtc-facing modules, native targets only.
#[cfg(feature = "native")]
pub mod native;
#[cfg(feature = "native")]
mod video;

pub use codec::Codec;
pub use config::{
    ChunkSpec, DEFAULT_H264_MAX_BITRATE_KBPS, FieldSpec, FrameVideoSpec, PortalConfig,
    VideoTrackSpec,
};
pub use config_file::ConfigFileError;
pub use dtype::DType;
pub use error::{PortalError, PortalResult};
pub use frame_video::BYTE_STREAM_CHUNK_SIZE;
pub use metrics::{
    BufferMetrics, PolicyMetrics, PortalMetrics, RttMetrics, SyncMetrics, TransportMetrics,
};
pub use portal::{ACTIVE_OPERATOR_ATTR_KEY, Portal, ROLE_ATTR_KEY, SET_ACTIVE_OPERATOR_RPC};
pub use rpc::{RpcError, RpcHandler, RpcInvocationData};
pub use time::now_us;
pub use transport::{
    ParticipantInfo, PortalTransport, TransportConnect, TransportEvent, TransportFuture,
    TransportRpcRequest, VideoReceiverHandle, VideoSink,
};
pub use types::{
    Action, ActionChunk, ChunkColumn, Observation, Role, State, SyncConfig, TypedValue,
    VideoFrameData, VideoTrackSlots,
};

#[cfg(feature = "native")]
pub use native::LiveKitRustTransport;