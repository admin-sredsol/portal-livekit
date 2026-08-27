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

//! JS-facing inbound event sink.
//!
//! Natively the transport pumps SDK room events into Portal's channel
//! itself. In the browser the room lives in `livekit-js` on the JS side of
//! the seam, so the JS transport calls these methods instead; the sink
//! forwards into the exact channel core's event loop drains. One channel,
//! two producers, zero protocol duplication.
//!
//! The sink also carries the inbound-RPC entry point
//! ([`PortalEventSink::invoke_rpc_method`]): livekit-js delivers RPC
//! invocations to the JS transport, which dispatches them here. The
//! returned promise resolves with the handler's string result or rejects
//! with `{code, message, data}` — the same shape outbound RPC rejects with.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use wasm_bindgen::prelude::*;

use tokio::sync::mpsc::UnboundedSender;

use livekit_portal_core::rpc::RpcInvocationData;
use livekit_portal_core::transport::{ParticipantInfo, TransportEvent};

use crate::transport::{invoke_rpc_handler, rpc_error_to_js};

/// Handed to the JS transport at connect time (`connectInfo.events`). JS
/// forwards every translated room event through these methods; each maps
/// 1:1 onto a [`TransportEvent`]. Malformed attribute maps degrade to
/// empty rather than throwing — a missing role attribute is handled by
/// core's classification logic, not by unwinding into the JS caller.
#[wasm_bindgen]
pub struct PortalEventSink {
    tx: UnboundedSender<TransportEvent>,
    handlers: crate::transport::RpcHandlerMap,
}

// The real constructor takes the channel sender and the shared handler
// map, neither of which is JS-constructible — so it lives outside the
// exported impl block.
impl PortalEventSink {
    pub(crate) fn new(
        tx: UnboundedSender<TransportEvent>,
        handlers: crate::transport::RpcHandlerMap,
    ) -> Self {
        Self { tx, handlers }
    }
}

#[wasm_bindgen]
impl PortalEventSink {
    /// A data packet arrived (`livekit-js` `DataReceived`). Packets without
    /// a topic are never forwarded — Portal's protocol always topics.
    #[wasm_bindgen(js_name = onDataReceived)]
    pub fn on_data_received(&self, payload: Vec<u8>, topic: String, sender: Option<String>) {
        let _ = self.tx.send(TransportEvent::DataReceived {
            payload: Bytes::from(payload),
            topic,
            sender,
        });
    }

    /// A byte stream on a subscribed topic finished reading — one finished
    /// stream is one payload (action chunk, frame-video frame).
    #[wasm_bindgen(js_name = onByteStream)]
    pub fn on_byte_stream(&self, topic: String, sender: String, payload: Vec<u8>) {
        let _ = self.tx.send(TransportEvent::ByteStream {
            topic,
            sender,
            payload: Bytes::from(payload),
        });
    }

    /// A remote video track was subscribed. In the browser the sink is the
    /// only delivery path: the JS transport starts decoding and pushes
    /// frames through `WasmPortal.ingestVideoFrame`; core uses this event
    /// to wire up the sync pipeline for the track.
    #[wasm_bindgen(js_name = onVideoTrackSubscribed)]
    pub fn on_video_track_subscribed(&self, track_name: String) {
        let _ = self.tx.send(TransportEvent::VideoTrackSubscribed { track_name });
    }

    #[wasm_bindgen(js_name = onParticipantConnected)]
    pub fn on_participant_connected(&self, identity: String, attributes: JsValue) {
        let _ = self.tx.send(TransportEvent::ParticipantConnected(ParticipantInfo {
            identity,
            attributes: attributes_to_map(attributes),
        }));
    }

    #[wasm_bindgen(js_name = onParticipantAttributesChanged)]
    pub fn on_participant_attributes_changed(&self, identity: String, attributes: JsValue) {
        let _ = self.tx.send(TransportEvent::ParticipantAttributesChanged(ParticipantInfo {
            identity,
            attributes: attributes_to_map(attributes),
        }));
    }

    #[wasm_bindgen(js_name = onParticipantDisconnected)]
    pub fn on_participant_disconnected(&self, identity: String) {
        let _ = self.tx.send(TransportEvent::ParticipantDisconnected { identity });
    }

    #[wasm_bindgen(js_name = onReconnected)]
    pub fn on_reconnected(&self) {
        let _ = self.tx.send(TransportEvent::Reconnected);
    }

    /// Inbound RPC invocation from livekit-js. Returns a promise the JS
    /// transport should relay as the RPC result: resolve it with the
    /// string the handler returned, or let it reject with
    /// `{code, message, data}`.
    #[wasm_bindgen(js_name = invokeRpcMethod)]
    pub fn invoke_rpc_method(
        &self,
        method: String,
        request_id: String,
        caller_identity: String,
        payload: String,
        response_timeout_ms: f64,
    ) -> js_sys::Promise {
        let handlers = self.handlers.clone();
        let data = RpcInvocationData {
            request_id,
            caller_identity,
            payload,
            response_timeout: Duration::from_millis(response_timeout_ms.max(0.0) as u64),
        };
        wasm_bindgen_futures::future_to_promise(async move {
            match invoke_rpc_handler(&handlers, &method, data).await {
                Ok(result) => Ok(JsValue::from(result)),
                Err(error) => Err(rpc_error_to_js(&error)),
            }
        })
    }
}

fn attributes_to_map(value: JsValue) -> HashMap<String, String> {
    serde_wasm_bindgen::from_value(value).unwrap_or_default()
}