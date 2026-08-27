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

//! The JS transport contract and its Rust adapter.
//!
//! `JsTransport` is the interface an embedder implements in JavaScript
//! (Phase 3's `LiveKitJsTransport` over `livekit-js`): one method per
//! [`PortalTransport`] seam operation, promise-returning for the async
//! ones. [`JsTransportAdapter`] bridges it into the trait Portal is
//! programmed against, converting serde payloads through
//! `serde-wasm-bindgen`.
//!
//! # Inbound RPC
//!
//! The JS transport receives RPC invocations from livekit-js and calls
//! back through the [`PortalEventSink`] it received at connect time
//! (`sink.invokeRpcMethod(...)`), which dispatches into the handler map
//! registered here. JS resolves the returned promise with the string
//! result, or rejects it with `{code, message, data}`.
//!
//! # Error convention
//!
//! Rejected promises are read as `{code, message, data}` (RPC semantics);
//! anything else degrades to a stringified message under the
//! application-error code.
//!
//! # Send / Sync
//!
//! `JsValue` is `Send + Sync` on wasm32 (single-threaded), so `JsFuture`s
//! satisfy the `Send`-boxed [`TransportFuture`] contract unchanged.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use js_sys::Promise;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use livekit_portal_core::error::{PortalError, PortalResult};
use livekit_portal_core::rpc::{RpcError, RpcHandler, RpcInvocationData};
use livekit_portal_core::transport::{
    ParticipantInfo, PortalTransport, TransportConnect, TransportFuture, TransportRpcRequest,
    VideoReceiverHandle, VideoSink,
};

use crate::sink::PortalEventSink;

/// LiveKit RPC `ApplicationError` — the fallback code when a rejection
/// carries no `{code, ...}` shape.
const RPC_APPLICATION_ERROR: u32 = 1500;

/// RPC methods registered by Portal (built-in + user), keyed by method
/// name. Shared between the adapter (writer, via the trait) and the event
/// sink (reader/dispatcher, from JS).
pub(crate) type RpcHandlerMap = Arc<parking_lot::Mutex<HashMap<String, RpcHandler>>>;

/// The JS-side transport object. The embedder implements the methods below;
/// `WasmPortal::connect` receives one and Portal drives it through
/// [`JsTransportAdapter`].
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_name = JsTransport)]
    pub type JsTransportObject;

    /// Establish the room connection. `connectInfo` is
    /// `{url, token, byteStreamTopics: string[]}`; the event sink arrives
    /// separately via [`JsTransportObject::js_bind_event_sink`], always
    /// before this call. Events that fire during connect (self-join, early
    /// participants) are therefore not lost.
    #[wasm_bindgen(method, js_name = connect)]
    fn js_connect(this: &JsTransportObject, connect_info: JsValue) -> Promise;

    #[wasm_bindgen(method, js_name = disconnect)]
    fn js_disconnect(this: &JsTransportObject) -> Promise;

    #[wasm_bindgen(method, js_name = publishData)]
    fn js_publish_data(
        this: &JsTransportObject,
        payload: Vec<u8>,
        topic: Option<String>,
        reliable: bool,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = sendBytes)]
    fn js_send_bytes(this: &JsTransportObject, payload: Vec<u8>, topic: String) -> Promise;

    #[wasm_bindgen(method, js_name = setAttributes)]
    fn js_set_attributes(this: &JsTransportObject, attributes: JsValue) -> Promise;

    /// `request` is
    /// `{destination, method, payload, responseTimeoutMs: number | null}`.
    /// Resolves to the string result; rejects with `{code, message, data}`.
    #[wasm_bindgen(method, js_name = performRpc)]
    fn js_perform_rpc(this: &JsTransportObject, request: JsValue) -> Promise;

    #[wasm_bindgen(method, js_name = registerRpcMethod)]
    fn js_register_rpc_method(this: &JsTransportObject, method: String);

    #[wasm_bindgen(method, js_name = unregisterRpcMethod)]
    fn js_unregister_rpc_method(this: &JsTransportObject, method: String);

    #[wasm_bindgen(method, js_name = localIdentity)]
    fn js_local_identity(this: &JsTransportObject) -> Option<String>;

    #[wasm_bindgen(method, js_name = localAttributes)]
    fn js_local_attributes(this: &JsTransportObject) -> JsValue;

    /// Array of `{identity: string, attributes: {[k: string]: string}}`.
    #[wasm_bindgen(method, js_name = remoteParticipants)]
    fn js_remote_participants(this: &JsTransportObject) -> JsValue;

    /// WebRTC-path receivers do not exist in the browser — decoded frames
    /// arrive via `WasmPortal.ingestVideoFrame`. The JS side records the
    /// subscription and does its own per-track teardown.
    #[wasm_bindgen(method, js_name = startVideoReceiver)]
    fn js_start_video_receiver(this: &JsTransportObject, track_name: String, sink: JsValue);

    #[wasm_bindgen(method, js_name = publishVideoFrame)]
    fn js_publish_video_frame(
        this: &JsTransportObject,
        track_name: String,
        rgb: Vec<u8>,
        width: u32,
        height: u32,
        timestamp_us: Option<f64>,
    ) -> Promise;

    /// Bind the event sink for this session. Called synchronously before
    /// the `connect` promise resolves. Passed by value: exported structs
    /// cross the boundary as an owned reference JS holds for the session.
    #[wasm_bindgen(method, js_name = bindEventSink)]
    fn js_bind_event_sink(this: &JsTransportObject, sink: PortalEventSink);

    #[wasm_bindgen(method, js_name = sleep)]
    fn js_sleep(this: &JsTransportObject, ms: f64) -> Promise;
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct ConnectInfo<'a> {
    url: &'a str,
    token: &'a str,
    byte_stream_topics: Vec<String>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct JsRpcRequest {
    destination: String,
    method: String,
    payload: String,
    response_timeout_ms: Option<f64>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsRpcError {
    code: u32,
    message: String,
    data: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsParticipantInfo {
    identity: String,
    #[serde(default)]
    attributes: HashMap<String, String>,
}

fn js_error_to_portal(value: JsValue) -> PortalError {
    match serde_wasm_bindgen::from_value::<JsRpcError>(value.clone()) {
        Ok(e) => PortalError::Room(format!("[{}] {}", e.code, e.message)),
        Err(_) => {
            let message = value.as_string().unwrap_or_else(|| format!("{value:?}"));
            PortalError::Room(message)
        }
    }
}

fn js_error_to_rpc(value: JsValue) -> RpcError {
    match serde_wasm_bindgen::from_value::<JsRpcError>(value.clone()) {
        Ok(e) => RpcError::new(e.code, e.message, e.data),
        Err(_) => {
            let message = value.as_string().unwrap_or_else(|| format!("{value:?}"));
            RpcError::new(RPC_APPLICATION_ERROR, message, None)
        }
    }
}

async fn js_unit(promise: Promise) -> PortalResult<()> {
    JsFuture::from(promise).await.map(|_| ()).map_err(js_error_to_portal)
}

/// Adapter implementing the [`PortalTransport`] seam over the JS object.
/// One instance per connect; thin by design — all protocol decisions stay
/// in core. Owns the RPC handler map that [`PortalEventSink`] dispatches
/// inbound invocations into.
pub struct JsTransportAdapter {
    inner: JsTransportObject,
    handlers: RpcHandlerMap,
}

impl JsTransportAdapter {
    pub fn new(inner: JsTransportObject) -> Self {
        Self { inner, handlers: Arc::new(parking_lot::Mutex::new(HashMap::new())) }
    }
}

impl PortalTransport for JsTransportAdapter {
    fn connect(&self, params: TransportConnect<'_>) -> TransportFuture<PortalResult<()>> {
        let sink = PortalEventSink::new(params.events, self.handlers.clone());
        let mut topics: Vec<String> = params.byte_stream_topics.into_iter().collect();
        topics.sort();
        let info = match serde_wasm_bindgen::to_value(&ConnectInfo {
            url: params.url,
            token: params.token,
            byte_stream_topics: topics,
        }) {
            Ok(v) => v,
            Err(e) => {
                return Box::pin(std::future::ready(Err(PortalError::Room(format!(
                    "connect info serialization failed: {e}"
                )))));
            }
        };
        // Bind before awaiting: events that fire while the JS connect is in
        // flight (room join, snapshot participants) must land.
        self.inner.js_bind_event_sink(sink);
        let promise = self.inner.js_connect(info);
        Box::pin(js_unit(promise))
    }

    fn disconnect(&self) -> TransportFuture<PortalResult<()>> {
        let promise = self.inner.js_disconnect();
        Box::pin(js_unit(promise))
    }

    fn publish_data(
        &self,
        payload: Vec<u8>,
        topic: Option<String>,
        reliable: bool,
    ) -> TransportFuture<PortalResult<()>> {
        let promise = self.inner.js_publish_data(payload, topic, reliable);
        Box::pin(js_unit(promise))
    }

    fn send_bytes(&self, payload: Vec<u8>, topic: &str) -> TransportFuture<PortalResult<()>> {
        let promise = self.inner.js_send_bytes(payload, topic.to_string());
        Box::pin(js_unit(promise))
    }

    fn set_attributes(
        &self,
        attrs: HashMap<String, String>,
    ) -> TransportFuture<PortalResult<()>> {
        // Plain object, not serde: serde-wasm-bindgen serializes HashMap as
        // a JS Map, but the contract documents `{[k: string]: string}` —
        // and livekit-js's setAttributes takes a Record.
        let obj = js_sys::Object::new();
        for (key, value) in &attrs {
            let _ = js_sys::Reflect::set(
                &obj,
                &JsValue::from_str(key),
                &JsValue::from_str(value),
            );
        }
        let promise = self.inner.js_set_attributes(obj.into());
        Box::pin(js_unit(promise))
    }

    fn perform_rpc(
        &self,
        request: TransportRpcRequest,
    ) -> TransportFuture<Result<String, RpcError>> {
        let request = JsRpcRequest {
            destination: request.destination,
            method: request.method,
            payload: request.payload,
            response_timeout_ms: request.response_timeout.map(|d| d.as_millis() as f64),
        };
        let promise = match serde_wasm_bindgen::to_value(&request) {
            Ok(v) => self.inner.js_perform_rpc(v),
            Err(e) => {
                return Box::pin(std::future::ready(Err(RpcError::new(
                    RPC_APPLICATION_ERROR,
                    format!("rpc request serialization failed: {e}"),
                    None,
                ))));
            }
        };
        Box::pin(async move {
            let result = JsFuture::from(promise).await.map_err(js_error_to_rpc)?;
            Ok(result.as_string().unwrap_or_default())
        })
    }

    fn register_rpc_method(&self, method: String, handler: RpcHandler) {
        // Store locally for dispatch via the sink, and tell JS the method
        // exists so livekit-js's RPC machinery routes invocations to it.
        self.handlers.lock().insert(method.clone(), handler);
        self.inner.js_register_rpc_method(method);
    }

    fn unregister_rpc_method(&self, method: &str) {
        self.handlers.lock().remove(method);
        self.inner.js_unregister_rpc_method(method.to_string());
    }

    fn local_identity(&self) -> Option<String> {
        self.inner.js_local_identity()
    }

    fn local_attributes(&self) -> HashMap<String, String> {
        serde_wasm_bindgen::from_value(self.inner.js_local_attributes()).unwrap_or_default()
    }

    fn remote_participants(&self) -> Vec<ParticipantInfo> {
        serde_wasm_bindgen::from_value::<Vec<JsParticipantInfo>>(self.inner.js_remote_participants())
            .unwrap_or_default()
            .into_iter()
            .map(|p| ParticipantInfo { identity: p.identity, attributes: p.attributes })
            .collect()
    }

    fn start_video_receiver(
        &self,
        track_name: &str,
        _sink: VideoSink,
    ) -> Option<Box<dyn VideoReceiverHandle>> {
        // The sink's Arc contents are not JS-reachable; the browser decodes
        // and re-ingests instead. JS gets a notification for symmetry.
        self.inner.js_start_video_receiver(track_name.to_string(), JsValue::NULL);
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
        let promise = self.inner.js_publish_video_frame(
            track_name.to_string(),
            rgb.to_vec(),
            width,
            height,
            timestamp_us.map(|t| t as f64),
        );
        // The trait method is sync (native publishers queue internally), so
        // the promise is detached; failures surface on the console rather
        // than stalling the publish hot path.
        wasm_bindgen_futures::spawn_local(async move {
            if let Err(e) = JsFuture::from(promise).await {
                web_sys::console::error_1(
                    &format!("[portal-wasm] publish_video_frame failed: {e:?}").into(),
                );
            }
        });
        Ok(())
    }

    fn sleep(&self, duration: Duration) -> TransportFuture<()> {
        let promise = self.inner.js_sleep(duration.as_secs_f64() * 1000.0);
        Box::pin(async move {
            let _ = JsFuture::from(promise).await;
        })
    }
}

/// Dispatch an inbound RPC invocation to the handler registered under
/// `method`. Free function so the sink can borrow the map briefly without
/// holding the lock across an await.
pub(crate) async fn invoke_rpc_handler(
    handlers: &RpcHandlerMap,
    method: &str,
    data: RpcInvocationData,
) -> Result<String, RpcError> {
    let handler = handlers.lock().get(method).cloned();
    match handler {
        Some(handler) => handler(data).await,
        None => Err(RpcError::new(
            RPC_METHOD_NOT_FOUND,
            format!("no handler registered for method '{method}'"),
            None,
        )),
    }
}

/// LiveKit RPC `MethodNotFound`.
const RPC_METHOD_NOT_FOUND: u32 = 1602;

/// Serialize an [`RpcError`] into the `{code, message, data}` rejection
/// shape the JS side resolves errors with.
pub(crate) fn rpc_error_to_js(error: &RpcError) -> JsValue {
    let obj = js_sys::Object::new();
    let _ = js_sys::Reflect::set(&obj, &"code".into(), &error.code.into());
    let _ = js_sys::Reflect::set(&obj, &"message".into(), &error.message.clone().into());
    let _ = js_sys::Reflect::set(&obj, &"data".into(), &error.data.clone().into());
    obj.into()
}