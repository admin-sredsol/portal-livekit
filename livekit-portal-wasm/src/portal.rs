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

//! The JS-facing Portal wrapper.
//!
//! [`WasmPortal`] owns a core [`Portal`] and mirrors the UniFFI surface
//! one-to-one: senders, latest-wins getters, callback registration (JS
//! functions instead of Rust closures), RPC, the multi-controller
//! operations, metrics, and the browser-only video ingest. Every payload
//! crossing the boundary is converted by hand here — core types stay
//! serde-free (their `Bytes` buffers and typed maps do not map cleanly
//! onto serde), so the JS shapes are defined and documented in this file.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use js_sys::JsString;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use livekit_portal_core::config::FieldSpec;
use livekit_portal_core::metrics::PortalMetrics;
use livekit_portal_core::portal::Portal;
use livekit_portal_core::rpc::{RpcError, RpcInvocationData};
use livekit_portal_core::types::{
    Action, ActionChunk, ChunkColumn, Observation, State, TypedValue, VideoFrameData,
};

use crate::config::{field_map_from_js, WasmPortalConfig};
use crate::transport::JsTransportAdapter;

/// Convert any `Display`-able core error into a JS error carrying the
/// message. Errors keep their Rust `Display` text; JS matches on strings,
/// same as the UniFFI surface.
pub(crate) fn js_error(message: impl Into<String>) -> wasm_bindgen::JsError {
    wasm_bindgen::JsError::new(&message.into())
}

// --- Value conversion helpers (core → JS) ---

fn typed_value_to_js(value: &TypedValue) -> JsValue {
    match value {
        TypedValue::F64(v) => JsValue::from(*v),
        TypedValue::F32(v) => JsValue::from(*v),
        TypedValue::I32(v) => JsValue::from(*v),
        TypedValue::I16(v) => JsValue::from(*v),
        TypedValue::I8(v) => JsValue::from(*v),
        TypedValue::U32(v) => JsValue::from(*v),
        TypedValue::U16(v) => JsValue::from(*v),
        TypedValue::U8(v) => JsValue::from(*v),
        TypedValue::Bool(v) => JsValue::from(*v),
    }
}

fn map_to_js<V>(map: &HashMap<String, V>, to_js: impl Fn(&V) -> JsValue) -> JsValue {
    let obj = js_sys::Object::new();
    for (key, value) in map {
        let _ = js_sys::Reflect::set(&obj, &JsString::from(key.as_str()), &to_js(value));
    }
    obj.into()
}

fn video_frame_to_js(frame: &VideoFrameData) -> JsValue {
    let obj = js_sys::Object::new();
    let _ = js_sys::Reflect::set(&obj, &"width".into(), &frame.width.into());
    let _ = js_sys::Reflect::set(&obj, &"height".into(), &frame.height.into());
    let _ = js_sys::Reflect::set(&obj, &"timestampUs".into(), &frame.timestamp_us.into());
    let _ = js_sys::Reflect::set(&obj, &"data".into(), &js_sys::Uint8Array::from(&frame.data[..]).into());
    obj.into()
}

/// Convert a timestamp option (JS number, µs) into `Option<u64>`.
/// Negative values saturate to 0; µs timestamps stay under 2^53 until
/// roughly the year 2255, so f64 round-tripping is lossless in practice.
fn ts_option(v: Option<f64>) -> Option<u64> {
    v.map(|t| if t.is_nan() { 0 } else { t.max(0.0) as u64 })
}

// --- JS → core payload conversion ---

fn action_to_js(action: &Action) -> JsValue {
    let obj = js_sys::Object::new();
    let _ = js_sys::Reflect::set(
        &obj,
        &"values".into(),
        &map_to_js(&action.values, typed_value_to_js),
    );
    let _ = js_sys::Reflect::set(
        &obj,
        &"rawValues".into(),
        &map_to_js(&action.raw_values, |v| JsValue::from(*v)),
    );
    let _ = js_sys::Reflect::set(&obj, &"timestampUs".into(), &action.timestamp_us.into());
    let in_reply = match action.in_reply_to_ts_us {
        Some(ts) => ts.into(),
        None => JsValue::NULL,
    };
    let _ = js_sys::Reflect::set(&obj, &"inReplyToTsUs".into(), &in_reply);
    let _ = js_sys::Reflect::set(&obj, &"sender".into(), &action.sender.clone().into());
    obj.into()
}

fn state_to_js(state: &State) -> JsValue {
    let obj = js_sys::Object::new();
    let _ = js_sys::Reflect::set(
        &obj,
        &"values".into(),
        &map_to_js(&state.values, typed_value_to_js),
    );
    let _ = js_sys::Reflect::set(
        &obj,
        &"rawValues".into(),
        &map_to_js(&state.raw_values, |v| JsValue::from(*v)),
    );
    let _ = js_sys::Reflect::set(&obj, &"timestampUs".into(), &state.timestamp_us.into());
    obj.into()
}

fn observation_to_js(obs: &Observation) -> JsValue {
    let obj = js_sys::Object::new();
    let _ = js_sys::Reflect::set(
        &obj,
        &"state".into(),
        &map_to_js(&obs.state, typed_value_to_js),
    );
    let _ = js_sys::Reflect::set(
        &obj,
        &"rawState".into(),
        &map_to_js(&obs.raw_state, |v| JsValue::from(*v)),
    );
    let frames = js_sys::Object::new();
    for (name, frame) in &obs.frames {
        let _ = js_sys::Reflect::set(&frames, &JsString::from(name.as_str()), &video_frame_to_js(frame));
    }
    let _ = js_sys::Reflect::set(&obj, &"frames".into(), &frames.into());
    let _ = js_sys::Reflect::set(&obj, &"timestampUs".into(), &obs.timestamp_us.into());
    obj.into()
}

fn action_chunk_to_js(chunk: &ActionChunk) -> JsValue {
    let obj = js_sys::Object::new();
    let _ = js_sys::Reflect::set(&obj, &"name".into(), &chunk.name.clone().into());
    let _ = js_sys::Reflect::set(&obj, &"horizon".into(), &chunk.horizon.into());
    let columns = js_sys::Object::new();
    for (name, column) in &chunk.data {
        let arr = js_sys::Float64Array::from(column.as_slice());
        let _ = js_sys::Reflect::set(&columns, &JsString::from(name.as_str()), &arr.into());
    }
    let _ = js_sys::Reflect::set(&obj, &"data".into(), &columns.into());
    let _ = js_sys::Reflect::set(&obj, &"timestampUs".into(), &chunk.timestamp_us.into());
    let in_reply = match chunk.in_reply_to_ts_us {
        Some(ts) => ts.into(),
        None => JsValue::NULL,
    };
    let _ = js_sys::Reflect::set(&obj, &"inReplyToTsUs".into(), &in_reply);
    let _ = js_sys::Reflect::set(&obj, &"sender".into(), &chunk.sender.clone().into());
    obj.into()
}

fn metrics_to_js(metrics: &PortalMetrics) -> JsValue {
    // PortalMetrics' sub-structs are plain data but not serde-derived;
    // expose the aggregate counters that matter for dashboards and extend
    // deliberately as bindings need more. Per-track maps (bytes_sent etc.)
    // are summed; the maps stay reachable through core for consumers who
    // need per-track detail.
    let sum = |m: &HashMap<String, u64>| m.values().sum::<u64>();
    let obj = js_sys::Object::new();
    let _ = js_sys::Reflect::set(
        &obj,
        &"observationsEmitted".into(),
        &metrics.sync.observations_emitted.into(),
    );
    let _ = js_sys::Reflect::set(
        &obj,
        &"staleObservationsEmitted".into(),
        &metrics.sync.stale_observations_emitted.into(),
    );
    let _ = js_sys::Reflect::set(
        &obj,
        &"statesDropped".into(),
        &metrics.sync.states_dropped.into(),
    );
    let _ = js_sys::Reflect::set(
        &obj,
        &"bytesSent".into(),
        &sum(&metrics.transport.bytes_sent).into(),
    );
    let _ = js_sys::Reflect::set(
        &obj,
        &"bytesReceived".into(),
        &sum(&metrics.transport.bytes_received).into(),
    );
    let _ = js_sys::Reflect::set(
        &obj,
        &"statesSent".into(),
        &metrics.transport.states_sent.into(),
    );
    let _ = js_sys::Reflect::set(
        &obj,
        &"statesReceived".into(),
        &metrics.transport.states_received.into(),
    );
    let _ = js_sys::Reflect::set(
        &obj,
        &"actionsSent".into(),
        &metrics.transport.actions_sent.into(),
    );
    let _ = js_sys::Reflect::set(
        &obj,
        &"actionsReceived".into(),
        &metrics.transport.actions_received.into(),
    );
    let _ = js_sys::Reflect::set(&obj, &"pingsSent".into(), &metrics.rtt.pings_sent.into());
    let _ = js_sys::Reflect::set(
        &obj,
        &"pongsReceived".into(),
        &metrics.rtt.pongs_received.into(),
    );
    let rtt_mean = match metrics.rtt.rtt_us_mean {
        Some(v) => v.into(),
        None => JsValue::NULL,
    };
    let _ = js_sys::Reflect::set(&obj, &"rttUsMean".into(), &rtt_mean);
    let rtt_p95 = match metrics.rtt.rtt_us_p95 {
        Some(v) => v.into(),
        None => JsValue::NULL,
    };
    let _ = js_sys::Reflect::set(&obj, &"rttUsP95".into(), &rtt_p95);
    obj.into()
}

/// Chunk payload from JS: `{fieldName: number[]}` — one column per field,
/// untyped (coerces to the declared dtype, matching the wire's uniform
/// f64 widening).
fn chunk_data_from_js(
    data: JsValue,
    horizon: u32,
    schema: &[FieldSpec],
) -> Result<HashMap<String, ChunkColumn>, wasm_bindgen::JsError> {
    let obj = js_sys::Object::try_from(&data)
        .ok_or_else(|| js_error("data must be an object of {fieldName: number[]}"))?;
    let mut out = HashMap::new();
    for spec in schema {
        let v = js_sys::Reflect::get(obj, &JsString::from(spec.name.as_str()))
            .map_err(|e| js_error(format!("failed to read chunk field '{}': {e:?}", spec.name)))?;
        if v.is_undefined() {
            continue;
        }
        let arr = js_sys::Array::from(&v);
        let mut column = Vec::with_capacity(horizon as usize);
        for i in 0..arr.length() {
            let item = arr.get(i);
            column.push(item.as_f64().ok_or_else(|| {
                js_error(format!("chunk field '{}' contains a non-number", spec.name))
            })?);
        }
        out.insert(spec.name.clone(), ChunkColumn::untyped(column));
    }
    Ok(out)
}

// --- The exported Portal wrapper ---

/// Browser Portal. Wraps the transport-agnostic core and drives it through
/// a JS-implemented `JsTransport` (see the `livekit-portal-wasm-js`
/// package for the reference `LiveKitJsTransport` implementation).
#[wasm_bindgen]
pub struct WasmPortal {
    inner: Arc<Portal>,
}

#[wasm_bindgen]
impl WasmPortal {
    #[wasm_bindgen(constructor)]
    pub fn new(config: WasmPortalConfig) -> WasmPortal {
        WasmPortal { inner: Arc::new(Portal::new(config.config)) }
    }

    // --- Connection lifecycle ---

    /// Connect through a JS-implemented transport. `transport` must
    /// implement the `JsTransport` contract (see the module docs of
    /// `transport`).
    pub async fn connect(
        &self,
        transport: crate::transport::JsTransportObject,
        url: String,
        token: String,
    ) -> Result<(), wasm_bindgen::JsError> {
        let adapter = JsTransportAdapter::new(transport);
        self.inner
            .connect_with_transport(std::sync::Arc::new(adapter), &url, &token)
            .await
            .map_err(js_error_from_portal)
    }

    pub async fn disconnect(&self) -> Result<(), wasm_bindgen::JsError> {
        self.inner.disconnect().await.map_err(js_error_from_portal)
    }

    // --- Senders ---

    /// Publish state (robot only). `values` is `{fieldName: number|bool}`
    /// per the declared state schema; missing fields are omitted samples.
    /// `timestampUs` defaults to now.
    pub fn send_state(
        &self,
        values: JsValue,
        timestamp_us: Option<f64>,
    ) -> Result<(), wasm_bindgen::JsError> {
        let values = field_map_from_js(values, self.inner.state_schema())?;
        self.inner.send_state(&values, ts_option(timestamp_us)).map_err(js_error_from_portal)
    }

    /// Publish an action (operator only). Same shape as `sendState`, plus
    /// `inReplyToTsUs` — the observation timestamp this action answers,
    /// feeding end-to-end policy latency metrics.
    pub fn send_action(
        &self,
        values: JsValue,
        timestamp_us: Option<f64>,
        in_reply_to_ts_us: Option<f64>,
    ) -> Result<(), wasm_bindgen::JsError> {
        let values = field_map_from_js(values, self.inner.action_schema())?;
        self.inner
            .send_action(&values, ts_option(timestamp_us), ts_option(in_reply_to_ts_us))
            .map_err(js_error_from_portal)
    }

    /// Publish an action chunk (operator only). `data` is
    /// `{fieldName: number[]}` — columns are padded/truncated to the
    /// declared horizon.
    pub fn send_action_chunk(
        &self,
        chunk_name: &str,
        data: JsValue,
        timestamp_us: Option<f64>,
        in_reply_to_ts_us: Option<f64>,
    ) -> Result<(), wasm_bindgen::JsError> {
        let spec = self
            .inner
            .action_chunks()
            .iter()
            .find(|s| s.name == chunk_name)
            .ok_or_else(|| js_error(format!("chunk '{chunk_name}' is not declared")))?;
        let data = chunk_data_from_js(data, spec.horizon, &spec.fields)?;
        self.inner
            .send_action_chunk(chunk_name, &data, ts_option(timestamp_us), ts_option(in_reply_to_ts_us))
            .map_err(js_error_from_portal)
    }

    /// Publish one frame on a declared WebRTC video track (robot only,
    /// native robots publishing real media — in a browser deployment the
    /// robot side almost always uses `sendFrameVideo`-style byte streams
    /// or stays native). `rgb` is packed RGB24.
    pub fn send_video_frame(
        &self,
        track_name: &str,
        rgb: Vec<u8>,
        width: u32,
        height: u32,
        timestamp_us: Option<f64>,
    ) -> Result<(), wasm_bindgen::JsError> {
        self.inner
            .send_video_frame(track_name, &rgb, width, height, ts_option(timestamp_us))
            .map_err(js_error_from_portal)
    }

    /// Operator-side video ingest: push one decoded RGB frame (canvas /
    /// WebCodecs output) into Portal's sync pipeline. `rgb` is packed
    /// RGB24, `width * height * 3` bytes.
    pub fn ingest_video_frame(
        &self,
        track_name: &str,
        rgb: Vec<u8>,
        width: u32,
        height: u32,
        timestamp_us: f64,
    ) -> Result<(), wasm_bindgen::JsError> {
        self.inner
            .ingest_video_frame(track_name, rgb, width, height, ts_option(Some(timestamp_us)).unwrap_or(0))
            .map_err(js_error_from_portal)
    }

    // --- Pull API (latest-wins) ---

    pub fn get_observation(&self) -> JsValue {
        self.inner.get_observation().map(|o| observation_to_js(&o)).unwrap_or(JsValue::NULL)
    }

    pub fn get_action(&self) -> JsValue {
        self.inner.get_action().map(|a| action_to_js(&a)).unwrap_or(JsValue::NULL)
    }

    pub fn get_state(&self) -> JsValue {
        self.inner.get_state().map(|s| state_to_js(&s)).unwrap_or(JsValue::NULL)
    }

    pub fn get_video_frame(&self, track_name: String) -> JsValue {
        self.inner
            .get_video_frame(&track_name)
            .map(|f| video_frame_to_js(&f))
            .unwrap_or(JsValue::NULL)
    }

    pub fn get_action_chunk(&self, chunk_name: String) -> JsValue {
        self.inner
            .get_action_chunk(&chunk_name)
            .map(|c| action_chunk_to_js(&c))
            .unwrap_or(JsValue::NULL)
    }

    // --- Queries ---

    pub fn local_identity(&self) -> Option<String> {
        self.inner.local_identity()
    }

    pub fn active_operator(&self) -> Option<String> {
        self.inner.active_operator()
    }

    pub fn operators(&self) -> Vec<String> {
        self.inner.operators()
    }

    pub fn action_chunks(&self) -> Vec<String> {
        self.inner.action_chunks().iter().map(|s| s.name.clone()).collect()
    }

    pub fn metrics(&self) -> JsValue {
        metrics_to_js(&self.inner.metrics())
    }

    // --- Multi-controller ---

    pub async fn set_active_operator(&self, identity: Option<String>) -> Result<(), wasm_bindgen::JsError> {
        self.inner.set_active_operator(identity).await.map_err(js_error_from_portal)
    }

    // --- RPC ---

    /// Invoke a remote RPC. `destination` may be `null` to resolve the
    /// peer automatically (robot for an operator and vice versa).
    /// `responseTimeoutMs` may be `null` for the transport default.
    pub async fn perform_rpc(
        &self,
        destination: Option<String>,
        method: String,
        payload: String,
        response_timeout_ms: Option<f64>,
    ) -> Result<String, wasm_bindgen::JsError> {
        let response_timeout =
            response_timeout_ms.map(|ms| Duration::from_millis(ms.max(0.0) as u64));
        self.inner
            .perform_rpc(destination.as_deref(), &method, payload, response_timeout)
            .await
            .map_err(js_error_from_portal)
    }

    /// Register a handler for inbound RPC `method`. The JS function
    /// receives `{requestId, callerIdentity, payload, responseTimeoutMs}`
    /// and must return a promise resolving to the string result (or
    /// rejecting with `{code, message, data}`).
    pub fn register_rpc_method(
        &self,
        method: &str,
        handler: js_sys::Function,
    ) {
        let handler_fn = handler;
        let wrapped: livekit_portal_core::rpc::RpcHandler = std::sync::Arc::new(
            move |data: RpcInvocationData| {
                let handler_fn = handler_fn.clone();
                Box::pin(async move {
                    let args = serde_wasm_bindgen::to_value(&RpcInvocationJs {
                        request_id: data.request_id.clone(),
                        caller_identity: data.caller_identity.clone(),
                        payload: data.payload.clone(),
                        response_timeout_ms: data.response_timeout.as_millis() as f64,
                    })
                    .map_err(|e| {
                        RpcError::new(1500, format!("handler args serialization failed: {e}"), None)
                    })?;
                    let returned = handler_fn
                        .call1(&JsValue::NULL, &args)
                        .map_err(|e| {
                            RpcError::new(1500, format!("handler call failed: {e:?}"), None)
                        })?;
                    let result = JsFuture::from(js_sys::Promise::from(returned))
                        .await
                        .map_err(js_error_to_rpc)?;
                    Ok(result.as_string().unwrap_or_default())
                })
            },
        );
        self.inner.register_rpc_method(method, wrapped);
    }

    pub fn unregister_rpc_method(&self, method: &str) {
        self.inner.unregister_rpc_method(method);
    }

    // --- Callback registration (push API) ---

    pub fn on_observation(&self, callback: js_sys::Function) {
        self.inner.on_observation(move |obs| {
            let _ = callback.call1(&JsValue::NULL, &observation_to_js(obs));
        });
    }

    pub fn on_action(&self, callback: js_sys::Function) {
        self.inner.on_action(move |action| {
            let _ = callback.call1(&JsValue::NULL, &action_to_js(action));
        });
    }

    pub fn on_state(&self, callback: js_sys::Function) {
        self.inner.on_state(move |state| {
            let _ = callback.call1(&JsValue::NULL, &state_to_js(state));
        });
    }

    pub fn on_action_chunk(&self, chunk_name: &str, callback: js_sys::Function) {
        self.inner.on_action_chunk(chunk_name, move |chunk| {
            let _ = callback.call1(&JsValue::NULL, &action_chunk_to_js(chunk));
        });
    }

    pub fn on_video_frame(&self, track_name: &str, callback: js_sys::Function) {
        self.inner.on_video_frame(track_name, move |track_name, frame| {
            let _ = callback.call2(
                &JsValue::NULL,
                &JsString::from(track_name),
                &video_frame_to_js(frame),
            );
        });
    }

    pub fn on_drop(&self, callback: js_sys::Function) {
        self.inner.on_drop(move |dropped| {
            let arr = js_sys::Array::new();
            for state in &dropped {
                let obj = map_to_js(state, typed_value_to_js);
                let _ = arr.push(&obj);
            }
            let _ = callback.call1(&JsValue::NULL, &arr.into());
        });
    }
}

fn js_error_from_portal(error: livekit_portal_core::error::PortalError) -> wasm_bindgen::JsError {
    js_error(error.to_string())
}

fn js_error_to_rpc(value: JsValue) -> RpcError {
    match serde_wasm_bindgen::from_value::<RpcErrorJs>(value.clone()) {
        Ok(e) => RpcError::new(e.code, e.message, e.data),
        Err(_) => {
            let message = value.as_string().unwrap_or_else(|| format!("{value:?}"));
            RpcError::new(1500, message, None)
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcErrorJs {
    code: u32,
    message: String,
    data: Option<String>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct RpcInvocationJs {
    request_id: String,
    caller_identity: String,
    payload: String,
    response_timeout_ms: f64,
}