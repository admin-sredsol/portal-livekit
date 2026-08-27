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

//! JS-facing [`PortalConfig`] builder.
//!
//! Mirrors the UniFFI surface: scalars as getter/setter properties,
//! lists through `add*` methods. Stringly-typed enums (`role`, codec,
//! dtype) are parsed eagerly here so an invalid name errors at
//! configuration time, not at connect.

use std::collections::HashMap;

use wasm_bindgen::prelude::*;

use livekit_portal_core::codec::Codec;
use livekit_portal_core::config::{ChunkSpec, PortalConfig};
use livekit_portal_core::dtype::DType;

use crate::portal::js_error;

/// Parse the codec name used in both `add_video_track` (WebRTC codecs) and
/// `add_frame_video_track` (byte-stream codecs). Matching the UniFFI enum
/// spellings: `h264`, `vp8`, `vp9`, `av1`, `h265`, `raw`, `png`, `mjpeg`.
pub(crate) fn parse_codec(name: &str) -> Result<Codec, JsError> {
    match name.to_ascii_lowercase().as_str() {
        "h264" => Ok(Codec::H264),
        "vp8" => Ok(Codec::Vp8),
        "vp9" => Ok(Codec::Vp9),
        "av1" => Ok(Codec::Av1),
        "h265" => Ok(Codec::H265),
        "raw" => Ok(Codec::Raw),
        "png" => Ok(Codec::Png),
        "mjpeg" => Ok(Codec::Mjpeg),
        other => Err(js_error(format!(
            "unknown codec '{other}' (expected h264, vp8, vp9, av1, h265, raw, png, or mjpeg)"
        ))),
    }
}

fn parse_dtype(name: &str) -> Result<DType, JsError> {
    match name.to_ascii_lowercase().as_str() {
        "f64" => Ok(DType::F64),
        "f32" => Ok(DType::F32),
        "i32" => Ok(DType::I32),
        "i16" => Ok(DType::I16),
        "i8" => Ok(DType::I8),
        "u32" => Ok(DType::U32),
        "u16" => Ok(DType::U16),
        "u8" => Ok(DType::U8),
        "bool" => Ok(DType::Bool),
        other => Err(js_error(format!(
            "unknown dtype '{other}' (expected f64, f32, i32, i16, i8, u32, u16, u8, or bool)"
        ))),
    }
}

fn parse_role(name: &str) -> Result<livekit_portal_core::types::Role, JsError> {
    match name.to_ascii_lowercase().as_str() {
        "robot" => Ok(livekit_portal_core::types::Role::Robot),
        "operator" => Ok(livekit_portal_core::types::Role::Operator),
        other => Err(js_error(format!("unknown role '{other}' (expected robot or operator)"))),
    }
}

/// JS-facing [`PortalConfig`] builder. Scalars are properties; repeated
/// lists (tracks, schema fields, chunks) grow through `add*` calls.
///
/// ```js
/// const config = new WasmPortalConfig("teleop", "operator");
/// config.addVideoTrack("cam_left", "h264", null, false, false);
/// config.addStateField("j1.pos", "f32");
/// const portal = new WasmPortal(config);
/// ```
#[wasm_bindgen]
pub struct WasmPortalConfig {
    pub(crate) config: PortalConfig,
}

#[wasm_bindgen]
impl WasmPortalConfig {
    #[wasm_bindgen(constructor)]
    pub fn new(session: String, role: String) -> Result<WasmPortalConfig, JsError> {
        let role = parse_role(&role)?;
        Ok(WasmPortalConfig { config: PortalConfig::new(session, role) })
    }

    // --- Track declarations ---

    /// Declare a WebRTC media-path video track. `codec` is the WebRTC
    /// encoder (`h264` / `vp8` / `vp9` / `av1` / `h265`); `maxBitrateKbps`
    /// is an optional encoder ceiling.
    #[wasm_bindgen(js_name = addVideoTrack)]
    pub fn add_video_track(
        &mut self,
        name: String,
        codec: String,
        max_bitrate_kbps: Option<u32>,
        simulcast: bool,
        screencast: bool,
    ) -> Result<(), JsError> {
        let codec = parse_codec(&codec)?;
        self.config.video_tracks.push(livekit_portal_core::config::VideoTrackSpec {
            name: name.to_string(),
            codec,
            max_bitrate_kbps,
            simulcast,
            screencast,
        });
        Ok(())
    }

    /// Declare a byte-stream frame-video track (robot sends, operator
    /// receives as finished streams). `codec` is the wire codec
    /// (`raw` / `png` / `mjpeg`); `quality` is the encoder hint (0-100,
    /// ignored for `raw`).
    #[wasm_bindgen(js_name = addFrameVideoTrack)]
    pub fn add_frame_video_track(
        &mut self,
        name: String,
        codec: String,
        quality: u8,
    ) -> Result<(), JsError> {
        let codec = parse_codec(&codec)?;
        self.config.frame_video_tracks.push(livekit_portal_core::config::FrameVideoSpec {
            name,
            codec,
            quality,
        });
        Ok(())
    }

    // --- Schema fields ---

    #[wasm_bindgen(js_name = addStateField)]
    pub fn add_state_field(&mut self, name: String, dtype: String) -> Result<(), JsError> {
        self.config.state_schema.push(livekit_portal_core::config::FieldSpec {
            name,
            dtype: parse_dtype(&dtype)?,
        });
        Ok(())
    }

    #[wasm_bindgen(js_name = addActionField)]
    pub fn add_action_field(&mut self, name: String, dtype: String) -> Result<(), JsError> {
        self.config.action_schema.push(livekit_portal_core::config::FieldSpec {
            name,
            dtype: parse_dtype(&dtype)?,
        });
        Ok(())
    }

    /// Declare an action chunk: `name`, the per-column timestep count
    /// `horizon`, and `fields` — an array of `{name, dtype}` objects (or
    /// `[name, dtype]` pairs).
    #[wasm_bindgen(js_name = addActionChunk)]
    pub fn add_action_chunk(
        &mut self,
        name: String,
        horizon: u32,
        fields: JsValue,
    ) -> Result<(), JsError> {
        let fields: Vec<(String, String)> = serde_wasm_bindgen::from_value(fields)
            .map_err(|e| js_error(format!("fields must be an array of [name, dtype] pairs: {e}")))?;
        let fields = fields
            .into_iter()
            .map(|(name, dtype)| {
                Ok(livekit_portal_core::config::FieldSpec {
                    name,
                    dtype: parse_dtype(&dtype)?,
                })
            })
            .collect::<Result<Vec<_>, JsError>>()?;
        self.config.action_chunks.push(ChunkSpec { name, horizon, fields });
        Ok(())
    }

    // --- Scalar knobs (getter/setter properties) ---

    #[wasm_bindgen(getter = stateReliable)]
    pub fn state_reliable(&self) -> bool {
        self.config.state_reliable
    }

    #[wasm_bindgen(setter = stateReliable)]
    pub fn set_state_reliable(&mut self, value: bool) {
        self.config.state_reliable = value;
    }

    #[wasm_bindgen(getter = actionReliable)]
    pub fn action_reliable(&self) -> bool {
        self.config.action_reliable
    }

    #[wasm_bindgen(setter = actionReliable)]
    pub fn set_action_reliable(&mut self, value: bool) {
        self.config.action_reliable = value;
    }

    #[wasm_bindgen(getter = reuseStaleFrames)]
    pub fn reuse_stale_frames(&self) -> bool {
        self.config.reuse_stale_frames
    }

    #[wasm_bindgen(setter = reuseStaleFrames)]
    pub fn set_reuse_stale_frames(&mut self, value: bool) {
        self.config.reuse_stale_frames = value;
    }

    /// Operator-side subscribe-to-actions switch (recorders, shadow eval).
    #[wasm_bindgen(getter = actionSubscription)]
    pub fn action_subscription(&self) -> bool {
        self.config.action_subscription
    }

    #[wasm_bindgen(setter = actionSubscription)]
    pub fn set_action_subscription(&mut self, value: bool) {
        self.config.action_subscription = value;
    }

    /// Sync target framerate (Hz).
    #[wasm_bindgen(getter = fps)]
    pub fn fps(&self) -> u32 {
        self.config.fps
    }

    #[wasm_bindgen(setter = fps)]
    pub fn set_fps(&mut self, value: u32) {
        self.config.fps = value;
    }

    #[wasm_bindgen(getter = slack)]
    pub fn slack(&self) -> u32 {
        self.config.slack
    }

    #[wasm_bindgen(setter = slack)]
    pub fn set_slack(&mut self, value: u32) {
        self.config.slack = value;
    }

    /// Sync tolerance as a multiple of the frame interval.
    #[wasm_bindgen(getter = tolerance)]
    pub fn tolerance(&self) -> f32 {
        self.config.tolerance
    }

    #[wasm_bindgen(setter = tolerance)]
    pub fn set_tolerance(&mut self, value: f32) {
        self.config.tolerance = value;
    }

    /// RTT ping interval in milliseconds; 0 disables the pinger.
    #[wasm_bindgen(getter = pingMs)]
    pub fn ping_ms(&self) -> f64 {
        self.config.ping_ms as f64
    }

    #[wasm_bindgen(setter = pingMs)]
    pub fn set_ping_ms(&mut self, value: f64) {
        self.config.ping_ms = value.max(0.0) as u64;
    }

    /// Optional E2EE shared key (GCM). Key management itself is the JS
    /// transport's job (livekit-js E2EE config); this key only configures
    /// core's payloads.
    #[wasm_bindgen(getter = sharedKey)]
    pub fn shared_key(&self) -> Option<Vec<u8>> {
        self.config.shared_key.clone()
    }

    #[wasm_bindgen(setter = sharedKey)]
    pub fn set_shared_key(&mut self, value: Option<Vec<u8>>) {
        self.config.shared_key = value;
    }
}

// Re-exported for the portal module's schema-driven value conversion.
pub(crate) fn field_map_from_js(
    values: JsValue,
    schema: &[livekit_portal_core::config::FieldSpec],
) -> Result<HashMap<String, livekit_portal_core::types::TypedValue>, JsError> {
    let obj = js_sys::Object::try_from(&values)
        .ok_or_else(|| js_error("values must be an object of {fieldName: value}"))?;
    let mut out = HashMap::new();
    for spec in schema {
        let v = js_sys::Reflect::get(obj, &js_sys::JsString::from(spec.name.as_str()))
            .map_err(|e| js_error(format!("failed to read field '{}': {e:?}", spec.name)))?;
        // Missing keys are allowed: partial updates carry forward on the
        // receive side (actions) / read as absent on state.
        if v.is_undefined() {
            continue;
        }
        let tv = match spec.dtype {
            DType::Bool => livekit_portal_core::types::TypedValue::Bool(
                v.as_bool().ok_or_else(|| {
                    js_error(format!("field '{}' expects a boolean", spec.name))
                })?,
            ),
            _ => livekit_portal_core::types::TypedValue::from_f64(
                v.as_f64().ok_or_else(|| {
                    js_error(format!("field '{}' expects a number", spec.name))
                })?,
                spec.dtype,
            ),
        };
        out.insert(spec.name.clone(), tv);
    }
    Ok(out)
}