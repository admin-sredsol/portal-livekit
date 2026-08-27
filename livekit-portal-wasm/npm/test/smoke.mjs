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

/**
 * End-to-end smoke test: two wasm Portals — a robot and an operator —
 * talking through a mock JsTransport pair in one Node process.
 *
 * Exercises the full browser seam without a room server: connect +
 * attribute-based role classification, state publish → receive, inbound and
 * outbound RPC, the frame-video byte-stream path, and
 * `ingest_video_frame`. The same mock shapes the CI job uses; a real
 * LiveKit room swaps in `LiveKitJsTransport`.
 *
 * Run with the nodejs-target artifact: `node test/smoke.mjs` after
 * `npm run build` (see build.mjs).
 */

import assert from "node:assert/strict";
import { WasmPortal, WasmPortalConfig } from "../dist/node/livekit_portal_wasm.js";

/**
 * An in-memory room: participants attribute state, data/byte-stream
 * routing, and RPC brokering. Delivery is deferred to a macrotask so no
 * call re-enters wasm synchronously — the same relaxation a real network
 * provides.
 */
class MockRoom {
  constructor() {
    this.transports = new Map();
    this.attrs = new Map();
    this.rpcMethods = new Map(); // identity -> Set<method>
    this.requestCounter = 0;
  }

  join(identity, transport) {
    this.transports.set(identity, transport);
    this.attrs.set(identity, {});
    this.rpcMethods.set(identity, new Set());
    for (const [other, t] of this.transports) {
      if (other !== identity) {
        setTimeout(() => t.sink?.onParticipantConnected(identity, this.attrsOf(identity)), 0);
      }
    }
  }

  leave(identity) {
    const t = this.transports.get(identity);
    if (!t) return;
    this.transports.delete(identity);
    for (const [, other] of this.transports) {
      setTimeout(() => other.sink?.onParticipantDisconnected(identity), 0);
    }
  }

  setAttrs(identity, attrs) {
    const current = this.attrs.get(identity) ?? {};
    const merged = { ...current, ...attrs };
    this.attrs.set(identity, merged);
    for (const [other, t] of this.transports) {
      if (other !== identity) {
        setTimeout(() => t.sink?.onParticipantAttributesChanged(identity, { ...merged }), 0);
      }
    }
  }

  attrsOf(identity) {
    return { ...(this.attrs.get(identity) ?? {}) };
  }

  deliverData(sender, topic, payload) {
    for (const [identity, t] of this.transports) {
      if (identity !== sender) {
        t.sink?.onDataReceived(payload, topic, sender);
      }
    }
  }

  deliverBytes(sender, topic, payload) {
    for (const [identity, t] of this.transports) {
      if (identity !== sender) {
        t.sink?.onByteStream(topic, sender, payload);
      }
    }
  }

  async rpc(caller, request) {
    const target = this.transports.get(request.destination);
    if (!target || !target.sink) {
      const e = new Error(`recipient '${request.destination}' not connected`);
      e.code = 1503; // RECIPIENT_DISCONNECTED
      throw e;
    }
    const requestId = `req-${++this.requestCounter}`;
    const timeoutMs = request.responseTimeoutMs ?? 15000;
    // The peer's sink dispatches into its registered Rust handlers; an
    // unregistered method rejects with {code: 1602} from the wasm side.
    return target.sink.invokeRpcMethod(
      request.method,
      requestId,
      caller,
      request.payload,
      timeoutMs,
    );
  }
}

/** One side of a paired JsTransport. Implements the full wasm contract. */
class MockTransport {
  constructor(room, identity) {
    this.room = room;
    this.identity = identity;
    this.sink = null;
    this.connected = false;
  }

  bindEventSink(sink) {
    this.sink = sink;
  }

  async connect(_info) {
    this.connected = true;
    this.room.join(this.identity, this);
  }

  async disconnect() {
    if (!this.connected) return;
    this.connected = false;
    this.room.leave(this.identity);
  }

  async publishData(payload, topic, _reliable) {
    if (topic === null || topic === undefined) return;
    setTimeout(() => this.room.deliverData(this.identity, topic, payload), 0);
  }

  async sendBytes(payload, topic) {
    setTimeout(() => this.room.deliverBytes(this.identity, topic, payload), 0);
  }

  async setAttributes(attrs) {
    this.room.setAttrs(this.identity, attrs);
  }

  async performRpc(request) {
    return this.room.rpc(this.identity, request);
  }

  registerRpcMethod(method) {
    this.room.rpcMethods.get(this.identity).add(method);
  }

  unregisterRpcMethod(method) {
    this.room.rpcMethods.get(this.identity)?.delete(method);
  }

  localIdentity() {
    return this.connected ? this.identity : null;
  }

  localAttributes() {
    return this.connected ? this.room.attrsOf(this.identity) : {};
  }

  remoteParticipants() {
    return [...this.room.transports.keys()]
      .filter((id) => id !== this.identity)
      .map((id) => ({ identity: id, attributes: this.room.attrsOf(id) }));
  }

  startVideoReceiver(_trackName, _sink) {}

  async publishVideoFrame(_trackName, _rgb, _w, _h, _ts) {}

  async sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

const tick = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  const room = new MockRoom();
  const robotTransport = new MockTransport(room, "robot-1");
  const operatorTransport = new MockTransport(room, "op-1");

  // --- configs: identical schemas on both sides ---
  const robotConfig = new WasmPortalConfig("smoke", "robot");
  robotConfig.addStateField("j1.pos", "f32");
  robotConfig.addActionField("grip", "f32");
  robotConfig.addFrameVideoTrack("fv", "raw", 0);
  robotConfig.pingMs = 0; // no RTT pinger in the test harness

  const operatorConfig = new WasmPortalConfig("smoke", "operator");
  operatorConfig.addStateField("j1.pos", "f32");
  operatorConfig.addActionField("grip", "f32");
  operatorConfig.addFrameVideoTrack("fv", "raw", 0);
  operatorConfig.pingMs = 0;

  const robot = new WasmPortal(robotConfig);
  const operator = new WasmPortal(operatorConfig);

  // --- connect; role classification rides the lk.portal.* attributes ---
  await robot.connect(robotTransport, "mock://room", "token-robot");
  await operator.connect(operatorTransport, "mock://room", "token-op");
  await tick(30);

  assert.equal(robot.localIdentity(), "robot-1");
  assert.equal(operator.localIdentity(), "op-1");
  // The robot classified the operator from the connect snapshot + role
  // attribute; no active operator is elected until set_active_operator.
  assert.ok(robot.operators().includes("op-1"));
  assert.equal(operator.activeOperator(), undefined);

  // --- state: robot publishes, operator receives ---
  let seenState = null;
  operator.onState((state) => {
    seenState = state;
  });
  robot.sendState({ "j1.pos": 0.75 });
  await tick(30);

  assert.ok(seenState, "operator on_state never fired");
  assert.equal(seenState.values["j1.pos"], 0.75);
  assert.equal(seenState.rawValues["j1.pos"], 0.75);
  assert.ok(seenState.timestampUs > 0);

  // --- actions: operator → robot, with an inReplyTo timestamp. Actions
  // are gated on the active operator (core drops actions from anyone
  // else), so elect op-1 on the robot first; the mirrored attribute
  // propagates to the operator side. Also guards the u64→Number
  // conversion: inReplyToTsUs must be a plain Number.
  await robot.setActiveOperator("op-1");
  await tick(30);
  assert.equal(operator.activeOperator(), "op-1");

  let seenAction = null;
  robot.onAction((action) => {
    seenAction = action;
  });
  operator.sendAction({ "grip": 0.5 }, null, 123456);
  await tick(30);

  assert.ok(seenAction, "robot on_action never fired");
  assert.equal(seenAction.values["grip"], 0.5);
  assert.equal(seenAction.inReplyToTsUs, 123456);
  assert.equal(typeof seenAction.inReplyToTsUs, "number", "inReplyToTsUs must be a Number, not BigInt");

  // --- RPC: operator calls a robot method; error path checks the wire shape ---
  robot.registerRpcMethod("ping", async (data) => `pong:${data.payload}`);
  const reply = await operator.performRpc(null, "ping", "hello", null);
  assert.equal(reply, "pong:hello");

  await assert.rejects(
    () => operator.performRpc(null, "missing-method", "x", null),
    (err) => err && Number(err.code) === 1602,
  );

  // --- frame-video byte stream: robot sends, operator receives ---
  let cbFrame = null;
  operator.onVideoFrame("fv", (_name, frame) => {
    cbFrame = frame;
  });
  const rgb = new Uint8Array(2 * 2 * 3);
  for (let i = 0; i < rgb.length; i++) {
    rgb[i] = i;
  }
  robot.sendVideoFrame("fv", rgb, 2, 2, 1000);
  await tick(30);

  const received = operator.getVideoFrame("fv");
  assert.ok(received, "no frame after send_video_frame");
  assert.equal(received.width, 2);
  assert.equal(received.height, 2);
  assert.equal(received.timestampUs, 1000);
  assert.deepEqual(Array.from(received.data), Array.from(rgb));
  assert.ok(cbFrame, "on_video_frame callback never fired");

  // --- ingest_video_frame: WebRTC-style decode path feeds the same slots ---
  const rgb2 = new Uint8Array(2 * 2 * 3).fill(9);
  operator.ingestVideoFrame("fv", rgb2, 2, 2, 2000);
  const ingested = operator.getVideoFrame("fv");
  assert.equal(ingested.timestampUs, 2000);
  assert.equal(ingested.data[0], 9);

  // --- wrong-role guard on ingest ---
  assert.throws(() => robot.ingestVideoFrame("fv", rgb2, 2, 2, 3000));

  // --- metrics are aggregate Numbers, not BigInt ---
  // Per-track maps (bytes_sent etc.) are summed into one Number on the
  // wasm surface; the frame-video publish above (12 raw bytes + header)
  // must show up in the total.
  const robotMetrics = robot.metrics();
  assert.equal(robotMetrics.statesSent, 1);
  assert.ok(
    typeof robotMetrics.bytesSent === "number" && robotMetrics.bytesSent >= 12,
    `bytesSent should be a Number >= 12, got ${robotMetrics.bytesSent}`,
  );
  assert.ok(
    typeof robotMetrics.rttUsMean === "object" && robotMetrics.rttUsMean === null,
    "rttUsMean is null with the pinger disabled",
  );

  // --- teardown ---
  await operator.disconnect();
  await robot.disconnect();
  assert.equal(operatorTransport.connected, false);

  console.log("smoke test: all assertions passed");
}

main().catch((err) => {
  console.error("smoke test FAILED:", err);
  process.exit(1);
});