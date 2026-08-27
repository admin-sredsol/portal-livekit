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

//! Executor-agnostic background tasks.
//!
//! Core's internal loops — publisher drainers, the RTT service, Portal's
//! event loop — must spawn without assuming which executor drives them:
//! natively that is tokio, in the browser it is the JS event loop via
//! `wasm-bindgen-futures`. [`spawn`] + [`Task`] give both the shape the
//! call sites already used (fire-and-forget, store handle, `abort()`), so
//! no call site branches on the target.
//!
//! Native: [`Task`] wraps a `tokio::task::JoinHandle`; `abort` is tokio's.
//!
//! Wasm: there is no way to synchronously cancel a task on the JS event
//! loop, so `abort` sets a cancel token and wakes the task; the wrapper
//! future stops at its next poll. Teardown paths that abort also drop the
//! task's channel inputs (publishers' senders, the event receiver), so the
//! awaited work ends anyway — cancellation latency is bounded by the next
//! inbound message, not unbounded.

use std::future::Future;

#[cfg(target_arch = "wasm32")]
use std::pin::Pin;
#[cfg(target_arch = "wasm32")]
use std::sync::Arc;
#[cfg(target_arch = "wasm32")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(target_arch = "wasm32")]
use std::task::{Context, Poll, Waker};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn spawn<F>(fut: F) -> Task
where
    F: Future<Output = ()> + Send + 'static,
{
    Task { inner: Inner::Tokio(tokio::spawn(fut)) }
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn spawn<F>(fut: F) -> Task
where
    F: Future<Output = ()> + 'static,
{
    let token = Arc::new(CancelToken::default());
    let fut = CancelUnless { token: token.clone(), inner: Box::pin(fut) };
    wasm_bindgen_futures::spawn_local(fut);
    Task { inner: Inner::Wasm(token) }
}

/// Handle for a spawned background task. Dropping a `Task` does nothing;
/// call [`Task::abort`] (usually from a `Drop` impl, as the publishers do)
/// to stop the task.
pub(crate) struct Task {
    inner: Inner,
}

enum Inner {
    #[cfg(not(target_arch = "wasm32"))]
    Tokio(tokio::task::JoinHandle<()>),
    #[cfg(target_arch = "wasm32")]
    Wasm(Arc<CancelToken>),
}

impl Task {
    pub(crate) fn abort(&self) {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Inner::Tokio(handle) => handle.abort(),
            #[cfg(target_arch = "wasm32")]
            Inner::Wasm(token) => token.cancel(),
        }
    }
}

// --- Wasm cancellation plumbing (target-gated, std-only) ---

#[cfg(target_arch = "wasm32")]
#[derive(Default)]
struct CancelToken {
    cancelled: AtomicBool,
    waker: parking_lot::Mutex<Option<Waker>>,
}

#[cfg(target_arch = "wasm32")]
impl CancelToken {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
        if let Some(waker) = self.waker.lock().take() {
            waker.wake();
        }
    }
}

/// `inner` but it completes early (dropping `inner`) once the token's
/// `cancel` ran. `Arc` + `Pin<Box<_>>` fields make this `Unpin`, so the
/// `Future` impl can use `get_mut` without a pin-project dependency.
#[cfg(target_arch = "wasm32")]
struct CancelUnless {
    token: Arc<CancelToken>,
    inner: Pin<Box<dyn Future<Output = ()> + 'static>>,
}

#[cfg(target_arch = "wasm32")]
impl Future for CancelUnless {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.token.cancelled.load(Ordering::Relaxed) {
            return Poll::Ready(());
        }
        // Register before re-checking so a concurrent `cancel` between the
        // load and this store can't be lost.
        *this.token.waker.lock() = Some(cx.waker().clone());
        if this.token.cancelled.load(Ordering::Relaxed) {
            return Poll::Ready(());
        }
        this.inner.as_mut().poll(cx)
    }
}