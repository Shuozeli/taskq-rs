//! Cooperative shutdown signal used by [`crate::Worker::run_with_signal`].
//!
//! Encodes the standard "watch one of N events to start draining" pattern.
//! Callers can build a signal from any future, including
//! [`tokio::signal::ctrl_c`] or a `tokio::sync::oneshot::Receiver`.

use std::pin::Pin;

use futures::future::BoxFuture;
use futures::FutureExt;

/// Owned future that, once polled to completion, signals the harness to start
/// graceful shutdown.
///
/// The harness only awaits this future once. After it resolves, the acquire
/// loop stops issuing new long-polls and the deregister sequence begins.
pub struct ShutdownSignal {
    fut: BoxFuture<'static, ()>,
}

impl ShutdownSignal {
    /// Build a signal from any future that resolves to `()` when shutdown
    /// should begin.
    ///
    /// ```no_run
    /// use taskq_worker_sdk::ShutdownSignal;
    /// let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    /// let signal = ShutdownSignal::new(async move {
    ///     let _ = rx.await;
    /// });
    /// drop(tx);
    /// drop(signal);
    /// ```
    pub fn new<F>(future: F) -> Self
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        Self {
            fut: future.boxed(),
        }
    }

    /// Wait for the underlying future to complete. Consumes the signal.
    pub async fn wait(self) {
        self.fut.await
    }
}

impl std::fmt::Debug for ShutdownSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShutdownSignal").finish()
    }
}

impl std::future::Future for ShutdownSignal {
    type Output = ();

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // Safety: ShutdownSignal is `!Unpin` only via the contained BoxFuture,
        // which is itself `Unpin`. Project through to it.
        let this = self.get_mut();
        this.fut.as_mut().poll(cx)
    }
}
