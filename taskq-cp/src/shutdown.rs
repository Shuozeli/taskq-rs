//! Graceful shutdown signal plumbing.
//!
//! Listens for SIGTERM / SIGINT (Ctrl-C) and broadcasts the request via a
//! `tokio::sync::watch` channel. Every component that needs to drain
//! (the gRPC server, long-poll waiters, reaper tasks) clones the receiver
//! and races it against its own work future.
//!
//! See `design.md` §13 (graceful shutdown is part of v1) and `tasks.md` §5.8.
//! The 60s hard deadline mentioned in §5.8 is enforced by callers; this
//! module only owns the signal and the broadcast.

use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;

/// Broadcast channel sender. Held by `main`; dropping it after the gRPC
/// server returns ensures any lingering watcher exits cleanly.
pub type ShutdownSender = watch::Sender<bool>;

/// Cloneable receiver. `false` means "keep going", `true` means "drain and
/// exit". Components hold one each.
pub type ShutdownReceiver = watch::Receiver<bool>;

/// Build a fresh shutdown channel and the sender side. The receiver is
/// cloned into every component that needs to react to shutdown.
pub fn channel() -> (ShutdownSender, ShutdownReceiver) {
    watch::channel(false)
}

/// Spawn the OS-signal listener. Returns a future that resolves to `()` the
/// first time SIGINT or SIGTERM arrives. Callers typically do
/// `wait_for_shutdown(rx).await` from a dedicated task that flips the
/// `ShutdownSender` once the future returns.
pub async fn wait_for_signal() {
    // SIGINT (Ctrl-C in dev) and SIGTERM (Kubernetes / systemd / docker stop).
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(handle) => handle,
        Err(err) => {
            tracing::error!(error = %err, "failed to install SIGTERM handler; falling back to SIGINT only");
            tokio::signal::ctrl_c().await.ok();
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("SIGINT received; initiating graceful shutdown");
        }
        _ = sigterm.recv() => {
            tracing::info!("SIGTERM received; initiating graceful shutdown");
        }
    }
}

/// Future-style helper — waits until the receiver observes a `true` value.
/// Use inside `tokio::select!` arms in long-running tasks (waiters,
/// reapers).
pub async fn wait_for_shutdown(mut rx: ShutdownReceiver) {
    // First, see if it's already true (broadcast happened before we were
    // spawned).
    if *rx.borrow() {
        return;
    }
    // Then wait for the next change.
    while rx.changed().await.is_ok() {
        if *rx.borrow() {
            return;
        }
    }
    // The sender was dropped — treat as shutdown.
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn wait_for_shutdown_returns_when_signal_fires() {
        // Arrange
        let (tx, rx) = channel();

        // Act
        let waiter = tokio::spawn(wait_for_shutdown(rx));
        tokio::time::sleep(Duration::from_millis(10)).await;
        tx.send(true).unwrap();

        // Assert
        tokio::time::timeout(Duration::from_millis(500), waiter)
            .await
            .expect("wait_for_shutdown did not return after broadcast")
            .unwrap();
    }

    #[tokio::test]
    async fn wait_for_shutdown_returns_when_sender_dropped() {
        // Arrange
        let (tx, rx) = channel();

        // Act
        let waiter = tokio::spawn(wait_for_shutdown(rx));
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(tx);

        // Assert
        tokio::time::timeout(Duration::from_millis(500), waiter)
            .await
            .expect("wait_for_shutdown did not return after sender drop")
            .unwrap();
    }

    #[tokio::test]
    async fn wait_for_shutdown_returns_immediately_if_already_set() {
        // Arrange
        let (tx, rx) = channel();
        tx.send(true).unwrap();

        // Act / Assert
        tokio::time::timeout(Duration::from_millis(50), wait_for_shutdown(rx))
            .await
            .expect("expected immediate return when value is already true");
    }
}
