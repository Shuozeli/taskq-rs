//! Long-lived `LISTEN` connection multiplexer for `subscribe_pending`.
//!
//! A single connection holds `LISTEN taskq_pending_<namespace>` for every
//! namespace that has live waiters, and demultiplexes incoming notifications
//! into per-waiter `tokio::sync::mpsc` channels. The transaction-side
//! `subscribe_pending` call doesn't open its own connection — it asks this
//! dispatcher for a fresh receiver and gets one back synchronously.
//!
//! ## Why a dedicated connection
//!
//! Postgres `LISTEN` is per-session: a transaction that ends loses its
//! subscriptions. Putting the listener on the per-call transaction
//! connection would mean re-`LISTEN`ing on every poll, which both wastes
//! round-trips and admits a window where a notify is missed between
//! commits. A dedicated long-lived connection plus channel demux keeps
//! ordering deterministic and lets the trait method just hand out a
//! `Receiver`.
//!
//! ## Wiring
//!
//! [`Dispatcher`] is held by [`crate::storage::PostgresStorage`] inside an
//! `Arc`. A single tokio task owns the listener's `Connection` future; it
//! polls notifications and forwards them to subscribers, removing closed
//! senders lazily as it discovers them.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures_util::stream::StreamExt;
use taskq_storage::types::WakeSignal;
use taskq_storage::{Namespace, Result, StorageError};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_postgres::tls::NoTlsStream;
use tokio_postgres::{AsyncMessage, Connection, Socket};

use crate::errors::map_db_error;

/// Channel buffer size for each per-waiter receiver. A small value is fine —
/// the consumer just needs to know "something happened" and re-runs
/// `pick_and_lock_pending` regardless of how many notifies were coalesced.
const SUBSCRIBER_BUFFER: usize = 4;

struct DispatcherState {
    /// Per-namespace fan-out: the channels currently subscribed to this
    /// namespace. Bare `Vec<Sender>` because senders are cheaply cloneable
    /// and a small linear scan is fine for v1's waiter counts.
    subscribers: HashMap<Namespace, Vec<mpsc::Sender<WakeSignal>>>,
    /// Namespaces we've already issued `LISTEN` for on the dispatcher
    /// connection. Tracked here so we never duplicate the LISTEN command.
    listening: HashSet<Namespace>,
}

/// Multiplexes Postgres `LISTEN` notifications into per-namespace channels.
pub struct Dispatcher {
    state: Mutex<DispatcherState>,
    /// Connection used to issue `LISTEN` commands. The matching driver
    /// future is owned by a background tokio task spawned at construction
    /// time and pumps notifications back into [`DispatcherState`].
    listen_client: tokio_postgres::Client,
}

impl Dispatcher {
    /// Spawn a dispatcher backed by the supplied `(Client, Connection)`
    /// pair. The dispatcher owns the connection's polling task; the caller
    /// owns the dispatcher inside the `PostgresStorage` `Arc`.
    pub fn spawn(
        client: tokio_postgres::Client,
        connection: Connection<Socket, NoTlsStream>,
    ) -> Arc<Self> {
        let dispatcher = Arc::new(Self {
            state: Mutex::new(DispatcherState {
                subscribers: HashMap::new(),
                listening: HashSet::new(),
            }),
            listen_client: client,
        });

        let dispatch_clone = Arc::clone(&dispatcher);
        tokio::spawn(async move {
            // `Connection` is consumed by `poll_fn` here. The closure owns
            // it and yields a `Stream<Item = Result<AsyncMessage, Error>>`.
            let mut conn = connection;
            let mut stream = futures_util::stream::poll_fn(move |cx| conn.poll_message(cx));

            while let Some(item) = stream.next().await {
                match item {
                    Ok(AsyncMessage::Notification(notification)) => {
                        dispatch_clone.deliver(notification.channel()).await;
                    }
                    Ok(_) => {
                        // Notice / ParameterStatus / etc. — ignore.
                    }
                    Err(_e) => {
                        // Connection died. Future minors may add reconnect
                        // logic; v1's belt-and-suspenders 500 ms poll on
                        // the CP side covers correctness while the
                        // dispatcher is re-established at restart.
                        break;
                    }
                }
            }
        });

        dispatcher
    }

    /// Subscribe to wake signals for `namespace`. Returns an `mpsc::Receiver`
    /// the caller drives directly. The returned receiver is dropped when
    /// the caller is done; the dispatcher GCs closed senders on the next
    /// notification.
    pub async fn subscribe(&self, namespace: &Namespace) -> Result<mpsc::Receiver<WakeSignal>> {
        let (tx, rx) = mpsc::channel(SUBSCRIBER_BUFFER);

        // Issue LISTEN once per namespace. This is idempotent at the
        // Postgres level too, but tracking locally saves a round-trip.
        let need_listen = {
            let mut state = self.state.lock().await;
            state
                .subscribers
                .entry(namespace.clone())
                .or_default()
                .push(tx);
            state.listening.insert(namespace.clone())
        };

        if need_listen {
            let channel = listen_channel_name(namespace);
            // `LISTEN` does not support parameter binding, so we splice
            // the channel name. Defense in depth: refuse anything that
            // isn't bare ascii alphanumerics + underscore + dot + hyphen.
            validate_channel_name(&channel)?;
            let stmt = format!("LISTEN \"{channel}\"");
            self.listen_client
                .batch_execute(&stmt)
                .await
                .map_err(map_db_error)?;
        }

        Ok(rx)
    }

    async fn deliver(&self, channel: &str) {
        let Some(namespace) = parse_namespace(channel) else {
            return;
        };
        let mut state = self.state.lock().await;
        if let Some(subs) = state.subscribers.get_mut(&namespace) {
            // Send to every live subscriber; drop closed ones in place.
            subs.retain(|sender| match sender.try_send(WakeSignal) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => true,
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            });
        }
    }
}

/// Channel name for a namespace. Mirrors the trigger in
/// `migrations/0001_initial.sql`.
pub fn listen_channel_name(namespace: &Namespace) -> String {
    format!("taskq_pending_{}", namespace.as_str())
}

fn parse_namespace(channel: &str) -> Option<Namespace> {
    channel
        .strip_prefix("taskq_pending_")
        .map(|ns| Namespace::new(ns.to_owned()))
}

fn validate_channel_name(channel: &str) -> Result<()> {
    let ok = channel
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-');
    if !ok {
        return Err(StorageError::ConstraintViolation(format!(
            "invalid LISTEN channel name: {channel}"
        )));
    }
    Ok(())
}
