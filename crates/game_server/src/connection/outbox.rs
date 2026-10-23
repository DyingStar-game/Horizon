//! Per-connection outgoing queue that never drops a message.
//!
//! Each connection gets a bounded channel (the fast path) and, behind it, a backlog
//! for whatever the channel cannot take. A client that reads slowly — a game client
//! still loading its scene while a zone entry streams thousands of objects to it —
//! used to lose every message past the channel's capacity, and a lost
//! `gorc_zone_enter` is never resent: the object stayed invisible for the whole
//! session (2026-10-23, 550 messages dropped in one second, 38 shelves and 765 crates
//! of a cargo depot missing on the client).
//!
//! Now nothing is dropped and nobody waits. A message the channel cannot take goes to
//! the backlog, and from then on every message goes there too until the writer has
//! caught up, so the client receives them all, in the order they were sent. The
//! senders (the LOD loop, GORC emits) never block on one slow client.
//!
//! A client that does not catch up is cut instead — behind for longer than
//! `SLOW_CLIENT_LIMIT`, or holding more than `MAX_BACKLOG_BYTES`. Its connection ends
//! and the client reconnects to a full zone entry; a client missing messages would
//! only ever show a broken world.

use super::ConnectionId;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Notify};
use tracing::{info, warn};

/// How long a connection may stay behind (its backlog not empty) before it is cut.
/// A game client freezes for seconds while it loads a zone; a minute is far past that.
pub const SLOW_CLIENT_LIMIT: Duration = Duration::from_secs(60);

/// How many bytes a connection's backlog may hold before it is cut, whatever its age.
pub const MAX_BACKLOG_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Default)]
struct Backlog {
    queue: VecDeque<Vec<u8>>,
    bytes: usize,
    /// When the backlog last went from empty to not empty.
    since: Option<Instant>,
    /// Messages that went through the backlog since `since`, for the drain report.
    deferred: usize,
    cut: bool,
}

#[derive(Debug)]
struct Shared {
    connection_id: ConnectionId,
    backlog: Mutex<Backlog>,
    /// Wakes the writer when the connection is cut.
    cut: Notify,
}

impl Shared {
    fn backlog(&self) -> MutexGuard<'_, Backlog> {
        // Nothing in a critical section can panic half-way, so a poisoned lock still
        // holds a consistent backlog.
        self.backlog.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// What became of a message handed to [`Outbox::push`].
#[derive(Debug, PartialEq, Eq)]
pub enum Pushed {
    /// In the channel.
    Queued,
    /// In the backlog, behind the messages sent before it.
    Deferred,
    /// The connection was cut, by this message or before it: not delivered.
    Cut,
    /// The connection's writer is gone: not delivered.
    Closed,
}

/// Sending half, one per connection (cloned freely).
#[derive(Debug, Clone)]
pub struct Outbox {
    tx: mpsc::Sender<Vec<u8>>,
    shared: Arc<Shared>,
}

/// Receiving half, owned by the connection's writer.
#[derive(Debug)]
pub struct OutboxReceiver {
    rx: mpsc::Receiver<Vec<u8>>,
    shared: Arc<Shared>,
}

/// A connection's outbox: `capacity` messages in the channel, the rest in the backlog.
pub fn outbox(connection_id: ConnectionId, capacity: usize) -> (Outbox, OutboxReceiver) {
    let (tx, rx) = mpsc::channel(capacity);
    let shared = Arc::new(Shared {
        connection_id,
        backlog: Mutex::new(Backlog::default()),
        cut: Notify::new(),
    });
    (Outbox { tx, shared: Arc::clone(&shared) }, OutboxReceiver { rx, shared })
}

impl Outbox {
    /// Queue `message` for the client without waiting.
    pub fn push(&self, message: Vec<u8>) -> Pushed {
        let mut backlog = self.shared.backlog();
        if backlog.cut {
            return Pushed::Cut;
        }
        // The channel only while the backlog is empty: a message put there while older
        // ones wait in the backlog would overtake them.
        let message = if backlog.queue.is_empty() {
            match self.tx.try_send(message) {
                Ok(()) => return Pushed::Queued,
                Err(mpsc::error::TrySendError::Closed(_)) => return Pushed::Closed,
                Err(mpsc::error::TrySendError::Full(message)) => message,
            }
        } else {
            message
        };
        let now = Instant::now();
        let since = *backlog.since.get_or_insert(now);
        if backlog.deferred == 0 {
            warn!(
                "Connection {} is not reading fast enough: its messages wait in its backlog (not dropped)",
                self.shared.connection_id
            );
        }
        backlog.bytes += message.len();
        backlog.deferred += 1;
        backlog.queue.push_back(message);
        let behind = now.duration_since(since);
        if behind > SLOW_CLIENT_LIMIT || backlog.bytes > MAX_BACKLOG_BYTES {
            backlog.cut = true;
            warn!(
                "Connection {} cut: {:.1} s behind, {} messages ({} bytes) waiting — it reconnects to a full zone entry rather than miss them",
                self.shared.connection_id,
                behind.as_secs_f64(),
                backlog.queue.len(),
                backlog.bytes
            );
            backlog.queue.clear();
            backlog.bytes = 0;
            drop(backlog);
            self.shared.cut.notify_one();
            return Pushed::Cut;
        }
        Pushed::Deferred
    }
}

impl OutboxReceiver {
    /// The next message, in the order they were pushed; `None` once the connection is
    /// cut or every sender is gone.
    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        if self.is_cut() {
            return None;
        }
        // The channel first: while the backlog holds messages nothing new enters the
        // channel, so whatever is in it is older than the backlog.
        match self.rx.try_recv() {
            Ok(message) => return Some(message),
            Err(mpsc::error::TryRecvError::Disconnected) => return None,
            Err(mpsc::error::TryRecvError::Empty) => {}
        }
        if let Some(message) = self.pop_backlog() {
            return Some(message);
        }
        tokio::select! {
            message = self.rx.recv() => message,
            _ = self.shared.cut.notified() => None,
        }
    }

    /// Completes when the connection is cut: for the writer to give up a send blocked
    /// on a client that stopped reading.
    pub async fn cut(&self) {
        if self.is_cut() {
            return;
        }
        self.shared.cut.notified().await;
    }

    fn is_cut(&self) -> bool {
        self.shared.backlog().cut
    }

    fn pop_backlog(&self) -> Option<Vec<u8>> {
        let mut backlog = self.shared.backlog();
        let message = backlog.queue.pop_front()?;
        backlog.bytes -= message.len();
        if backlog.queue.is_empty() {
            if let Some(since) = backlog.since.take() {
                info!(
                    "Connection {} caught up: {} messages went through its backlog in {:.1} s",
                    self.shared.connection_id,
                    backlog.deferred,
                    since.elapsed().as_secs_f64()
                );
            }
            backlog.deferred = 0;
        }
        Some(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(i: usize) -> Vec<u8> {
        i.to_string().into_bytes()
    }

    #[tokio::test]
    async fn overflow_is_delivered_in_order() {
        let (tx, mut rx) = outbox(1, 4);
        let pushed: Vec<Pushed> = (0..10).map(|i| tx.push(msg(i))).collect();
        assert_eq!(pushed[..4], [Pushed::Queued, Pushed::Queued, Pushed::Queued, Pushed::Queued]);
        assert!(pushed[4..].iter().all(|p| *p == Pushed::Deferred));
        // Drained half-way, a new message still goes behind the backlog.
        for i in 0..6 {
            assert_eq!(rx.recv().await, Some(msg(i)));
        }
        assert_eq!(tx.push(msg(10)), Pushed::Deferred);
        for i in 6..11 {
            assert_eq!(rx.recv().await, Some(msg(i)));
        }
        // Caught up: the channel again.
        assert_eq!(tx.push(msg(11)), Pushed::Queued);
        assert_eq!(rx.recv().await, Some(msg(11)));
    }

    #[tokio::test]
    async fn too_big_a_backlog_cuts_the_connection() {
        let (tx, mut rx) = outbox(2, 1);
        assert_eq!(tx.push(msg(0)), Pushed::Queued);
        assert_eq!(tx.push(vec![0u8; MAX_BACKLOG_BYTES + 1]), Pushed::Cut);
        assert_eq!(tx.push(msg(1)), Pushed::Cut);
        assert_eq!(rx.recv().await, None);
        rx.cut().await; // already cut: returns at once
    }

    #[tokio::test]
    async fn closed_writer_is_reported() {
        let (tx, rx) = outbox(3, 1);
        drop(rx);
        assert_eq!(tx.push(msg(0)), Pushed::Closed);
    }
}
