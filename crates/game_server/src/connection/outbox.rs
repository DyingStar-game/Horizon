//! Per-connection outgoing queue that never drops a reliable frame.
//!
//! Each connection gets a bounded channel (the fast path) and, behind it, a backlog for
//! whatever the channel cannot take. Before this, every connection shared one broadcast
//! channel of 1000 messages: a client that read slowly (a game client loading a scene
//! while a zone entry streams thousands of objects to it) lagged behind that channel and
//! was disconnected, and the frames it missed were never resent.
//!
//! Now nothing is dropped and nobody waits. A frame the channel cannot take goes to the
//! backlog, and from then on every frame goes there too until the writer has caught up,
//! so the client receives them all, in the order they were sent. Senders never block on
//! one slow client.
//!
//! A client that does not catch up is cut instead: behind for longer than
//! [`SLOW_CLIENT_LIMIT`], or holding more than [`MAX_BACKLOG_BYTES`]. Its connection ends
//! with [`CloseCode::SlowClient`](super::CloseCode::SlowClient).
//!
//! Control items (pongs, close requests) travel on a separate channel that the writer
//! serves first, so a backlog of game data never delays them.

use super::{CloseCode, ConnectionId};
use bytes::Bytes;
use horizon_event_system::Outbound;
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

/// Frames the fast-path channel holds before frames go to the backlog.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// A frame waiting in an outbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Queued {
    /// The frame.
    pub frame: Outbound,
    /// Whether the frame is ready for the wire as is (on a listener with compression:
    /// header byte added, compressed if large). Unwrapped frames are grouped and
    /// wrapped by the writer.
    pub wrapped: bool,
}

impl Queued {
    fn len(&self) -> usize {
        self.frame.len()
    }
}

/// A request for the connection's writer that skips the frame queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    /// Answer a transport-level ping.
    Pong(Bytes),
    /// Send the close code and reason, then end the connection.
    Close(CloseCode, String),
}

#[derive(Debug, Default)]
struct Backlog {
    queue: VecDeque<Queued>,
    bytes: usize,
    /// When the backlog last went from empty to not empty.
    since: Option<Instant>,
    /// Frames that went through the backlog since `since`, for the drain report.
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

/// What became of a frame handed to [`Outbox::push`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pushed {
    /// In the channel.
    Queued,
    /// In the backlog, behind the frames sent before it.
    Deferred,
    /// The connection was cut, by this frame or before it: not delivered.
    Cut,
    /// The connection's writer is gone: not delivered.
    Closed,
}

impl Pushed {
    /// Whether the frame will reach the client.
    pub fn is_accepted(self) -> bool {
        matches!(self, Pushed::Queued | Pushed::Deferred)
    }
}

/// Sending half, one per connection (cloned freely).
#[derive(Debug, Clone)]
pub struct Outbox {
    tx: mpsc::Sender<Queued>,
    control: mpsc::UnboundedSender<Control>,
    shared: Arc<Shared>,
}

/// Receiving half, owned by the connection's writer.
#[derive(Debug)]
pub struct OutboxReceiver {
    rx: mpsc::Receiver<Queued>,
    control: mpsc::UnboundedReceiver<Control>,
    shared: Arc<Shared>,
}

/// Completes when its connection is cut (see [`OutboxReceiver::cut_signal`]).
#[derive(Debug, Clone)]
pub struct CutSignal(Arc<Shared>);

impl CutSignal {
    /// Waits until the connection is cut; returns at once if it already is.
    pub async fn cut(&self) {
        loop {
            let notified = self.0.cut.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.0.backlog().cut {
                return;
            }
            notified.await;
        }
    }
}

/// What the writer should do next.
#[derive(Debug, PartialEq, Eq)]
pub enum Next {
    /// Write this control item first.
    Control(Control),
    /// Write this frame.
    Frame(Queued),
    /// The connection was cut for being too slow.
    Cut,
    /// Every sender is gone.
    Closed,
}

/// A connection's outbox: `capacity` frames in the channel, the rest in the backlog.
pub fn outbox(connection_id: ConnectionId, capacity: usize) -> (Outbox, OutboxReceiver) {
    let (tx, rx) = mpsc::channel(capacity.max(1));
    let (control_tx, control_rx) = mpsc::unbounded_channel();
    let shared = Arc::new(Shared {
        connection_id,
        backlog: Mutex::new(Backlog::default()),
        cut: Notify::new(),
    });
    (
        Outbox { tx, control: control_tx, shared: Arc::clone(&shared) },
        OutboxReceiver { rx, control: control_rx, shared },
    )
}

impl Outbox {
    /// Queue `frame` for the client without waiting; the writer wraps it for the wire.
    pub fn push(&self, frame: Outbound) -> Pushed {
        self.push_queued(Queued { frame, wrapped: false })
    }

    /// Queue a frame already wrapped for the wire.
    pub fn push_wrapped(&self, frame: Outbound) -> Pushed {
        self.push_queued(Queued { frame, wrapped: true })
    }

    fn push_queued(&self, frame: Queued) -> Pushed {
        let mut backlog = self.shared.backlog();
        if backlog.cut {
            return Pushed::Cut;
        }
        // The channel only while the backlog is empty: a frame put there while older ones
        // wait in the backlog would overtake them.
        let frame = if backlog.queue.is_empty() {
            match self.tx.try_send(frame) {
                Ok(()) => return Pushed::Queued,
                Err(mpsc::error::TrySendError::Closed(_)) => return Pushed::Closed,
                Err(mpsc::error::TrySendError::Full(frame)) => frame,
            }
        } else {
            frame
        };
        let now = Instant::now();
        let since = *backlog.since.get_or_insert(now);
        if backlog.deferred == 0 {
            warn!(
                "Connection {} is not reading fast enough: its frames wait in its backlog (not dropped)",
                self.shared.connection_id
            );
        }
        backlog.bytes += frame.len();
        backlog.deferred += 1;
        backlog.queue.push_back(frame);
        let behind = now.duration_since(since);
        if behind > SLOW_CLIENT_LIMIT || backlog.bytes > MAX_BACKLOG_BYTES {
            backlog.cut = true;
            warn!(
                "Connection {} cut: {:.1} s behind, {} frames ({} bytes) waiting",
                self.shared.connection_id,
                behind.as_secs_f64(),
                backlog.queue.len(),
                backlog.bytes
            );
            backlog.queue.clear();
            backlog.bytes = 0;
            drop(backlog);
            self.shared.cut.notify_waiters();
            return Pushed::Cut;
        }
        Pushed::Deferred
    }

    /// Hand a control item to the writer, ahead of any queued frame. Returns `false` if
    /// the writer is gone.
    pub fn control(&self, item: Control) -> bool {
        self.control.send(item).is_ok()
    }

    /// Whether frames are waiting in the backlog, that is, whether the client is behind.
    pub fn is_behind(&self) -> bool {
        !self.shared.backlog().queue.is_empty()
    }
}

impl OutboxReceiver {
    /// The next thing to write: control items first, then frames in the order they were
    /// pushed.
    pub async fn next(&mut self) -> Next {
        if let Ok(item) = self.control.try_recv() {
            return Next::Control(item);
        }
        // Registered before the cut flag is read, so a cut between the two still wakes us.
        let shared = Arc::clone(&self.shared);
        let cut = shared.cut.notified();
        tokio::pin!(cut);
        cut.as_mut().enable();
        if let Some(next) = self.try_frame() {
            return next;
        }
        tokio::select! {
            biased;
            item = self.control.recv() => match item {
                Some(item) => Next::Control(item),
                // Control senders live as long as frame senders.
                None => Next::Closed,
            },
            _ = cut => Next::Cut,
            frame = self.rx.recv() => match frame {
                Some(frame) => Next::Frame(frame),
                None => Next::Closed,
            },
        }
    }

    /// A handle that completes when the connection is cut, for the writer to give up a
    /// write blocked on a client that stopped reading.
    pub fn cut_signal(&self) -> CutSignal {
        CutSignal(Arc::clone(&self.shared))
    }

    /// The next frame if one is ready now, for the writer to batch what is already
    /// queued into one flush. Control items are not returned here.
    pub fn try_next_frame(&mut self) -> Option<Queued> {
        match self.try_frame()? {
            Next::Frame(frame) => Some(frame),
            _ => None,
        }
    }

    /// Whether a control item is waiting.
    pub fn has_control(&self) -> bool {
        !self.control.is_empty()
    }

    fn try_frame(&mut self) -> Option<Next> {
        if self.is_cut() {
            return Some(Next::Cut);
        }
        // The channel first: while the backlog holds frames nothing new enters the
        // channel, so whatever is in it is older than the backlog.
        match self.rx.try_recv() {
            Ok(frame) => return Some(Next::Frame(frame)),
            Err(mpsc::error::TryRecvError::Disconnected) => return Some(Next::Closed),
            Err(mpsc::error::TryRecvError::Empty) => {}
        }
        self.pop_backlog().map(Next::Frame)
    }

    fn is_cut(&self) -> bool {
        self.shared.backlog().cut
    }

    fn pop_backlog(&self) -> Option<Queued> {
        let mut backlog = self.shared.backlog();
        let frame = backlog.queue.pop_front()?;
        backlog.bytes -= frame.len();
        if backlog.queue.is_empty() {
            if let Some(since) = backlog.since.take() {
                info!(
                    "Connection {} caught up: {} frames went through its backlog in {:.1} s",
                    self.shared.connection_id,
                    backlog.deferred,
                    since.elapsed().as_secs_f64()
                );
            }
            backlog.deferred = 0;
        }
        Some(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(i: usize) -> Outbound {
        Outbound::reliable(i.to_string().into_bytes())
    }

    async fn next_frame(rx: &mut OutboxReceiver) -> Option<Outbound> {
        match rx.next().await {
            Next::Frame(queued) => Some(queued.frame),
            _ => None,
        }
    }

    #[tokio::test]
    async fn overflow_is_delivered_in_order() {
        let (tx, mut rx) = outbox(1, 4);
        let pushed: Vec<Pushed> = (0..10).map(|i| tx.push(frame(i))).collect();
        assert_eq!(pushed[..4], [Pushed::Queued; 4]);
        assert!(pushed[4..].iter().all(|p| *p == Pushed::Deferred));
        assert!(tx.is_behind());
        // Drained half-way, a new frame still goes behind the backlog.
        for i in 0..6 {
            assert_eq!(next_frame(&mut rx).await, Some(frame(i)));
        }
        assert_eq!(tx.push(frame(10)), Pushed::Deferred);
        for i in 6..11 {
            assert_eq!(next_frame(&mut rx).await, Some(frame(i)));
        }
        // Caught up: the channel again.
        assert!(!tx.is_behind());
        assert_eq!(tx.push(frame(11)), Pushed::Queued);
        assert_eq!(next_frame(&mut rx).await, Some(frame(11)));
    }

    #[tokio::test]
    async fn control_items_skip_the_queue() {
        let (tx, mut rx) = outbox(1, 2);
        for i in 0..5 {
            tx.push(frame(i));
        }
        assert!(tx.control(Control::Pong(Bytes::from_static(b"p"))));
        assert!(rx.has_control());
        assert_eq!(rx.next().await, Next::Control(Control::Pong(Bytes::from_static(b"p"))));
        assert_eq!(next_frame(&mut rx).await, Some(frame(0)));
    }

    #[tokio::test]
    async fn wrapped_frames_keep_their_flag() {
        let (tx, mut rx) = outbox(1, 4);
        tx.push(frame(0));
        tx.push_wrapped(frame(1));
        assert_eq!(rx.try_next_frame(), Some(Queued { frame: frame(0), wrapped: false }));
        assert_eq!(rx.try_next_frame(), Some(Queued { frame: frame(1), wrapped: true }));
    }

    #[tokio::test]
    async fn try_next_frame_drains_what_is_ready() {
        let (tx, mut rx) = outbox(1, 2);
        for i in 0..5 {
            tx.push(frame(i));
        }
        let drained: Vec<Outbound> = std::iter::from_fn(|| rx.try_next_frame()).map(|q| q.frame).collect();
        assert_eq!(drained, (0..5).map(frame).collect::<Vec<_>>());
        assert_eq!(rx.try_next_frame(), None);
    }

    #[tokio::test]
    async fn too_big_a_backlog_cuts_the_connection() {
        let (tx, mut rx) = outbox(2, 1);
        assert_eq!(tx.push(frame(0)), Pushed::Queued);
        assert_eq!(tx.push(Outbound::reliable(vec![0u8; MAX_BACKLOG_BYTES + 1])), Pushed::Cut);
        assert_eq!(tx.push(frame(1)), Pushed::Cut);
        assert!(!Pushed::Cut.is_accepted());
        assert_eq!(rx.next().await, Next::Cut);
        rx.cut_signal().cut().await; // already cut: returns at once
    }

    #[tokio::test]
    async fn cut_signal_wakes_a_waiting_writer() {
        let (tx, rx) = outbox(5, 1);
        let signal = rx.cut_signal();
        let waiter = tokio::spawn(async move { signal.cut().await });
        tokio::task::yield_now().await;
        tx.push(frame(0));
        tx.push(Outbound::reliable(vec![0u8; MAX_BACKLOG_BYTES + 1]));
        tokio::time::timeout(Duration::from_secs(1), waiter).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn closed_writer_is_reported() {
        let (tx, rx) = outbox(3, 1);
        drop(rx);
        assert_eq!(tx.push(frame(0)), Pushed::Closed);
        assert!(!tx.control(Control::Close(CloseCode::Kicked, String::new())));
    }

    #[tokio::test]
    async fn dropped_senders_close_the_receiver() {
        let (tx, mut rx) = outbox(4, 1);
        drop(tx);
        assert_eq!(rx.next().await, Next::Closed);
    }
}
