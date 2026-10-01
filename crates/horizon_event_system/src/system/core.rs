/// Core EventSystem implementation
use crate::events::EventHandler;
use crate::gorc::instance::GorcInstanceManager;
use super::client::ClientResponseSender;
use super::stats::EventSystemStats;
use super::path_router::PathRouter;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use dashmap::DashMap;
// use smallvec::SmallVec;
use compact_str::CompactString;
use super::cache::SerializationBufferPool;
use tokio::sync::RwLock;

/// The core event system that manages event routing and handler execution.
/// 
/// This is the central hub for all event processing in the system. It provides
/// type-safe event registration and emission with support for different event
/// categories (core, client, plugin, and GORC instance events).
/// 
/// Uses DashMap for lock-free concurrent access to handlers, significantly improving
/// performance under high concurrency by eliminating reader-writer lock contention.
/// Uses SmallVec to eliminate heap allocations for the common case of 1-4 handlers per event.
pub struct EventSystem {
    /// Lock-free map of event keys to their registered handlers (optimized with SmallVec + CompactString)  
    pub(super) handlers: DashMap<CompactString, Vec<Arc<dyn EventHandler>>>,
    /// Path-based router for efficient similarity searches and hierarchical organization
    pub(super) path_router: RwLock<PathRouter>,
    /// System statistics for monitoring (kept as RwLock for atomic updates)
    pub(super) stats: tokio::sync::RwLock<EventSystemStats>,
    /// Emission counters, kept out of `stats`: every emit used to take
    /// `stats.write().await`, one async lock shared by every handler of every
    /// plugin. Polled by luminal (which re-polls a pending task at once) during a
    /// burst of a few thousand concurrent emits (genericprops startup, 400
    /// players), its waiter list became a convoy that froze every luminal worker
    /// (preprod, 2026-10-01).
    pub(super) events_emitted: AtomicU64,
    pub(super) gorc_events_emitted: AtomicU64,
    /// High-performance serialization buffer pool to reduce allocations
    pub(super) serialization_pool: SerializationBufferPool,
    /// GORC instance manager for object-specific events
    pub(super) gorc_instances: Option<Arc<GorcInstanceManager>>,
    /// Client response sender for connection-aware handlers
    pub(super) client_response_sender: Option<Arc<dyn ClientResponseSender + Send + Sync>>,
}

impl std::fmt::Debug for EventSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventSystem")
            .field("handlers", &"[handlers]")
            .field("stats", &"[stats]")
            .field("gorc_instances", &self.gorc_instances.is_some())
            .field("client_response_sender", &self.client_response_sender.is_some())
            .finish()
    }
}

impl EventSystem {
    /// Creates a new event system with no registered handlers.
    pub fn new() -> Self {
        Self {
            handlers: DashMap::new(),
            path_router: RwLock::new(PathRouter::new()),
            stats: tokio::sync::RwLock::new(EventSystemStats::default()),
            events_emitted: AtomicU64::new(0),
            gorc_events_emitted: AtomicU64::new(0),
            serialization_pool: SerializationBufferPool::default(),
            gorc_instances: None,
            client_response_sender: None,
        }
    }

    /// Creates a new event system with GORC instance manager integration
    pub fn with_gorc(gorc_instances: Arc<GorcInstanceManager>) -> Self {
        Self {
            handlers: DashMap::new(),
            path_router: RwLock::new(PathRouter::new()),
            stats: tokio::sync::RwLock::new(EventSystemStats::default()),
            events_emitted: AtomicU64::new(0),
            gorc_events_emitted: AtomicU64::new(0),
            serialization_pool: SerializationBufferPool::default(),
            gorc_instances: Some(gorc_instances),
            client_response_sender: None,
        }
    }

    /// Sets the GORC instance manager for this event system
    pub fn set_gorc_instances(&mut self, gorc_instances: Arc<GorcInstanceManager>) {
        self.gorc_instances = Some(gorc_instances);
    }

    /// Sets the client response sender for connection-aware handlers
    pub fn set_client_response_sender(&mut self, sender: Arc<dyn ClientResponseSender + Send + Sync>) {
        self.client_response_sender = Some(sender);
    }


    /// Gets the client response sender if available
    #[inline]
    pub fn get_client_response_sender(&self) -> Option<Arc<dyn ClientResponseSender + Send + Sync>> {
        self.client_response_sender.clone()
    }

    /// Gets the current event system statistics
    #[inline]
    pub async fn get_stats(&self) -> EventSystemStats {
        let mut stats = self.stats.read().await.clone();
        stats.events_emitted = self.events_emitted.load(Ordering::Relaxed);
        stats.gorc_events_emitted = self.gorc_events_emitted.load(Ordering::Relaxed);
        stats
    }
    
    /// Gets access to the GORC instances manager (if available)
    pub fn get_gorc_instances(&self) -> Option<Arc<crate::gorc::instance::GorcInstanceManager>> {
        self.gorc_instances.clone()
    }
}

impl Default for EventSystem {
    fn default() -> Self {
        Self::new()
    }
}