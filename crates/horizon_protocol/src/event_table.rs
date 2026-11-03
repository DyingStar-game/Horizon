//! Numeric ids for event names.
//!
//! A frequent event costs 2 bytes in a bitcode frame instead of its name. The server
//! gives an id to every event declared before its first client connects (client event
//! handlers registered by plugins, and the events plugins declare they send), then
//! freezes the table and sends it in [`ServerFrame::HelloAck`](crate::ServerFrame).
//! Because the table never changes after that, every client knows the same ids and a
//! frame's bytes stay the same for every recipient. An event declared later is sent by
//! name.

use crate::{EventKey, ProtocolError};
use std::collections::HashMap;
use std::sync::RwLock;

/// Event name to id table, frozen once the first client has received it.
#[derive(Debug, Default)]
pub struct EventTable {
    inner: RwLock<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    names: Vec<String>,
    ids: HashMap<String, u16>,
    frozen: bool,
}

impl EventTable {
    /// An empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Gives `name` an id if the table is still open and has room. Returns the id, or
    /// `None` if the name will travel by name.
    pub fn declare(&self, name: &str) -> Option<u16> {
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if let Some(id) = inner.ids.get(name) {
            return Some(*id);
        }
        if inner.frozen || inner.names.len() > u16::MAX as usize {
            return None;
        }
        let id = inner.names.len() as u16;
        inner.names.push(name.to_string());
        inner.ids.insert(name.to_string(), id);
        Some(id)
    }

    /// The table as sent to a client; freezes it.
    pub fn snapshot(&self) -> Vec<(u16, String)> {
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        inner.frozen = true;
        inner
            .names
            .iter()
            .enumerate()
            .map(|(id, name)| (id as u16, name.clone()))
            .collect()
    }

    /// Whether the table was sent and can no longer change.
    pub fn is_frozen(&self) -> bool {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).frozen
    }

    /// The key a frame uses for `name`: its id once the table is frozen, its name
    /// otherwise (a client could not know an id given after its handshake).
    pub fn key(&self, name: &str) -> EventKey {
        let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
        match inner.ids.get(name) {
            Some(id) if inner.frozen => EventKey::Id(*id),
            _ => EventKey::Name(name.to_string()),
        }
    }

    /// The event name a key stands for.
    pub fn resolve(&self, key: &EventKey) -> Result<String, ProtocolError> {
        match key {
            EventKey::Name(name) => Ok(name.clone()),
            EventKey::Id(id) => {
                let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
                inner
                    .names
                    .get(*id as usize)
                    .cloned()
                    .ok_or(ProtocolError::UnknownEventId(*id))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_given_until_the_table_is_sent() {
        let table = EventTable::new();
        assert_eq!(table.declare("chat:message"), Some(0));
        assert_eq!(table.declare("move"), Some(1));
        assert_eq!(table.declare("chat:message"), Some(0));

        // Before the handshake nobody knows the ids: names travel in full.
        assert_eq!(table.key("move"), EventKey::name("move"));

        assert_eq!(table.snapshot(), vec![(0, "chat:message".into()), (1, "move".into())]);
        assert!(table.is_frozen());
        assert_eq!(table.key("move"), EventKey::Id(1));

        // Declared after the handshake: by name, for every client alike.
        assert_eq!(table.declare("late"), None);
        assert_eq!(table.key("late"), EventKey::name("late"));
    }

    #[test]
    fn keys_resolve_to_names() {
        let table = EventTable::new();
        table.declare("chat:message");
        assert_eq!(table.resolve(&EventKey::Id(0)).unwrap(), "chat:message");
        assert_eq!(table.resolve(&EventKey::name("x:y")).unwrap(), "x:y");
        assert!(matches!(table.resolve(&EventKey::Id(9)), Err(ProtocolError::UnknownEventId(9))));
    }
}
