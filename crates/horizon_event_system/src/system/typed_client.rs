//! Client event handlers that receive the payload in the client's own format.
//!
//! Unlike [`on_client`](EventSystem::on_client), these handlers are called directly by
//! the server's router, with the sender's `PlayerId` and its real connection, and the
//! payload exactly as the client sent it. There is no JSON wrapper on the way, and no
//! player id recovered from the payload: the connection says who sent it.

use super::client::ClientConnectionRef;
use super::core::EventSystem;
use crate::events::EventError;
use crate::types::PlayerId;
use horizon_protocol::{payload, Format};
use std::sync::Arc;
use tracing::info;

/// A handler for one client event, given the raw payload and its format.
pub(crate) type RawClientHandler =
    Arc<dyn Fn(&[u8], Format, PlayerId, ClientConnectionRef) -> Result<(), EventError> + Send + Sync>;

fn decode_error(format: Format, event: &str, e: impl std::fmt::Display) -> EventError {
    EventError::HandlerExecution(format!("Invalid {format} payload for {event}: {e}"))
}

impl EventSystem {
    /// Declares an event name sent to or received from clients, so that bitcode frames
    /// carry it as a 2-byte id instead of its name. Only takes effect before the first
    /// client connects (see [`horizon_protocol::EventTable`]); returns the id if one was
    /// given.
    ///
    /// Client handlers registered with the `on_client_*` methods below are declared
    /// automatically. Plugins declare the GORC events they emit from their `init`.
    pub fn declare_client_event(&self, name: &str) -> Option<u16> {
        self.event_table.declare(name)
    }

    /// The table of event ids sent to bitcode clients.
    pub fn event_table(&self) -> &horizon_protocol::EventTable {
        &self.event_table
    }

    /// Registers a handler for a client event that works with every listener format.
    /// The payload is decoded through serde: from JSON for JSON clients, from bitcode's
    /// serde mode for bitcode clients.
    pub async fn on_client_typed<T, F>(&self, namespace: &str, event: &str, handler: F) -> Result<(), EventError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
        F: Fn(T, PlayerId, ClientConnectionRef) -> Result<(), EventError> + Send + Sync + 'static,
    {
        let name = format!("{namespace}:{event}");
        let label = name.clone();
        self.register_raw_client_handler(
            &name,
            Arc::new(move |bytes, format, player, conn| {
                let value: T = payload::decode_serde(format, bytes).map_err(|e| decode_error(format, &label, e))?;
                handler(value, player, conn)
            }),
        )
    }

    /// Like [`on_client_typed`](Self::on_client_typed), with the bitcode derive for
    /// bitcode clients: for hot types, which must derive both serde and bitcode.
    pub async fn on_client_packed<T, F>(&self, namespace: &str, event: &str, handler: F) -> Result<(), EventError>
    where
        T: serde::de::DeserializeOwned + horizon_protocol::bitcode::DecodeOwned + Send + 'static,
        F: Fn(T, PlayerId, ClientConnectionRef) -> Result<(), EventError> + Send + Sync + 'static,
    {
        let name = format!("{namespace}:{event}");
        let label = name.clone();
        self.register_raw_client_handler(
            &name,
            Arc::new(move |bytes, format, player, conn| {
                let value: T = payload::decode_packed(format, bytes).map_err(|e| decode_error(format, &label, e))?;
                handler(value, player, conn)
            }),
        )
    }

    /// Registers a handler for a client event from bitcode clients only, for types that
    /// derive bitcode but not serde. JSON clients sending this event get a decode error.
    pub async fn on_client_bin<T, F>(&self, namespace: &str, event: &str, handler: F) -> Result<(), EventError>
    where
        T: horizon_protocol::bitcode::DecodeOwned + Send + 'static,
        F: Fn(T, PlayerId, ClientConnectionRef) -> Result<(), EventError> + Send + Sync + 'static,
    {
        let name = format!("{namespace}:{event}");
        let label = name.clone();
        self.register_raw_client_handler(
            &name,
            Arc::new(move |bytes, format, player, conn| {
                if format != Format::Bitcode {
                    return Err(decode_error(format, &label, "this event only accepts bitcode"));
                }
                let value: T = horizon_protocol::bitcode::decode(bytes).map_err(|e| decode_error(format, &label, e))?;
                handler(value, player, conn)
            }),
        )
    }

    /// Registers a handler that receives the payload bytes and their format, for
    /// plugins that decode on their own.
    pub async fn on_client_raw<F>(&self, namespace: &str, event: &str, handler: F) -> Result<(), EventError>
    where
        F: Fn(&[u8], Format, PlayerId, ClientConnectionRef) -> Result<(), EventError> + Send + Sync + 'static,
    {
        self.register_raw_client_handler(&format!("{namespace}:{event}"), Arc::new(handler))
    }

    fn register_raw_client_handler(&self, name: &str, handler: RawClientHandler) -> Result<(), EventError> {
        self.declare_client_event(name);
        self.typed_client_handlers
            .entry(name.into())
            .or_default()
            .push(handler);
        info!("📝 Registered typed client handler for {}", name);
        Ok(())
    }

    /// Whether any typed handler is registered for `namespace:event`.
    pub fn has_typed_client_handler(&self, name: &str) -> bool {
        self.typed_client_handlers.contains_key(name)
    }

    /// Calls the typed handlers of `name` (`namespace:event`) with a client's payload.
    /// Returns how many handlers ran; the first error is returned after every handler
    /// has run.
    pub fn dispatch_client_payload(
        &self,
        name: &str,
        payload: &[u8],
        format: Format,
        player_id: PlayerId,
        connection: &ClientConnectionRef,
    ) -> Result<usize, EventError> {
        let Some(handlers) = self.typed_client_handlers.get(name).map(|h| h.clone()) else {
            return Ok(0);
        };
        let mut first_error = None;
        for handler in &handlers {
            if let Err(e) = handler(payload, format, player_id, connection.clone()) {
                first_error.get_or_insert(e);
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(handlers.len()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system::client::ClientResponseSender;
    use crate::types::AuthenticationStatus;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct NoopSender;

    impl ClientResponseSender for NoopSender {
        fn send_to_client(&self, _: PlayerId, _: Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
        fn is_connection_active(&self, _: PlayerId) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>> {
            Box::pin(async { true })
        }
        fn get_auth_status(&self, _: PlayerId) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<AuthenticationStatus>> + Send + '_>> {
            Box::pin(async { None })
        }
        fn kick(&self, _: PlayerId, _: Option<String>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize, horizon_protocol::bitcode::Encode, horizon_protocol::bitcode::Decode)]
    struct Chat {
        text: String,
    }

    fn connection(player: PlayerId, format: Format) -> ClientConnectionRef {
        ClientConnectionRef::new(
            player,
            "127.0.0.1:1".parse().unwrap(),
            "1".into(),
            0,
            AuthenticationStatus::default(),
            Arc::new(NoopSender),
        )
        .with_format(format)
    }

    #[tokio::test]
    async fn typed_handlers_decode_both_formats_and_get_the_real_player() {
        let events = EventSystem::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        events
            .on_client_typed("chat", "message", move |msg: Chat, player, conn| {
                sink.lock().unwrap().push((msg, player, conn.format()));
                Ok(())
            })
            .await
            .unwrap();

        let player = PlayerId::new();
        let chat = Chat { text: "hi".into() };
        for format in Format::ALL {
            let bytes = payload::encode_serde(format, &chat).unwrap();
            let ran = events
                .dispatch_client_payload("chat:message", &bytes, format, player, &connection(player, format))
                .unwrap();
            assert_eq!(ran, 1);
        }
        let seen = seen.lock().unwrap();
        assert_eq!(*seen, vec![(chat.clone(), player, Format::Json), (chat, player, Format::Bitcode)]);
    }

    #[tokio::test]
    async fn packed_and_bin_handlers_use_the_derive() {
        let events = EventSystem::new();
        let count = Arc::new(Mutex::new(0));
        for kind in ["packed", "bin"] {
            let count = count.clone();
            let handler = move |msg: Chat, _: PlayerId, _: ClientConnectionRef| {
                assert_eq!(msg.text, "x");
                *count.lock().unwrap() += 1;
                Ok(())
            };
            if kind == "packed" {
                events.on_client_packed("t", kind, handler).await.unwrap();
            } else {
                events.on_client_bin("t", kind, handler).await.unwrap();
            }
        }
        let player = PlayerId::new();
        let conn = connection(player, Format::Bitcode);
        let bytes = horizon_protocol::bitcode::encode(&Chat { text: "x".into() });
        events.dispatch_client_payload("t:packed", &bytes, Format::Bitcode, player, &conn).unwrap();
        events.dispatch_client_payload("t:bin", &bytes, Format::Bitcode, player, &conn).unwrap();
        assert_eq!(*count.lock().unwrap(), 2);

        // JSON is refused by a bitcode-only handler.
        let json = serde_json::to_vec(&Chat { text: "x".into() }).unwrap();
        let conn = connection(player, Format::Json);
        assert!(events.dispatch_client_payload("t:bin", &json, Format::Json, player, &conn).is_err());
    }

    #[tokio::test]
    async fn bad_payloads_are_errors_and_unknown_events_run_nothing() {
        let events = EventSystem::new();
        events
            .on_client_typed("chat", "message", |_: Chat, _, _| Ok(()))
            .await
            .unwrap();
        let player = PlayerId::new();
        let conn = connection(player, Format::Bitcode);
        assert!(events.dispatch_client_payload("chat:message", &[0xff], Format::Bitcode, player, &conn).is_err());
        assert_eq!(events.dispatch_client_payload("nope:nope", &[], Format::Bitcode, player, &conn).unwrap(), 0);
        // Registering declared the event in the table.
        assert_eq!(events.event_table().snapshot(), vec![(0, "chat:message".to_string())]);
    }
}
