//! Message routing logic for dispatching client messages to plugins.
//!
//! This module decodes incoming client frames in the format of their listener and
//! routes them to the appropriate plugin handlers through the event system.
//!
//! * JSON listeners accept today's messages (`{namespace, event, data}` and native GORC
//!   events), parsed once, plus an optional `{"type":"hello"}` handshake.
//! * Bitcode listeners accept [`ClientFrame`]s. The first frame must be a `Hello` with
//!   a supported protocol version.

use crate::{
    connection::{ConnectionId, ConnectionManager, ListenerCompression},
    error::ServerError,
    messaging::ClientMessage,
};
use horizon_event_system::{
    current_timestamp,
    protocol::{self, json, ClientFrame, CloseCode, ServerFrame, PROTOCOL_VERSION},
    ClientConnectionRef, EventSystem, Format, GorcObjectId, ListenerProfile, Outbound, PlayerId,
    RawClientMessageEvent,
};
use std::sync::Arc;
use tracing::{debug, trace, warn};

/// What the router keeps about one connection between frames.
#[derive(Debug)]
pub struct Session {
    /// The connection the frames come from.
    pub connection_id: ConnectionId,
    /// The listener's settings.
    pub profile: ListenerProfile,
    /// Frames larger than this are rejected before decoding.
    pub max_frame_size: usize,
    /// Undecodable frames tolerated before the connection is closed.
    pub max_decode_errors: u32,
    /// Whether the client completed the handshake.
    pub handshake_done: bool,
    /// Undecodable frames received so far.
    pub decode_errors: u32,
    /// The listener's compression.
    pub compression: Arc<ListenerCompression>,
}

impl Session {
    /// A session for a new connection.
    pub fn new(connection_id: ConnectionId, profile: ListenerProfile, max_frame_size: usize, max_decode_errors: u32) -> Self {
        Self {
            connection_id,
            profile,
            max_frame_size,
            max_decode_errors,
            handshake_done: false,
            decode_errors: 0,
            compression: Arc::new(ListenerCompression::none()),
        }
    }

    /// The same session on a listener with compression.
    pub fn with_compression(mut self, compression: Arc<ListenerCompression>) -> Self {
        self.compression = compression;
        self
    }

    /// Checks the dictionary a client announces against the server's. A client may
    /// have one the server does not use; the reverse means it cannot read frames
    /// compressed with the dictionary.
    fn dictionary_mismatch(&self, client: Option<u32>) -> Option<Routed> {
        let server = self.compression.settings().dictionary_id()?;
        (client != Some(server)).then(|| {
            Routed::Close(
                CloseCode::DictionaryMismatch,
                format!("the server compresses with dictionary {server}, the client has {client:?}"),
            )
        })
    }

    /// Counts an undecodable frame; returns the close code once there are too many.
    fn decode_error(&mut self, error: impl std::fmt::Display) -> Option<(CloseCode, String)> {
        self.decode_errors += 1;
        debug!(
            "Connection {}: undecodable frame ({}/{}): {}",
            self.connection_id, self.decode_errors, self.max_decode_errors, error
        );
        (self.max_decode_errors > 0 && self.decode_errors >= self.max_decode_errors)
            .then(|| (CloseCode::TooManyDecodeErrors, CloseCode::TooManyDecodeErrors.default_reason().to_string()))
    }
}

/// The router's verdict on a frame.
#[derive(Debug, PartialEq, Eq)]
pub enum Routed {
    /// Keep the connection open.
    Continue,
    /// Close the connection with this code and reason.
    Close(CloseCode, String),
}

impl From<Option<(CloseCode, String)>> for Routed {
    fn from(close: Option<(CloseCode, String)>) -> Self {
        match close {
            Some((code, reason)) => Routed::Close(code, reason),
            None => Routed::Continue,
        }
    }
}

/// Routes one client frame to the appropriate plugin handlers.
///
/// Errors from plugins are logged, not returned: only the client's own mistakes count
/// against it, and only an unreadable stream of frames closes the connection.
pub async fn route_client_frame(
    bytes: &[u8],
    session: &mut Session,
    connection_manager: &ConnectionManager,
    horizon_event_system: &EventSystem,
) -> Routed {
    if bytes.len() > session.max_frame_size {
        return Routed::Close(CloseCode::MessageTooLarge, CloseCode::MessageTooLarge.default_reason().to_string());
    }
    // The header byte, then the body decompressed to at most max_frame_size bytes.
    let body = match session.compression.settings().unwrap(bytes, session.max_frame_size) {
        Ok(body) => body,
        Err(protocol::ProtocolError::TooLarge { .. }) => {
            return Routed::Close(CloseCode::MessageTooLarge, CloseCode::MessageTooLarge.default_reason().to_string());
        }
        Err(e) if !session.handshake_done => {
            debug!("Connection {}: first frame has no valid header: {}", session.connection_id, e);
            return Routed::Close(CloseCode::SettingsMismatch, format!("invalid frame header: {e}"));
        }
        Err(e) => return session.decode_error(e).into(),
    };
    match session.profile.format {
        Format::Json => route_json(&body, session, connection_manager, horizon_event_system).await,
        Format::Bitcode => route_bitcode(&body, session, connection_manager, horizon_event_system).await,
    }
}

async fn route_json(
    bytes: &[u8],
    session: &mut Session,
    connection_manager: &ConnectionManager,
    horizon_event_system: &EventSystem,
) -> Routed {
    // One parse: the value is then read as whichever message it is.
    let value: serde_json::Value = match serde_json::from_slice(bytes) {
        Ok(value) => value,
        Err(e) => return session.decode_error(e).into(),
    };
    if value.get("type").and_then(|t| t.as_str()) == Some("hello") {
        return json_hello(value, session, connection_manager, horizon_event_system);
    }
    session.handshake_done = true;
    match route_json_value(value, session.connection_id, connection_manager, horizon_event_system).await {
        Ok(()) => Routed::Continue,
        Err(ServerError::Network(e)) => session.decode_error(e).into(),
        Err(e) => {
            trace!("❌ Message routing error: {}", e);
            Routed::Continue
        }
    }
}

fn json_hello(
    value: serde_json::Value,
    session: &mut Session,
    connection_manager: &ConnectionManager,
    horizon_event_system: &EventSystem,
) -> Routed {
    let hello: json::Hello = match serde_json::from_value(value) {
        Ok(hello) => hello,
        Err(e) => return session.decode_error(e).into(),
    };
    if hello.protocol_version != PROTOCOL_VERSION {
        return protocol_mismatch(hello.protocol_version);
    }
    if let Some(close) = session.dictionary_mismatch(hello.dictionary_id) {
        return close;
    }
    session.handshake_done = true;
    let _ = horizon_event_system.event_table().snapshot();
    let compression = session.compression.settings();
    let ack = json::HelloAck {
        kind: json::HelloAck::TYPE,
        protocol_version: PROTOCOL_VERSION,
        format: Format::Json.as_str(),
        compression: compression.codec.as_str(),
        dictionary_id: compression.dictionary_id(),
    };
    match serde_json::to_vec(&ack) {
        Ok(bytes) => {
            connection_manager.send_frame(session.connection_id, Outbound::reliable(bytes));
        }
        Err(e) => warn!("Cannot encode hello_ack: {}", e),
    }
    Routed::Continue
}

fn protocol_mismatch(version: u16) -> Routed {
    Routed::Close(
        CloseCode::ProtocolMismatch,
        format!("protocol version {version} not supported (server speaks {PROTOCOL_VERSION})"),
    )
}

async fn route_json_value(
    value: serde_json::Value,
    connection_id: ConnectionId,
    connection_manager: &ConnectionManager,
    horizon_event_system: &EventSystem,
) -> Result<(), ServerError> {
    if value.get("type").and_then(|t| t.as_str()) == Some("gorc_event") {
        return route_native_gorc_event(value, connection_id, connection_manager, horizon_event_system).await;
    }
    let message: ClientMessage =
        serde_json::from_value(value).map_err(|e| ServerError::Network(format!("Invalid message: {e}")))?;

    let player_id = connection_manager
        .get_player_id(connection_id)
        .await
        .ok_or_else(|| ServerError::Internal("Player not found".to_string()))?;

    debug!(
        "📨 Routing message to namespace '{}' event '{}' from player {}",
        message.namespace, message.event, player_id
    );

    let name = format!("{}:{}", &message.namespace, &message.event);
    let data_bytes = message.data.to_string().into_bytes();

    // Create raw message event for plugins to handle
    let raw_event = RawClientMessageEvent {
        player_id,
        message_type: name.clone(),
        data: data_bytes.clone(),
        timestamp: current_timestamp(),
    };

    // Emit to core for routing (plugins will listen to this)
    horizon_event_system
        .emit_core("raw_client_message", &raw_event)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    // Typed handlers get the payload and the real connection directly.
    if horizon_event_system.has_typed_client_handler(&name) {
        if let Some(connection) =
            connection_ref(player_id, connection_id, Format::Json, connection_manager, horizon_event_system).await
        {
            if let Err(e) =
                horizon_event_system.dispatch_client_payload(&name, &data_bytes, Format::Json, player_id, &connection)
            {
                warn!("Typed handler for {} failed: {}", name, e);
            }
        }
    }

    // Generic routing using client-specified namespace and event with connection context
    horizon_event_system
        .emit_client_with_context(&message.namespace, &message.event, player_id, &message.data)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    // Check if this message should also be routed to GORC handlers
    // For messages that match the GORC format, also emit as GORC events
    if is_gorc_compatible_message(&message) {
        if let Err(e) = route_to_gorc_handlers(&message, player_id, horizon_event_system).await {
            // Log warning but don't fail the overall message routing
            warn!("Failed to route message to GORC handlers: {}", e);
        }
    }

    trace!(
        "✅ Routed '{}:{}' message from player {} to plugins",
        message.namespace, message.event, player_id
    );
    Ok(())
}

/// The connection of a player as handlers see it.
async fn connection_ref(
    player_id: PlayerId,
    connection_id: ConnectionId,
    format: Format,
    connection_manager: &ConnectionManager,
    horizon_event_system: &EventSystem,
) -> Option<ClientConnectionRef> {
    let sender = horizon_event_system.get_client_response_sender()?;
    let (_, remote_addr, connected_at, auth_status) =
        connection_manager.get_connection_info_by_player(player_id).await?;
    let connected_at = connected_at
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    Some(
        ClientConnectionRef::new(player_id, remote_addr, connection_id.to_string(), connected_at, auth_status, sender)
            .with_format(format),
    )
}

async fn route_bitcode(
    bytes: &[u8],
    session: &mut Session,
    connection_manager: &ConnectionManager,
    horizon_event_system: &EventSystem,
) -> Routed {
    let frame = match protocol::decode_client(bytes, session.max_frame_size) {
        Ok(frame) => frame,
        // A client whose very first frame is not bitcode does not speak this listener's
        // format at all (a JSON client on a bitcode listener): no point waiting.
        Err(e) if !session.handshake_done => {
            debug!("Connection {}: first frame is not bitcode: {}", session.connection_id, e);
            return Routed::Close(CloseCode::SettingsMismatch, "expected a bitcode Hello frame".to_string());
        }
        Err(e) => return session.decode_error(e).into(),
    };

    if !session.handshake_done {
        let ClientFrame::Hello { protocol_version, client, dictionary_id } = frame else {
            return Routed::Close(CloseCode::ProtocolMismatch, "the first frame must be Hello".to_string());
        };
        if protocol_version != PROTOCOL_VERSION {
            return protocol_mismatch(protocol_version);
        }
        if let Some(close) = session.dictionary_mismatch(dictionary_id) {
            return close;
        }
        debug!("Connection {}: bitcode client '{}'", session.connection_id, client);
        session.handshake_done = true;
        let compression = session.compression.settings();
        let ack = ServerFrame::HelloAck {
            protocol_version: PROTOCOL_VERSION,
            codec: compression.codec.bits(),
            dictionary_id: compression.dictionary_id(),
            event_table: horizon_event_system.event_table().snapshot(),
        };
        connection_manager.send_frame(session.connection_id, Outbound::reliable(protocol::encode_server(&ack)));
        return Routed::Continue;
    }

    let Some(player_id) = connection_manager.get_player_id(session.connection_id).await else {
        return Routed::Continue;
    };
    match frame {
        ClientFrame::Hello { .. } => session.decode_error("second Hello").into(),
        ClientFrame::Ping { nonce } => {
            let pong = protocol::encode_server(&ServerFrame::Pong { nonce });
            connection_manager.send_frame(session.connection_id, Outbound::reliable(pong));
            Routed::Continue
        }
        ClientFrame::Event { key, payload } => {
            let name = match horizon_event_system.event_table().resolve(&key) {
                Ok(name) => name,
                Err(e) => return session.decode_error(e).into(),
            };
            emit_raw(horizon_event_system, player_id, name.clone(), payload.clone()).await;
            if !horizon_event_system.has_typed_client_handler(&name) {
                debug!("No typed handler for bitcode event {} (on_client handlers only read JSON)", name);
                return Routed::Continue;
            }
            let Some(connection) = connection_ref(
                player_id,
                session.connection_id,
                Format::Bitcode,
                connection_manager,
                horizon_event_system,
            )
            .await
            else {
                return Routed::Continue;
            };
            match horizon_event_system.dispatch_client_payload(&name, &payload, Format::Bitcode, player_id, &connection) {
                Ok(_) => Routed::Continue,
                Err(e) => session.decode_error(e).into(),
            }
        }
        ClientFrame::GorcEvent { handle, channel, event, payload } => {
            let Some(object_id) = horizon_event_system
                .get_gorc_instances()
                .and_then(|gorc| gorc.object_by_handle(handle))
            else {
                return session.decode_error(protocol::ProtocolError::UnknownHandle(handle)).into();
            };
            let name = match horizon_event_system.event_table().resolve(&event) {
                Ok(name) => name,
                Err(e) => return session.decode_error(e).into(),
            };
            emit_raw(horizon_event_system, player_id, format!("gorc:{channel}:{name}"), payload.clone()).await;
            if let Err(e) = horizon_event_system
                .emit_gorc_client_payload(player_id, object_id, channel, &name, &payload, Format::Bitcode)
                .await
            {
                debug!("📝 No client GORC handlers found for {}:{}:{}: {}", object_id, channel, name, e);
            }
            Routed::Continue
        }
    }
}

async fn emit_raw(horizon_event_system: &EventSystem, player_id: PlayerId, message_type: String, data: Vec<u8>) {
    let raw_event = RawClientMessageEvent {
        player_id,
        message_type,
        data,
        timestamp: current_timestamp(),
    };
    if let Err(e) = horizon_event_system.emit_core("raw_client_message", &raw_event).await {
        debug!("Failed to emit raw_client_message: {}", e);
    }
}

/// Routes a native GORC event directly to the EventSystem.
///
/// Native GORC event format (JSON):
///
/// ```json
/// {"type": "gorc_event", "object_id": "GorcObjectId(<uuid>)", "channel": 0,
///  "event": "move", "data": {...}, "player_id": "..."}
/// ```
///
/// The client's `player_id` field is ignored: the connection says who sent the event.
async fn route_native_gorc_event(
    value: serde_json::Value,
    connection_id: ConnectionId,
    connection_manager: &ConnectionManager,
    horizon_event_system: &EventSystem,
) -> Result<(), ServerError> {
    #[derive(serde::Deserialize)]
    struct NativeGorcEvent {
        object_id: String,
        channel: u8,
        event: String,
        data: serde_json::Value,
    }

    let gorc_msg: NativeGorcEvent = serde_json::from_value(value)
        .map_err(|e| ServerError::Network(format!("Invalid native GORC event JSON: {e}")))?;

    let player_id = connection_manager
        .get_player_id(connection_id)
        .await
        .ok_or_else(|| ServerError::Internal("Player not found".to_string()))?;

    debug!(
        "🎯 Routing native GORC event: object_id='{}', channel={}, event='{}' from player {}",
        gorc_msg.object_id, gorc_msg.channel, gorc_msg.event, player_id
    );

    emit_raw(
        horizon_event_system,
        player_id,
        format!("gorc:{}:{}", gorc_msg.channel, gorc_msg.event),
        gorc_msg.data.to_string().into_bytes(),
    )
    .await;

    // Parse the object_id to extract GORC ID if it's in the expected format
    let gorc_id = match gorc_msg
        .object_id
        .strip_prefix("GorcObjectId(")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        Some(uuid_str) => GorcObjectId::from_str(uuid_str)
            .map_err(|e| ServerError::Network(format!("Invalid GORC object ID format: {}", e)))?,
        None => return Err(ServerError::Network("Invalid GORC object_id format".to_string())),
    };

    // Route to client-to-server GORC handlers with security validation
    match horizon_event_system
        .emit_gorc_client(player_id, gorc_id, gorc_msg.channel, &gorc_msg.event, &gorc_msg.data)
        .await
    {
        Ok(()) => debug!(
            "✅ Successfully routed client GORC event to handlers: player {} -> {}:{}:{}",
            player_id, gorc_id, gorc_msg.channel, gorc_msg.event
        ),
        // Might be no handlers registered yet
        Err(e) => warn!(
            "📝 No client GORC handlers found for {}:{}:{}: {}",
            gorc_id, gorc_msg.channel, gorc_msg.event, e
        ),
    }
    Ok(())
}

/// Checks if a client message is a GORC event.
/// 
/// A message is considered a GORC event if it contains an `instance_uuid` field
/// in its data. This provides a unified message format where the presence of
/// this field determines routing behavior.
/// 
/// # Arguments
/// 
/// * `message` - The parsed client message to check
/// 
/// # Returns
/// 
/// `true` if the message contains an instance_uuid and should be routed to GORC handlers
fn is_gorc_compatible_message(message: &ClientMessage) -> bool {
    // Simple check: if the message has an instance_uuid field, it's a GORC event
    if let Ok(data_obj) = serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(message.data.clone()) {
        return data_obj.contains_key("instance_uuid");
    }
    
    false
}

/// Routes a client message to available GORC handlers.
/// 
/// This function attempts to map client messages to GORC handler patterns
/// by interpreting the namespace and event as GORC object type and event names.
/// 
/// # Arguments
/// 
/// * `message` - The client message to route
/// * `player_id` - The player ID of the originating client
/// * `horizon_event_system` - The event system for emitting GORC events
/// 
/// # Returns
/// 
/// `Ok(())` if routing succeeded, or a `ServerError` if routing failed
async fn route_to_gorc_handlers(
    message: &ClientMessage,
    player_id: horizon_event_system::PlayerId,
    horizon_event_system: &EventSystem,
) -> Result<(), ServerError> {
    // Extract GORC parameters from the message
    let object_type = extract_object_type_from_message(message);
    let channel = extract_channel_from_message(message);
    let event_name = &message.event;
    
    debug!(
        "🔄 Attempting GORC routing: object_type='{}', channel={}, event='{}'",
        object_type, channel, event_name
    );
    
    // Extract instance_uuid and object_id from message
    let instance_uuid = extract_instance_uuid_from_message(message);
    let object_id = extract_object_id_from_message(message);
    
    // Parse the instance UUID to get the target object ID first
    if let Ok(gorc_id) = horizon_event_system::GorcObjectId::from_str(&instance_uuid) {
        // Create a proper GorcEvent structure
        let gorc_event = horizon_event_system::GorcEvent {
            object_id,
            instance_uuid,
            object_type: object_type.clone(),
            channel,
            data: serde_json::to_vec(&serde_json::json!({
                "player_id": player_id,
                "event_name": event_name,
                "original_namespace": message.namespace,
                "data": message.data,
                "timestamp": current_timestamp()
            })).unwrap_or_default(),
            priority: "Normal".to_string(),
            timestamp: current_timestamp(),
            format: Format::Json,
        };
        
        // Use the secure client-to-server GORC routing
        match horizon_event_system.emit_gorc_client(
            player_id,
            gorc_id,
            channel,
            event_name,
            &gorc_event
        ).await {
            Ok(()) => {
                debug!("✅ Successfully routed legacy client message to GORC client handlers: player {} -> {}:{}:{}", 
                    player_id, gorc_id, channel, event_name);
            }
            Err(e) => {
                // This is expected if no GORC client handlers exist for this pattern
                debug!("📝 No GORC client handlers found for {}:{}:{}: {}", gorc_id, channel, event_name, e);
            }
        }
    } else {
        debug!("❌ Invalid GORC object ID format in legacy message: {}", instance_uuid);
    }
    
    Ok(())
}

/// Extracts the object type from a client message for GORC routing.
/// 
/// # Arguments
/// 
/// * `message` - The client message to analyze
/// 
/// # Returns
/// 
/// A string representing the object type for GORC handler routing
fn extract_object_type_from_message(message: &ClientMessage) -> String {
    // First try to extract from data if it has an explicit object_type field
    if let Ok(data_obj) = serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(message.data.clone()) {
        if let Some(object_type) = data_obj.get("object_type") {
            if let Some(type_str) = object_type.as_str() {
                return type_str.to_string();
            }
        }
    }
    
    // Fallback to using the namespace as object type
    // Convert to PascalCase for consistency with GORC conventions
    capitalize_first_letter(&message.namespace)
}

/// Extracts the channel from a client message for GORC routing.
/// 
/// # Arguments
/// 
/// * `message` - The client message to analyze
/// 
/// # Returns
/// 
/// The channel number (defaults to 0 if not specified)
fn extract_channel_from_message(message: &ClientMessage) -> u8 {
    if let Ok(data_obj) = serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(message.data.clone()) {
        if let Some(channel_value) = data_obj.get("channel") {
            if let Some(channel_num) = channel_value.as_u64() {
                return (channel_num as u8).min(3); // GORC channels are 0-3
            }
        }
    }
    
    // Default to channel 0
    0
}

/// Extracts the instance_uuid from a client message for GORC routing.
/// 
/// # Arguments
/// 
/// * `message` - The client message to analyze
/// 
/// # Returns
/// 
/// A string representing the instance UUID, or generates a new one if not provided
fn extract_instance_uuid_from_message(message: &ClientMessage) -> String {
    if let Ok(data_obj) = serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(message.data.clone()) {
        if let Some(instance_uuid) = data_obj.get("instance_uuid") {
            if let Some(uuid_str) = instance_uuid.as_str() {
                return uuid_str.to_string();
            }
        }
    }
    
    // Generate a new UUID if not provided (for cases where client doesn't specify)
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    message.namespace.hash(&mut hasher);
    message.event.hash(&mut hasher);
    message.data.to_string().hash(&mut hasher);
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default().as_nanos().hash(&mut hasher);
    
    format!("gen_{:x}", hasher.finish())
}

/// Extracts the object_id from a client message for GORC routing.
/// 
/// # Arguments
/// 
/// * `message` - The client message to analyze
/// 
/// # Returns
/// 
/// A string representing the object ID, or generates one based on message content
fn extract_object_id_from_message(message: &ClientMessage) -> String {
    if let Ok(data_obj) = serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(message.data.clone()) {
        if let Some(object_id) = data_obj.get("object_id") {
            if let Some(id_str) = object_id.as_str() {
                return id_str.to_string();
            }
        }
        // Also check for entity_id as an alternative
        if let Some(entity_id) = data_obj.get("entity_id") {
            if let Some(id_str) = entity_id.as_str() {
                return id_str.to_string();
            }
        }
    }
    
    // Generate a default object_id based on namespace and event
    format!("{}_{}", message.namespace, message.event)
}

/// Capitalizes the first letter of a string for PascalCase conversion.
/// 
/// # Arguments
/// 
/// * `s` - The string to capitalize
/// 
/// # Returns
/// 
/// A new string with the first letter capitalized
fn capitalize_first_letter(s: &str) -> String {
    if s.is_empty() {
        return s.to_string();
    }
    
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
    }
}