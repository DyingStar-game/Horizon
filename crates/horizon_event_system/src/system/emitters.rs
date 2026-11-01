/// Event emission methods
use crate::events::{Event, EventError};
use crate::gorc::instance::GorcObjectId;
use crate::{PlayerId, Vec3};
use super::core::EventSystem;
use super::stats::{DetailedEventSystemStats, HandlerCategoryStats};
use futures::{self, stream::{FuturesUnordered, StreamExt}};
use horizon_protocol::{json, Format, ServerFrame};
use crate::system::client::{Delivery, Outbound};
use std::sync::atomic::Ordering;
use tracing::{debug, error, info, warn};
use compact_str::CompactString;


impl EventSystem {
    /// Emits a core server event to all registered handlers.
    #[inline]
    pub async fn emit_core<T>(&self, event_name: &str, event: &T) -> Result<(), EventError>
    where
        T: Event,
    {
        let event_key = CompactString::new_inline("core:") + event_name;
        self.emit_event(&event_key, event).await
    }

    /// Emits a client event to all registered handlers.
    #[inline]
    pub async fn emit_client<T>(
        &self,
        namespace: &str,
        event_name: &str,
        event: &T,
    ) -> Result<(), EventError>
    where
        T: Event,
    {
        let event_key = CompactString::new_inline("client:") + namespace + ":" + event_name;
        self.emit_event(&event_key, event).await
    }

    /// Emits a client event with connection context for connection-aware handlers.
    /// 
    /// This method wraps the event data with player context information, allowing
    /// connection-aware handlers to respond directly to the originating client.
    /// 
    /// # Arguments
    /// 
    /// * `namespace` - The client event namespace
    /// * `event_name` - The specific event name
    /// * `player_id` - The player ID of the client that triggered the event
    /// * `event` - The event data
    pub async fn emit_client_with_context<T>(
        &self,
        namespace: &str,
        event_name: &str,
        player_id: crate::types::PlayerId,
        event: &T,
    ) -> Result<(), EventError>
    where
        T: Event + serde::Serialize,
    {
        // Create a wrapper that includes the player context
        let context_event = serde_json::json!({
            "player_id": player_id,
            "data": event
        });
        
        let event_key = CompactString::new_inline("client:") + namespace + ":" + event_name;
        self.emit_event(&event_key, &context_event).await
    }

    /// Emits a plugin event to all registered handlers.
    #[inline]
    pub async fn emit_plugin<T>(
        &self,
        plugin_name: &str,
        event_name: &str,
        event: &T,
    ) -> Result<(), EventError>
    where
        T: Event,
    {
        let event_key = CompactString::new_inline("plugin:") + plugin_name + ":" + event_name;
        self.emit_event(&event_key, event).await
    }

    /// Emits a GORC instance event for a specific object instance.
    /// 
    /// This is the new API for emitting events that target specific object instances.
    /// The event will only be delivered to handlers that are registered for this
    /// specific object type, channel, and event name.
    /// 
    /// # Arguments
    /// 
    /// * `object_id` - The specific object instance to emit the event for
    /// * `channel` - Replication channel for the event
    /// * `event_name` - Name of the specific event
    /// * `event` - The event data to emit
    /// 
    /// # Examples
    /// 
    /// ```rust,no_run
    /// use horizon_event_system::{EventSystem, GorcEvent, current_timestamp, GorcObjectId, Dest};
    /// use serde::{Serialize, Deserialize};
    /// use std::sync::Arc;
    /// 
    /// #[derive(Serialize, Deserialize, Debug, Clone)]
    /// struct PositionUpdate {
    ///     x: f32,
    ///     y: f32,
    ///     z: f32,
    /// }
    /// 
    /// async fn emit_example() -> Result<(), Box<dyn std::error::Error>> {
    ///     let events = Arc::new(EventSystem::new());
    ///     let asteroid_id = GorcObjectId::new();
    ///     let position_update = PositionUpdate { x: 100.0, y: 200.0, z: 300.0 };
    ///     
    ///     // Emit a position update for a specific asteroid instance
    ///     events.emit_gorc_instance(
    ///         asteroid_id,
    ///         0,
    ///         "position_update",
    ///         &position_update,
    ///         Dest::Both
    ///     ).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn emit_gorc_instance<T>(
        &self,
        object_id: GorcObjectId,
        channel: u8,
        event_name: &str,
        event: &T,
        dest: crate::events::Dest,
    ) -> Result<(), EventError>
    where
        T: Event + serde::Serialize,
    {
        use crate::events::Dest;
        
        // Handle None destination early
        if dest == Dest::None {
            return Ok(());
        }
        
        // Get the object instance to determine its type and position
        if let Some(ref gorc_instances) = self.gorc_instances {
            if let Some(instance) = gorc_instances.get_object(object_id).await {
                let object_type = &instance.type_name;
                
                // Handle Server or Both destinations - emit to server-side handlers
                if dest == Dest::Server || dest == Dest::Both {
                    let instance_key = CompactString::new_inline("gorc_instance:") + object_type + ":" + &channel.to_string() + ":" + event_name;
                    
                    // Wrap the event data in a GorcEvent structure for server-side handlers
                    let gorc_event = crate::events::GorcEvent {
                        object_id: object_id.to_string(),
                        instance_uuid: object_id.to_string(),
                        object_type: object_type.clone(),
                        channel,
                        data: Event::serialize(event)?,
                        priority: "Normal".to_string(),
                        timestamp: crate::utils::current_timestamp(),
                        format: Format::Json,
                    };
                    
                    // Emit to instance-specific handlers only
                    if let Err(e) = self.emit_event(&instance_key, &gorc_event).await {
                        warn!("Failed to emit instance event: {}", e);
                    }
                }
                
                // Handle Client or Both destinations - emit to subscribed clients
                if dest == Dest::Client || dest == Dest::Both {
                    self.deliver_gorc_event(object_id, channel, event_name, &|format| encode_serde_payload(format, event), None, None)
                        .await?;
                }
                
                Ok(())
            } else {
                Err(EventError::HandlerNotFound(format!("Object instance {} not found", object_id)))
            }
        } else {
            Err(EventError::HandlerExecution("GORC instance manager not available".to_string()))
        }
    }
    
    /// Emits a GORC instance event with an explicit delivery class (`None`: the
    /// channel's default, see [`ReplicationLayer::delivery`]).
    ///
    /// Each subscriber gets the event in its listener's format; the payload is encoded
    /// at most once per format in use (bitcode through serde).
    ///
    /// [`ReplicationLayer::delivery`]: crate::ReplicationLayer::delivery
    pub async fn emit_gorc_instance_typed<T>(
        &self,
        object_id: GorcObjectId,
        channel: u8,
        event_name: &str,
        event: &T,
        dest: crate::events::Dest,
        delivery: Option<Delivery>,
    ) -> Result<(), EventError>
    where
        T: Event + serde::Serialize,
    {
        self.emit_gorc_instance_with(object_id, channel, event_name, event, dest, delivery, &|format| {
            encode_serde_payload(format, event)
        })
        .await
    }

    /// Like [`emit_gorc_instance_typed`](Self::emit_gorc_instance_typed), with the
    /// bitcode derive for bitcode subscribers: for hot types.
    pub async fn emit_gorc_instance_packed<T>(
        &self,
        object_id: GorcObjectId,
        channel: u8,
        event_name: &str,
        event: &T,
        dest: crate::events::Dest,
        delivery: Option<Delivery>,
    ) -> Result<(), EventError>
    where
        T: Event + serde::Serialize + horizon_protocol::bitcode::Encode,
    {
        self.emit_gorc_instance_with(object_id, channel, event_name, event, dest, delivery, &|format| {
            horizon_protocol::payload::encode_packed(format, event)
                .map_err(|e| EventError::HandlerExecution(e.to_string()))
        })
        .await
    }

    /// Sends an already encoded payload to the subscribers of an object's channel that
    /// use `format`. Subscribers on another format are skipped, with a warning and a
    /// count ([`EventSystem::skipped_format_mismatches`]). Server-side handlers are not
    /// called: they need a typed event.
    pub async fn emit_gorc_instance_bytes(
        &self,
        object_id: GorcObjectId,
        channel: u8,
        event_name: &str,
        payload: bytes::Bytes,
        format: Format,
        delivery: Option<Delivery>,
    ) -> Result<usize, EventError> {
        self.deliver_gorc_event(object_id, channel, event_name, &|_| Ok(payload.to_vec()), delivery, Some(format))
            .await
    }

    /// Subscribers skipped so far by [`emit_gorc_instance_bytes`] because they use
    /// another format.
    ///
    /// [`emit_gorc_instance_bytes`]: EventSystem::emit_gorc_instance_bytes
    pub fn skipped_format_mismatches(&self) -> u64 {
        self.skipped_format_mismatch.load(Ordering::Relaxed)
    }

    #[allow(clippy::too_many_arguments)]
    async fn emit_gorc_instance_with<T>(
        &self,
        object_id: GorcObjectId,
        channel: u8,
        event_name: &str,
        event: &T,
        dest: crate::events::Dest,
        delivery: Option<Delivery>,
        encode: &(dyn Fn(Format) -> Result<Vec<u8>, EventError> + Sync),
    ) -> Result<(), EventError>
    where
        T: Event + serde::Serialize,
    {
        use crate::events::Dest;
        if dest == Dest::None {
            return Ok(());
        }
        if dest == Dest::Server || dest == Dest::Both {
            let gorc_instances = self.gorc_instances.as_ref().ok_or_else(|| {
                EventError::HandlerExecution("GORC instance manager not available".to_string())
            })?;
            let object_type = gorc_instances
                .get_object_type(object_id)
                .await
                .ok_or_else(|| EventError::HandlerNotFound(format!("Object instance {} not found", object_id)))?;
            let instance_key = CompactString::new_inline("gorc_instance:") + &object_type + ":" + &channel.to_string() + ":" + event_name;
            // Server-side handlers deserialize a GorcEvent, as in emit_gorc_instance.
            let gorc_event = crate::events::GorcEvent {
                object_id: object_id.to_string(),
                instance_uuid: object_id.to_string(),
                object_type: object_type.clone(),
                channel,
                data: Event::serialize(event)?,
                priority: "Normal".to_string(),
                timestamp: crate::utils::current_timestamp(),
                format: Format::Json,
            };
            if let Err(e) = self.emit_event(&instance_key, &gorc_event).await {
                warn!("Failed to emit instance event: {}", e);
            }
        }
        if dest == Dest::Client || dest == Dest::Both {
            self.deliver_gorc_event(object_id, channel, event_name, encode, delivery, None).await?;
        }
        Ok(())
    }

    /// The listener format of each player (JSON when the sender does not know).
    async fn formats_of(&self, players: Vec<PlayerId>) -> Vec<(PlayerId, Format)> {
        let Some(sender) = self.client_response_sender.as_ref() else {
            return players.into_iter().map(|p| (p, Format::Json)).collect();
        };
        let mut out = Vec::with_capacity(players.len());
        for player in players {
            let format = sender
                .connection_profile(player)
                .await
                .map(|profile| profile.format)
                .unwrap_or_default();
            out.push((player, format));
        }
        out
    }

    /// Delivers an event to the subscribers of an object's channel, each in its own
    /// format. `only` restricts delivery to the subscribers of one format. Returns how
    /// many subscribers it was queued for.
    async fn deliver_gorc_event(
        &self,
        object_id: GorcObjectId,
        channel: u8,
        event_name: &str,
        encode: &(dyn Fn(Format) -> Result<Vec<u8>, EventError> + Sync),
        delivery: Option<Delivery>,
        only: Option<Format>,
    ) -> Result<usize, EventError> {
        let sender = self.client_response_sender.as_ref().ok_or_else(|| {
            EventError::HandlerExecution("Client response sender not configured for GORC emission".to_string())
        })?;
        let gorc_instances = self.gorc_instances.as_ref().ok_or_else(|| {
            EventError::HandlerExecution("GORC instance manager not available".to_string())
        })?;
        let instance = gorc_instances.get_object(object_id).await.ok_or_else(|| {
            EventError::HandlerNotFound(format!("Object instance {} not found", object_id))
        })?;
        let layer = instance.cached_layers.iter().find(|l| l.channel == channel).ok_or_else(|| {
            EventError::HandlerExecution(format!("Channel {} not defined for object {}", channel, object_id))
        })?;
        let delivery = delivery.unwrap_or(layer.delivery);
        
        // The subscribers list is managed by zone enter/exit logic and is the authoritative source
        let subscribers = instance.get_subscribers(channel);
        debug!("📡 GORC EMIT: Object {} channel {} has {} subscribers", object_id, channel, subscribers.len());
        if subscribers.is_empty() {
            return Ok(0);
        }
        let seq = gorc_instances.next_sequence(object_id, channel);
        let timestamp = crate::utils::current_timestamp();
        
        // One encoding per format, made on first use and shared by every subscriber.
        let mut frames: [Option<Option<bytes::Bytes>>; 2] = [None, None];
        let mut sent_count = 0;
        for (player_id, format) in self.formats_of(subscribers).await {
            if only.is_some_and(|f| f != format) {
                let skipped = self.skipped_format_mismatch.fetch_add(1, Ordering::Relaxed);
                if skipped % 1000 == 0 {
                    warn!(
                        "GORC event {} of object {} not sent to player {}: payload is {} but the player's listener is {} ({} skipped so far)",
                        event_name, object_id, player_id, only.unwrap_or_default(), format, skipped + 1
                    );
                }
                continue;
            }
            let frame = frames[format.index()].get_or_insert_with(|| {
                let encoded = match format {
                    Format::Json => encode(Format::Json).and_then(|payload| {
                        json_gorc_event(&instance, event_name, channel, payload, timestamp, delivery, seq)
                    }),
                    Format::Bitcode => encode(Format::Bitcode).and_then(|payload| {
                        let handle = gorc_instances.object_handle(object_id).ok_or_else(|| {
                            EventError::HandlerExecution(format!("Object {} has no wire handle", object_id))
                        })?;
                        Ok(horizon_protocol::encode_server(&ServerFrame::GorcEvent {
                            handle,
                            channel,
                            seq,
                            event: self.event_table.key(event_name),
                            payload,
                        }))
                    }),
                };
                encoded
                    .map_err(|e| warn!("Cannot encode GORC event {} for {} clients: {}", event_name, format, e))
                    .ok()
            });
            let Some(bytes) = frame else { continue };
            let outbound = Outbound { bytes: bytes.clone(), delivery };
            if let Err(e) = sender.send_frame(player_id, outbound).await {
                // debug!, not warn!: a subscriber that went away produces one line per
                // replicated event per tick. On preprod three stale subscribers alone
                // pushed this to ~700 lines/s, which cut the pod's log retention to 97
                // seconds and made incidents impossible to investigate after the fact.
                debug!("Failed to send GORC event to player {}: {}", player_id, e);
            } else {
                sent_count += 1;
            }
        }
        
        debug!("📡 GORC: Sent {} event to {} clients on channel {} for object {}", 
               event_name, sent_count, channel, object_id);
        Ok(sent_count)
    }
    
    /// Update player position and handle zone membership changes (event-driven GORC)
    pub async fn update_player_position(&self, player_id: PlayerId, new_position: Vec3) -> Result<(), EventError> {

        debug!("🎮 UPDATE_PLAYER_POSITION called for player {} at {:?}", player_id, new_position);

        // Get the GORC instances manager
        let gorc_instances = self.gorc_instances.as_ref().ok_or_else(|| {
            EventError::HandlerExecution("GORC instance manager not available".to_string())
        })?;


        // Update position and get zone changes
        let (zone_entries, zone_exits) = gorc_instances.update_player_position(player_id, new_position).await;

        debug!("🎮 EVENT DEBUG: Got zone results - {} entries, {} exits", zone_entries.len(), zone_exits.len());

        // Handle zone entries - send zone entry messages with current layer state
        for (object_id, channel) in zone_entries {
            debug!("🎮 EVENT DEBUG: Sending zone entry message for object {} channel {}", object_id, channel);
            self.send_zone_entry_message(player_id, object_id, channel).await?;
        }

        // Handle zone exits - send zone exit messages to inform client
        for (object_id, channel) in zone_exits {
            if object_id.0 == player_id.0 {
                // Never tell a player it left its own object's zone: see send_zone_exit_message.
                continue;
            }
            debug!("🎮 EVENT DEBUG: Sending zone exit message for object {} channel {}", object_id, channel);
            self.send_zone_exit_message(player_id, object_id, channel).await?;
        }

        Ok(())
    }

    /// Subscribe a newly added player to all existing objects they are within range of.
    /// Call this AFTER add_player() and AFTER the player's own object is registered.
    /// Sends zone entry messages for all objects the player is initially inside.
    pub async fn subscribe_player_to_existing_objects(&self, player_id: PlayerId, player_position: Vec3) -> Result<(), EventError> {
        // Get the GORC instances manager
        let gorc_instances = self.gorc_instances.as_ref().ok_or_else(|| {
            EventError::HandlerExecution("GORC instance manager not available".to_string())
        })?;

        // Get zone entries for this new player against existing objects
        let zone_entries = gorc_instances.subscribe_player_to_existing_objects(player_id, player_position).await;

        info!("🆕 GORC New Player: Player {} initial zone detection - {} objects in range",
              player_id, zone_entries.len());

        // Send zone entry messages for all objects the player starts inside
        for (object_id, channel) in zone_entries {
            debug!("🎮 GORC Initial: Sending zone entry message for object {} channel {} to new player {}",
                   object_id, channel, player_id);
            self.send_zone_entry_message(player_id, object_id, channel).await?;
        }

        Ok(())
    }

    /// Update object position and handle zone membership changes for stationary players
    pub async fn update_object_position(&self, object_id: GorcObjectId, new_position: Vec3) -> Result<(), EventError> {
        // Get the GORC instances manager
        let gorc_instances = self.gorc_instances.as_ref().ok_or_else(|| {
            EventError::HandlerExecution("GORC instance manager not available".to_string())
        })?;

        // Update object position and get zone changes for all players
        if let Some((old_position, new_position, zone_changes)) = gorc_instances.update_object_position(object_id, new_position).await {
            debug!("🎯 GORC Object Movement: Object {} moved from {:?} to {:?}, {} zone changes",
                   object_id, old_position, new_position, zone_changes.len());

            // Handle zone changes caused by object movement
            for (player_id, channel, is_entry) in zone_changes {
                if is_entry {
                    debug!("🎮 GORC Object Movement: Sending zone entry message for object {} channel {} to player {}",
                           object_id, channel, player_id);
                    self.send_zone_entry_message(player_id, object_id, channel).await?;
                } else if object_id.0 != player_id.0 {
                    debug!("🎮 GORC Object Movement: Sending zone exit message for object {} channel {} to player {}",
                           object_id, channel, player_id);
                    self.send_zone_exit_message(player_id, object_id, channel).await?;
                }
            }
        }

        Ok(())
    }

    /// Notify existing players when a new GORC object is created
    pub async fn notify_players_for_new_gorc_object(&self, object_id: GorcObjectId) -> Result<(), EventError> {
        // Get the GORC instances manager
        let gorc_instances = self.gorc_instances.as_ref().ok_or_else(|| {
            EventError::HandlerExecution("GORC instance manager not available".to_string())
        })?;

        // Get zone entries for existing players
        let zone_entries = gorc_instances.notify_existing_players_for_new_object(object_id).await;

        debug!("🆕 GORC New Object: Object {} created, {} automatic zone entries",
               object_id, zone_entries.len());

        // Send zone entry messages to all affected players
        for (player_id, channel) in zone_entries {
            debug!("🎮 GORC New Object: Sending zone entry message for new object {} channel {} to player {}",
                   object_id, channel, player_id);
            self.send_zone_entry_message(player_id, object_id, channel).await?;
        }

        Ok(())
    }
    
    /// Send zone entry message with current object state for a specific layer to a player
    async fn send_zone_entry_message(&self, player_id: PlayerId, object_id: GorcObjectId, channel: u8) -> Result<(), EventError> {
        let sender = self.client_response_sender.as_ref().ok_or_else(|| {
            EventError::HandlerExecution("Client response sender not configured".to_string())
        })?;
        let gorc_instances = self.gorc_instances.as_ref().ok_or_else(|| {
            EventError::HandlerExecution("GORC instance manager not available".to_string())
        })?;
        let object_type = gorc_instances.get_object_type(object_id).await.ok_or_else(|| {
            EventError::HandlerNotFound(format!("Object instance {} not found", object_id))
        })?;
        let format = self.formats_of(vec![player_id]).await[0].1;
        
        let Some(layer_data) = gorc_instances.get_object_state_for_layer_as(object_id, channel, format).await else {
            warn!("❌ GORC: No layer data available for object {} channel {}", object_id, channel);
            return Ok(());
        };
        // ds_audio reacts to player zone entries through a core event, which stays JSON
        // whatever the client's format.
        let core_zone_data = if object_type == "player" {
            let json_state = match format {
                Format::Json => Some(layer_data.clone()),
                Format::Bitcode => gorc_instances.get_object_state_for_layer(object_id, channel).await,
            };
            Some(
                json_state
                    .and_then(|data| serde_json::from_slice::<serde_json::Value>(&data).ok())
                    .unwrap_or(serde_json::Value::Null),
            )
        } else {
            None
        };
        let data = match format {
            Format::Json => {
                let zone_entry_event = json::ZoneEnter {
                    kind: json::ZoneEnter::TYPE,
                    object_id: object_id.to_string(),
                    object_type: &object_type,
                    channel,
                    player_id: player_id.to_string(),
                    zone_data: serde_json::from_slice::<serde_json::Value>(&layer_data)
                        .unwrap_or(serde_json::Value::Null),
                    timestamp: crate::utils::current_timestamp(),
                };
                bytes::Bytes::from(serde_json::to_vec(&zone_entry_event)?)
            }
            Format::Bitcode => {
                let handle = gorc_instances.object_handle(object_id).ok_or_else(|| {
                    EventError::HandlerExecution(format!("Object {} has no wire handle", object_id))
                })?;
                horizon_protocol::encode_server(&ServerFrame::ZoneEnter {
                    handle,
                    object_id: *object_id.0.as_bytes(),
                    object_type: object_type.clone(),
                    channel,
                    zone_data: layer_data,
                })
            }
        };
        
        if let Err(e) = sender.send_frame(player_id, Outbound::reliable(data)).await {
            warn!("❌ Failed to send zone entry message to player {}: {}", player_id, e);
        } else {
            info!("🔔 GORC: Player {} entered zone {} of object {} ({})",
                  player_id, channel, object_id, object_type);
        }

        if let Some(zone_data) = core_zone_data {
            // Also broadcast a server-side core event so plugins can react to zone enter.
            let zone_entry_event = serde_json::json!({
                "type": "gorc_zone_enter",
                "object_id": object_id.to_string(),
                "object_type": object_type,
                "channel": channel,
                "player_id": player_id.to_string(),
                "zone_data": zone_data,
                "timestamp": crate::utils::current_timestamp()
            });
            if let Err(e) = self.emit_core("gorc_zone_entered", &zone_entry_event).await {
                warn!("⚠️ Failed to emit core gorc_zone_entered event: {}", e);
            }
        }
        Ok(())
    }

    /// Send zone exit message to inform player they left an object's zone.
    ///
    /// Never called for the player's own object: the observer position and the
    /// object position are updated by separate calls, so a jump of the body (a
    /// hand-over between game servers re-spawning it elsewhere) briefly puts the
    /// player out of its own zones. The client then dropped its own body and its
    /// camera fell to another avatar's (preprod, 2026-10-04).
    async fn send_zone_exit_message(&self, player_id: PlayerId, object_id: GorcObjectId, channel: u8) -> Result<(), EventError> {
        let sender = self.client_response_sender.as_ref().ok_or_else(|| {
            EventError::HandlerExecution("Client response sender not configured".to_string())
        })?;
        let gorc_instances = self.gorc_instances.as_ref().ok_or_else(|| {
            EventError::HandlerExecution("GORC instance manager not available".to_string())
        })?;
        // Graceful fallback if the object no longer exists
        let object_type = gorc_instances
            .get_object_type(object_id)
            .await
            .unwrap_or_else(|| "Unknown".to_string());
        let format = self.formats_of(vec![player_id]).await[0].1;
        
        let data = match format {
            Format::Json => {
                let zone_exit_event = json::ZoneExit {
                    kind: json::ZoneExit::TYPE,
                    object_id: object_id.to_string(),
                    object_type: &object_type,
                    channel,
                    player_id: player_id.to_string(),
                    timestamp: crate::utils::current_timestamp(),
                };
                bytes::Bytes::from(serde_json::to_vec(&zone_exit_event)?)
            }
            Format::Bitcode => match gorc_instances.object_handle(object_id) {
                Some(handle) => horizon_protocol::encode_server(&ServerFrame::ZoneExit { handle, channel }),
                None => {
                    debug!("No wire handle for removed object {}: zone exit not sent", object_id);
                    return Ok(());
                }
            },
        };
        
        if let Err(e) = sender.send_frame(player_id, Outbound::reliable(data)).await {
            warn!("❌ Failed to send zone exit message to player {}: {}", player_id, e);
        } else {
            info!("🚪 GORC: Player {} exited zone {} of object {} ({})", 
                  player_id, channel, object_id, object_type);
        }
        Ok(())
    }


    /// Routes a client message to GORC client handlers, providing security and authorization.
    /// 
    /// This method is specifically for client-initiated events targeting server objects.
    /// It emits to handlers registered with `on_gorc_client` that can validate permissions
    /// and apply security checks before modifying server state.
    pub async fn emit_gorc_client<T>(
        &self,
        client_player_id: crate::PlayerId,
        target_object_id: GorcObjectId,
        channel: u8,
        event_name: &str,
        event: &T,
    ) -> Result<(), EventError>
    where
        T: Event + serde::Serialize,
    {
        // Get the GORC instances manager
        let gorc_instances = self.gorc_instances.as_ref().ok_or_else(|| {
            EventError::HandlerExecution("GORC instance manager not available".to_string())
        })?;

        // Get the target object instance to determine its type
        if let Some(instance) = gorc_instances.get_object(target_object_id).await {
            let object_type = &instance.type_name;
            
            // Create the event key for client-to-server GORC events
            let event_key = CompactString::new_inline("gorc_client:") + object_type + ":" + &channel.to_string() + ":" + event_name;
            
            // Wrap the event with player context for the handler
            let client_event = serde_json::json!({
                "player_id": client_player_id,
                "object_id": target_object_id.to_string(),
                "object_type": object_type,
                "channel": channel,
                "data": event,
                "timestamp": crate::utils::current_timestamp()
            });

            self.emit_event(&event_key, &client_event).await
        } else {
            Err(EventError::HandlerNotFound(format!("Target object {} not found", target_object_id)))
        }
    }


    /// Routes a client's GORC event whose payload is already encoded in `format` (a
    /// bitcode client's) to the `on_gorc_client` handlers. The handlers receive it in
    /// `GorcEvent::data`, with `GorcEvent::format` saying how to decode it.
    pub async fn emit_gorc_client_payload(
        &self,
        client_player_id: crate::PlayerId,
        target_object_id: GorcObjectId,
        channel: u8,
        event_name: &str,
        payload: &[u8],
        format: horizon_protocol::Format,
    ) -> Result<(), EventError> {
        let gorc_instances = self.gorc_instances.as_ref().ok_or_else(|| {
            EventError::HandlerExecution("GORC instance manager not available".to_string())
        })?;
        let object_type = gorc_instances
            .get_object_type(target_object_id)
            .await
            .ok_or_else(|| EventError::HandlerNotFound(format!("Target object {} not found", target_object_id)))?;
        let event_key = CompactString::new_inline("gorc_client:") + &object_type + ":" + &channel.to_string() + ":" + event_name;
        let data = match format {
            horizon_protocol::Format::Json => serde_json::from_slice::<serde_json::Value>(payload)?,
            horizon_protocol::Format::Bitcode => serde_json::Value::from(payload.to_vec()),
        };
        let client_event = serde_json::json!({
            "player_id": client_player_id,
            "object_id": target_object_id.to_string(),
            "object_type": object_type,
            "channel": channel,
            "data": data,
            "format": format,
            "timestamp": crate::utils::current_timestamp()
        });
        self.emit_event(&event_key, &client_event).await
    }

    /// Broadcasts an event to all connected clients.
    /// 
    /// This method sends the event data to every client currently connected to the server.
    /// The event is serialized once and then sent to all clients for optimal performance.
    /// 
    /// # Arguments
    /// 
    /// * `event` - The event data to broadcast
    /// 
    /// # Returns
    /// 
    /// Returns `Ok(usize)` with the number of clients that received the broadcast,
    /// or `Err(EventError)` if the broadcast failed or client response sender is not configured.
    /// 
    /// # Examples
    /// 
    /// ```rust,no_run
    /// use horizon_event_system::EventSystem;
    /// use serde::{Serialize, Deserialize};
    /// use std::sync::Arc;
    /// 
    /// #[derive(Serialize, Deserialize, Debug, Clone)]
    /// struct ServerAnnouncement {
    ///     message: String,
    ///     priority: String,
    /// }
    /// 
    /// async fn example() -> Result<(), Box<dyn std::error::Error>> {
    ///     let events = Arc::new(EventSystem::new());
    ///     
    ///     // Broadcast a server announcement to all players
    ///     let announcement = ServerAnnouncement {
    ///         message: "Server maintenance in 5 minutes".to_string(),
    ///         priority: "high".to_string(),
    ///     };
    ///     
    ///     match events.broadcast(&announcement).await {
    ///         Ok(client_count) => println!("Announcement sent to {} clients", client_count),
    ///         Err(e) => println!("Broadcast failed: {}", e),
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn broadcast<T>(&self, event: &T) -> Result<usize, EventError>
    where
        T: Event + serde::Serialize,
    {
        // Check if client response sender is configured
        let sender = self.client_response_sender.as_ref().ok_or_else(|| {
            EventError::HandlerExecution("Client response sender not configured for broadcasting".to_string())
        })?;

        // Serialize the event data using our serialization pool
        let data = self.serialization_pool.serialize_event(event)?;
        
        // Convert Arc<Vec<u8>> to Vec<u8> for the broadcast method
        let broadcast_data = (*data).clone();
        
        // Send to all clients via the client response sender
        match sender.broadcast_to_all(broadcast_data).await {
            Ok(client_count) => {
                if cfg!(debug_assertions) {
                    debug!("📡 Broadcasted event to {} clients", client_count);
                }
                
                // Update stats
                let mut stats = self.stats.write().await;
                stats.events_emitted += 1;
                
                Ok(client_count)
            },
            Err(e) => {
                error!("❌ Broadcast failed: {}", e);
                Err(EventError::HandlerExecution(format!("Broadcast failed: {}", e)))
            }
        }
    }

    /// Internal emit implementation that handles the actual event dispatch.
    /// Optimized for high throughput (500k messages/sec target).
    /// Now uses lock-free DashMap + serialization pool for maximum performance.
    async fn emit_event<T>(&self, event_key: &str, event: &T) -> Result<(), EventError>
    where
        T: Event,
    {
        // Use serialization pool for better performance and shared data
        let data = self.serialization_pool.serialize_event(event)?;
        
        // Lock-free read from DashMap - no contention!
        let event_handlers = self.handlers.get(event_key).map(|entry| entry.value().clone());

        if let Some(event_handlers) = event_handlers {
            // Only log debug info if handlers exist to reduce overhead
            if event_handlers.len() > 0 {
                if cfg!(debug_assertions) {
                    debug!("📤 Emitting {} to {} handlers", event_key, event_handlers.len());
                }

                // Use FuturesUnordered for better memory efficiency and concurrency
                let mut futures = FuturesUnordered::new();
                
                for handler in event_handlers.iter() {
                    let data_arc = data.clone(); // Clone the Arc, not the data for speed
                    let handler_name = handler.handler_name();
                    let handler_clone = handler.clone();
                    
                    futures.push(async move {
                        if let Err(e) = handler_clone.handle(&data_arc).await {
                            error!("❌ Handler {} failed: {}", handler_name, e);
                        }
                    });
                }

                // Execute all handlers concurrently with better memory usage
                while let Some(_) = futures.next().await {};
            }

            // Lock-free: see `EventSystem::events_emitted`.
            self.events_emitted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if event_key.starts_with("gorc") {
                self.gorc_events_emitted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        } else {
            // Show debugging info for missing handlers (except server_tick spam)
            if event_key != "core:server_tick" && event_key != "core:raw_client_message" {
                // Use PathRouter for efficient similarity search instead of expensive linear scan
                let similar_paths = {
                    let path_router = self.path_router.read().await;
                    path_router.find_similar_paths(event_key, 5)
                };
                
                if !similar_paths.is_empty() {
                    warn!("⚠️ No handlers for event: {} (similar keys available: {:?})", event_key, similar_paths);
                } else {
                    warn!("⚠️ No handlers for event: {} (no similar handlers found)", event_key);
                }
            }
        }

        Ok(())
    }

    /// Gets detailed statistics including GORC instance information
    pub async fn get_detailed_stats(&self) -> DetailedEventSystemStats {
        let base_stats = self.get_stats().await;
        let handler_count_by_category = self.get_handler_count_by_category().await;
        
        let gorc_instance_stats = if let Some(ref gorc_instances) = self.gorc_instances {
            Some(gorc_instances.get_stats().await)
        } else {
            None
        };

        DetailedEventSystemStats {
            base: base_stats,
            handler_count_by_category,
            gorc_instance_stats,
        }
    }

    /// Gets handler count breakdown by event category using lock-free DashMap
    async fn get_handler_count_by_category(&self) -> HandlerCategoryStats {
        let mut core_handlers = 0;
        let mut client_handlers = 0;
        let mut plugin_handlers = 0;
        let mut gorc_handlers = 0;
        let mut gorc_instance_handlers = 0;

        // Lock-free iteration over DashMap
        for entry in self.handlers.iter() {
            let key = entry.key();
            let count = entry.value().len();
            
            if key.starts_with("core:") {
                core_handlers += count;
            } else if key.starts_with("client:") {
                client_handlers += count;
            } else if key.starts_with("plugin:") {
                plugin_handlers += count;
            } else if key.starts_with("gorc_instance:") {
                gorc_instance_handlers += count;
            } else if key.starts_with("gorc:") {
                gorc_handlers += count;
            }
        }

        HandlerCategoryStats {
            core_handlers,
            client_handlers,
            plugin_handlers,
            gorc_handlers,
            gorc_instance_handlers,
        }
    }
}/// A payload encoded through serde in `format`.
fn encode_serde_payload<T: serde::Serialize + ?Sized>(format: Format, event: &T) -> Result<Vec<u8>, EventError> {
    horizon_protocol::payload::encode_serde(format, event).map_err(|e| EventError::HandlerExecution(e.to_string()))
}

/// The JSON envelope of a GORC event, exactly as the server has always sent it, plus
/// `seq` on unreliable events.
fn json_gorc_event(
    instance: &crate::gorc::instance::ObjectInstance,
    event_name: &str,
    channel: u8,
    payload: Vec<u8>,
    timestamp: u64,
    delivery: Delivery,
    seq: u16,
) -> Result<bytes::Bytes, EventError> {
    let data = serde_json::value::RawValue::from_string(String::from_utf8(payload).map_err(|e| {
        EventError::HandlerExecution(format!("JSON payload is not UTF-8: {e}"))
    })?)?;
    let envelope = json::GorcEvent {
        event_type: event_name,
        object_id: instance.object_id.to_string(),
        object_type: &instance.type_name,
        channel,
        player_id: instance.object_id.to_string(),
        data: &data,
        timestamp,
        seq: (delivery == Delivery::Unreliable).then_some(seq),
    };
    Ok(bytes::Bytes::from(serde_json::to_vec(&envelope)?))
}
