//! The zone grid must not change what a move reports: the transitions of a player's
//! move and of an object's move are checked against a sweep of everything, on a
//! random world with every size of zone (tiered and unbounded) and teleports.

use crate::gorc::channels::{CompressionType, ReplicationLayer, ReplicationPriority};
use crate::gorc::instance::{GorcInstanceManager, GorcObject, GorcObjectId};
use crate::types::{PlayerId, Vec3};
use std::any::Any;
use std::collections::HashSet;

#[derive(Debug, Clone)]
struct Sized {
    position: Vec3,
    radius: f64,
}

impl GorcObject for Sized {
    fn type_name(&self) -> &str {
        "Sized"
    }
    fn position(&self) -> Vec3 {
        self.position
    }
    fn get_priority(&self, _observer_pos: Vec3) -> ReplicationPriority {
        ReplicationPriority::High
    }
    fn serialize_for_layer(&self, _layer: &ReplicationLayer) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        Ok(Vec::new())
    }
    fn get_layers(&self) -> Vec<ReplicationLayer> {
        vec![
            ReplicationLayer::new(0, self.radius, 30.0, vec!["position".to_string()], CompressionType::None),
            ReplicationLayer::new(1, self.radius * 0.5, 10.0, vec!["name".to_string()], CompressionType::None),
        ]
    }
    fn update_position(&mut self, new_position: Vec3) {
        self.position = new_position;
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn clone_object(&self) -> Box<dyn GorcObject> {
        Box::new(self.clone())
    }
}

/// Deterministic xorshift, so a failure replays.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
    fn range(&mut self, half: f64) -> f64 {
        (self.next() * 2.0 - 1.0) * half
    }
    fn near(&mut self, center: Vec3, half: f64) -> Vec3 {
        Vec3::new(center.x + self.range(half), center.y + self.range(half), center.z + self.range(half))
    }
}

const RADII: [f64; 7] = [150.0, 203.0, 300.0, 5_000.0, 10_001.0, 200_001.0, 2e9];

struct World {
    gorc: GorcInstanceManager,
    objects: Vec<(GorcObjectId, Vec3, f64)>,
    /// Position of each player at its last sweep: what the manager compares against.
    players: Vec<(PlayerId, Vec3)>,
    rng: Rng,
}

impl World {
    async fn new(objects: usize, players: usize) -> Self {
        let gorc = GorcInstanceManager::new();
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let far = Vec3::new(1e9, 0.0, 0.0);
        let mut placed = Vec::new();
        for i in 0..objects {
            let radius = RADII[i % RADII.len()];
            let position = if i % 5 == 0 { rng.near(far, 20_000.0) } else { rng.near(Vec3::zero(), 20_000.0) };
            let id = gorc.register_object(Sized { position, radius }, position).await;
            placed.push((id, position, radius));
        }
        let mut tracked = Vec::new();
        for _ in 0..players {
            let player = PlayerId::new();
            let position = rng.near(Vec3::zero(), 20_000.0);
            gorc.add_player(player, position).await;
            tracked.push((player, position));
        }
        Self { gorc, objects: placed, players: tracked, rng }
    }

    fn step(&mut self, from: Vec3) -> Vec3 {
        match (self.rng.next() * 20.0) as u32 {
            0 => self.rng.near(Vec3::zero(), 30_000.0),        // teleport
            1 => self.rng.near(Vec3::new(1e9, 0.0, 0.0), 20_000.0), // to the other world
            2..=4 => self.rng.near(from, 400.0),
            _ => self.rng.near(from, 4.0),
        }
    }
}

fn layers(radius: f64) -> [(u8, f64); 2] {
    [(0, radius), (1, radius * 0.5)]
}

#[tokio::test]
async fn player_moves_report_what_a_full_sweep_reports() {
    let mut world = World::new(1_400, 12).await;
    let mut transitions = 0;
    for i in 0..3_000 {
        let index = i % world.players.len();
        let (player, old) = world.players[index];
        let new = world.step(old);
        let (entries, exits) = world.gorc.update_player_position(player, new).await;

        let mut expected_entries = HashSet::new();
        let mut expected_exits = HashSet::new();
        if old.distance_squared(new) >= 4.0 {
            world.players[index].1 = new;
            for (id, position, radius) in &world.objects {
                for (channel, r) in layers(*radius) {
                    let was = old.distance_squared(*position) <= r * r;
                    let is = new.distance_squared(*position) <= r * r;
                    if !was && is {
                        expected_entries.insert((*id, channel));
                    } else if was && !is {
                        expected_exits.insert((*id, channel));
                    }
                }
            }
        }
        assert_eq!(entries.iter().copied().collect::<HashSet<_>>(), expected_entries, "entries, move {}", i);
        assert_eq!(exits.iter().copied().collect::<HashSet<_>>(), expected_exits, "exits, move {}", i);
        assert_eq!(entries.len(), expected_entries.len(), "no duplicate entry, move {}", i);
        transitions += entries.len() + exits.len();
    }
    assert!(transitions > 500, "the walk crossed zones: {}", transitions);
}

#[tokio::test]
async fn object_moves_report_what_a_full_sweep_reports() {
    let mut world = World::new(300, 60).await;
    // Subscriptions in a state the moves can change.
    for index in 0..world.players.len() {
        let (player, position) = world.players[index];
        world.gorc.subscribe_player_to_existing_objects(player, position).await;
    }
    let mut transitions = 0;
    for i in 0..3_000 {
        let index = i % world.objects.len();
        let (id, old, radius) = world.objects[index];
        let new = world.step(old);
        let before = world.gorc.get_object(id).await.unwrap();

        let mut expected = HashSet::new();
        for (player, position) in &world.players {
            for (channel, r) in layers(radius) {
                let was = position.distance_squared(old) <= r * r;
                let is = position.distance_squared(new) <= r * r;
                let subscribed = before.is_subscribed(channel, *player);
                match (was, is, subscribed) {
                    (false, true, false) => { expected.insert((*player, channel, true)); }
                    (true, false, true) => { expected.insert((*player, channel, false)); }
                    _ => {}
                }
            }
        }

        let (_, _, changes) = world.gorc.update_object_position(id, new).await.unwrap();
        world.objects[index].1 = new;
        let changes_set: HashSet<_> = changes.iter().copied().collect();
        assert_eq!(changes_set, expected, "move {} of object {} (radius {})", i, id, radius);
        assert_eq!(changes.len(), expected.len(), "no duplicate change, move {}", i);
        transitions += changes.len();
    }
    assert!(transitions > 200, "the objects crossed players: {}", transitions);
}
