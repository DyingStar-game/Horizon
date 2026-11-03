//! # Zone grid
//!
//! Where zones can reach, so the zone checks of a movement visit what is near a
//! position instead of everything.
//!
//! A player's zone sweep used to visit every object (16 ms per 2 m of movement at
//! 55 000 objects, 70 ms with the full recalculation past 5 m), and an object's move
//! every player. At 903 players on preprod (2026-11-03) that was ~13 cores for
//! 9 900 moves/s.
//!
//! Keys live in tiers of cubic cells: tier `t` has cells of `BASE_CELL * 4^t` metres.
//! An object whose largest zone radius fits a tier's cell size is kept in that tier,
//! in the cell of its position, so any point inside one of its zones lies in that
//! cell or one of its 26 neighbours. Objects with larger zones (planets, stars) are
//! kept apart and always returned. Players are kept in every tier, to answer "who is
//! within this object's largest radius" from the tier of that radius.
//!
//! Queries return candidates, a superset: the callers still check the distance.

use crate::types::Vec3;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Number of tiers.
pub const TIERS: u8 = 7;
/// Cell size of tier 0, in metres: just over the 150-203 m zones of props and players.
pub const BASE_CELL: f64 = 256.0;

/// Cell size of a tier, in metres (256 m to 1 049 km).
pub fn cell_size(tier: u8) -> f64 {
    BASE_CELL * 4f64.powi(tier as i32)
}

/// Which cells a key occupies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    /// One tier: an object whose zones all fit that tier's cell size.
    Tier(u8),
    /// Every tier: a player, found from the tier of any radius.
    Every,
    /// No tier: an object with a zone larger than the largest cell.
    Unbounded,
}

impl Reach {
    /// The reach of an object whose largest zone radius is `radius` (metres).
    pub fn of_radius(radius: f64) -> Self {
        match (0..TIERS).find(|tier| radius <= cell_size(*tier)) {
            Some(tier) => Reach::Tier(tier),
            None => Reach::Unbounded,
        }
    }

    fn covers(self, tier: u8) -> bool {
        match self {
            Reach::Tier(own) => own == tier,
            Reach::Every => true,
            Reach::Unbounded => false,
        }
    }
}

type Cell = (u8, i64, i64, i64);

fn cell_of(tier: u8, position: Vec3) -> Cell {
    let size = cell_size(tier);
    (
        tier,
        (position.x / size).floor() as i64,
        (position.y / size).floor() as i64,
        (position.z / size).floor() as i64,
    )
}

/// Keys bucketed by position, per tier (see the module documentation).
#[derive(Debug)]
pub struct ZoneGrid<K: Copy + Eq + Hash> {
    cells: DashMap<Cell, Vec<K>>,
    placed: DashMap<K, (Reach, Vec3)>,
    unbounded: DashMap<K, ()>,
    /// Keys per tier, to skip the 27 lookups of an empty tier.
    population: [AtomicUsize; TIERS as usize],
}

impl<K: Copy + Eq + Hash> Default for ZoneGrid<K> {
    fn default() -> Self {
        Self {
            cells: DashMap::new(),
            placed: DashMap::new(),
            unbounded: DashMap::new(),
            population: Default::default(),
        }
    }
}

impl<K: Copy + Eq + Hash> ZoneGrid<K> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Puts `key` at `position` with `reach`, wherever it was before.
    pub fn place(&self, key: K, position: Vec3, reach: Reach) {
        // The entry stays locked while the cells change, so two moves of one key
        // cannot interleave. Lock order is always `placed` then `cells`.
        match self.placed.entry(key) {
            Entry::Occupied(mut entry) => {
                let old = *entry.get();
                entry.insert((reach, position));
                self.relocate(key, Some(old), Some((reach, position)));
            }
            Entry::Vacant(entry) => {
                entry.insert((reach, position));
                self.relocate(key, None, Some((reach, position)));
            }
        }
    }

    /// Moves `key` to `position`, keeping its reach. Does nothing if it was never placed.
    pub fn move_to(&self, key: K, position: Vec3) {
        if let Some(mut entry) = self.placed.get_mut(&key) {
            let old = *entry;
            *entry = (old.0, position);
            self.relocate(key, Some(old), Some((old.0, position)));
        }
    }

    /// Forgets `key`.
    pub fn remove(&self, key: K) {
        if let Entry::Occupied(entry) = self.placed.entry(key) {
            self.relocate(key, Some(*entry.get()), None);
            entry.remove();
        }
    }

    /// The reach `key` was placed with.
    pub fn reach_of(&self, key: K) -> Option<Reach> {
        self.placed.get(&key).map(|entry| entry.0)
    }

    /// Pushes onto `out` every key placed by tier whose zones can contain `position`,
    /// and every unbounded key. Each key appears once.
    pub fn near(&self, position: Vec3, out: &mut Vec<K>) {
        for tier in 0..TIERS {
            if self.population[tier as usize].load(Ordering::Relaxed) > 0 {
                self.near_in_tier(position, tier, out);
            }
        }
        out.extend(self.unbounded.iter().map(|entry| *entry.key()));
    }

    /// Pushes onto `out` every key of `tier` within one cell of `position`'s: all
    /// those closer than that tier's cell size, and some farther.
    pub fn near_in_tier(&self, position: Vec3, tier: u8, out: &mut Vec<K>) {
        let (_, x, y, z) = cell_of(tier, position);
        for dx in -1..=1 {
            for dy in -1..=1 {
                for dz in -1..=1 {
                    if let Some(keys) = self.cells.get(&(tier, x + dx, y + dy, z + dz)) {
                        out.extend(keys.iter().copied());
                    }
                }
            }
        }
    }

    fn relocate(&self, key: K, old: Option<(Reach, Vec3)>, new: Option<(Reach, Vec3)>) {
        for tier in 0..TIERS {
            let before = old.filter(|(reach, _)| reach.covers(tier)).map(|(_, p)| cell_of(tier, p));
            let after = new.filter(|(reach, _)| reach.covers(tier)).map(|(_, p)| cell_of(tier, p));
            if before == after {
                continue;
            }
            if let Some(cell) = before {
                if let Entry::Occupied(mut entry) = self.cells.entry(cell) {
                    let keys = entry.get_mut();
                    if let Some(index) = keys.iter().position(|k| *k == key) {
                        keys.swap_remove(index);
                        self.population[tier as usize].fetch_sub(1, Ordering::Relaxed);
                    }
                    if keys.is_empty() {
                        entry.remove();
                    }
                }
            }
            if let Some(cell) = after {
                self.cells.entry(cell).or_default().push(key);
                self.population[tier as usize].fetch_add(1, Ordering::Relaxed);
            }
        }
        let was_unbounded = old.is_some_and(|(reach, _)| reach == Reach::Unbounded);
        let is_unbounded = new.is_some_and(|(reach, _)| reach == Reach::Unbounded);
        if was_unbounded && !is_unbounded {
            self.unbounded.remove(&key);
        } else if is_unbounded && !was_unbounded {
            self.unbounded.insert(key, ());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn near(grid: &ZoneGrid<u32>, position: Vec3) -> Vec<u32> {
        let mut out = Vec::new();
        grid.near(position, &mut out);
        out.sort();
        out
    }

    #[test]
    fn reach_picks_the_smallest_tier_that_holds_the_radius() {
        assert_eq!(Reach::of_radius(150.0), Reach::Tier(0));
        assert_eq!(Reach::of_radius(256.0), Reach::Tier(0));
        assert_eq!(Reach::of_radius(300.0), Reach::Tier(1));
        assert_eq!(Reach::of_radius(10_001.0), Reach::Tier(3));
        assert_eq!(Reach::of_radius(200_001.0), Reach::Tier(5));
        assert_eq!(Reach::of_radius(1_000_000.0), Reach::Tier(6));
        assert_eq!(Reach::of_radius(2e9), Reach::Unbounded);
    }

    #[test]
    fn every_point_of_a_zone_finds_its_object() {
        let grid = ZoneGrid::new();
        let center = Vec3::new(1e9 + 37.0, -512.0, 4096.5);
        let radius = 203.0;
        grid.place(1, center, Reach::of_radius(radius));
        // Points on the zone's edge along every axis and diagonal, across cell borders.
        for dx in [-1.0, 0.0, 1.0] {
            for dy in [-1.0, 0.0, 1.0] {
                for dz in [-1.0, 0.0, 1.0] {
                    let norm = ((dx * dx + dy * dy + dz * dz) as f64).sqrt().max(1.0);
                    let edge = Vec3::new(center.x + radius * dx / norm, center.y + radius * dy / norm, center.z + radius * dz / norm);
                    assert_eq!(near(&grid, edge), vec![1], "at {:?}", edge);
                }
            }
        }
        assert!(near(&grid, Vec3::new(center.x + 3.0 * BASE_CELL, center.y, center.z)).is_empty());
    }

    #[test]
    fn moves_and_removals_keep_one_entry_per_key() {
        let grid = ZoneGrid::new();
        grid.place(1, Vec3::new(0.0, 0.0, 0.0), Reach::Tier(0));
        grid.place(2, Vec3::new(10.0, 0.0, 0.0), Reach::Tier(0));
        grid.move_to(1, Vec3::new(5_000.0, 0.0, 0.0));
        assert_eq!(near(&grid, Vec3::new(0.0, 0.0, 0.0)), vec![2]);
        assert_eq!(near(&grid, Vec3::new(5_000.0, 0.0, 0.0)), vec![1]);
        // Placed again (a reconnect re-registers the same id): no duplicate.
        grid.place(1, Vec3::new(5_001.0, 0.0, 0.0), Reach::Tier(0));
        assert_eq!(near(&grid, Vec3::new(5_000.0, 0.0, 0.0)), vec![1]);
        grid.remove(1);
        grid.remove(1);
        assert!(near(&grid, Vec3::new(5_000.0, 0.0, 0.0)).is_empty());
        grid.move_to(1, Vec3::new(0.0, 0.0, 0.0));
        assert_eq!(near(&grid, Vec3::new(0.0, 0.0, 0.0)), vec![2], "move_to does not place a removed key");
        assert_eq!(grid.population[0].load(Ordering::Relaxed), 1);
    }

    #[test]
    fn unbounded_zones_are_always_near() {
        let grid = ZoneGrid::new();
        grid.place(7, Vec3::new(0.0, 0.0, 0.0), Reach::of_radius(2e9));
        assert_eq!(near(&grid, Vec3::new(1e11, 0.0, 0.0)), vec![7]);
        grid.place(7, Vec3::new(0.0, 0.0, 0.0), Reach::Tier(0));
        assert!(near(&grid, Vec3::new(1e11, 0.0, 0.0)).is_empty());
    }

    #[test]
    fn players_are_found_from_any_tier() {
        let grid = ZoneGrid::new();
        grid.place(3, Vec3::new(9_000.0, 0.0, 0.0), Reach::Every);
        for tier in 0..TIERS {
            let mut out = Vec::new();
            grid.near_in_tier(Vec3::new(9_000.0 - cell_size(tier) * 0.99, 0.0, 0.0), tier, &mut out);
            assert_eq!(out, vec![3], "tier {}", tier);
        }
        let mut out = Vec::new();
        grid.near_in_tier(Vec3::new(0.0, 0.0, 0.0), 0, &mut out);
        assert!(out.is_empty());
    }
}
