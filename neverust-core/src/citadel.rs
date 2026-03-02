//! Citadel-style lens + defederation model for Neverust.
//!
//! This module focuses on three things:
//! 1) Deterministic anti-drift convergence using ordered op frontiers.
//! 2) Defederation semantics (follow graph + transitive visible content).
//! 3) Admission hardening against spam and sybil floods.
//!
//! It is intentionally transport-agnostic so it can be embedded in the
//! Neverust runtime and also stress-tested in large in-memory simulations.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

pub type NodeId = u32;
pub type SiteId = u64;
pub type ContentSlot = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Lamport {
    pub counter: u64,
    pub origin: NodeId,
}

impl Lamport {
    pub fn new(counter: u64, origin: NodeId) -> Self {
        Self { counter, origin }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LwwEntry {
    pub ts: Lamport,
    pub present: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LensGraph {
    follows: BTreeMap<(SiteId, SiteId), LwwEntry>,
    content: BTreeMap<(SiteId, ContentSlot), LwwEntry>,
}

impl LensGraph {
    fn lww_update<K: Ord + Copy>(map: &mut BTreeMap<K, LwwEntry>, key: K, next: LwwEntry) {
        match map.get(&key).copied() {
            Some(prev) if prev.ts >= next.ts => {}
            _ => {
                map.insert(key, next);
            }
        }
    }

    pub fn update_follow(&mut self, site_id: SiteId, target_site_id: SiteId, ts: Lamport, enabled: bool) {
        Self::lww_update(
            &mut self.follows,
            (site_id, target_site_id),
            LwwEntry {
                ts,
                present: enabled,
            },
        );
    }

    pub fn update_content(
        &mut self,
        site_id: SiteId,
        content_slot: ContentSlot,
        ts: Lamport,
        present: bool,
    ) {
        Self::lww_update(
            &mut self.content,
            (site_id, content_slot),
            LwwEntry { ts, present },
        );
    }

    pub fn merge_from(&mut self, other: &Self) {
        for (k, v) in &other.follows {
            Self::lww_update(&mut self.follows, *k, *v);
        }
        for (k, v) in &other.content {
            Self::lww_update(&mut self.content, *k, *v);
        }
    }

    pub fn reachable_sites(&self, root_site_id: SiteId) -> BTreeSet<SiteId> {
        let mut out = BTreeSet::new();
        let mut q = VecDeque::new();
        out.insert(root_site_id);
        q.push_back(root_site_id);

        while let Some(site) = q.pop_front() {
            for ((from, to), edge) in &self.follows {
                if *from == site && edge.present && out.insert(*to) {
                    q.push_back(*to);
                }
            }
        }

        out
    }

    pub fn visible_content(&self, root_site_id: SiteId) -> BTreeSet<(SiteId, ContentSlot)> {
        let reachable = self.reachable_sites(root_site_id);
        self.content
            .iter()
            .filter_map(|((site, slot), entry)| {
                if entry.present && reachable.contains(site) {
                    Some((*site, *slot))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn view_digest_hex(&self, root_site_id: SiteId) -> String {
        let reachable = self.reachable_sites(root_site_id);
        let visible = self.visible_content(root_site_id);
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"neverust-citadel-view-v1");
        hasher.update(&root_site_id.to_le_bytes());

        for site in &reachable {
            hasher.update(&site.to_le_bytes());
        }

        for (site, slot) in &visible {
            hasher.update(&site.to_le_bytes());
            hasher.update(&slot.to_le_bytes());
        }

        hasher.finalize().to_hex().to_string()
    }

    pub fn follows_len(&self) -> usize {
        self.follows.len()
    }

    pub fn content_len(&self) -> usize {
        self.content.len()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LensOpKind {
    Follow {
        site_id: SiteId,
        target_site_id: SiteId,
        enabled: bool,
    },
    Content {
        site_id: SiteId,
        content_slot: ContentSlot,
        present: bool,
    },
    TrustOrigin {
        origin: NodeId,
        trusted: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LensOp {
    pub origin: NodeId,
    pub counter: u64,
    pub host_id: u8,
    pub kind: LensOpKind,
    pub pow_nonce: u64,
}

impl LensOp {
    fn hash_without_nonce(&self, nonce: u64) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.origin.to_le_bytes());
        hasher.update(&self.counter.to_le_bytes());
        hasher.update(&[self.host_id]);
        match &self.kind {
            LensOpKind::Follow {
                site_id,
                target_site_id,
                enabled,
            } => {
                hasher.update(&[0]);
                hasher.update(&site_id.to_le_bytes());
                hasher.update(&target_site_id.to_le_bytes());
                hasher.update(&[*enabled as u8]);
            }
            LensOpKind::Content {
                site_id,
                content_slot,
                present,
            } => {
                hasher.update(&[1]);
                hasher.update(&site_id.to_le_bytes());
                hasher.update(&content_slot.to_le_bytes());
                hasher.update(&[*present as u8]);
            }
            LensOpKind::TrustOrigin { origin, trusted } => {
                hasher.update(&[2]);
                hasher.update(&origin.to_le_bytes());
                hasher.update(&[*trusted as u8]);
            }
        }
        hasher.update(&nonce.to_le_bytes());
        hasher.finalize()
    }

    pub fn leading_zero_bits(hash: &blake3::Hash) -> u32 {
        let mut bits = 0u32;
        for b in hash.as_bytes() {
            if *b == 0 {
                bits += 8;
                continue;
            }
            bits += b.leading_zeros();
            break;
        }
        bits
    }

    pub fn valid_pow(&self, required_bits: u8) -> bool {
        if required_bits == 0 {
            return true;
        }
        let h = self.hash_without_nonce(self.pow_nonce);
        Self::leading_zero_bits(&h) >= required_bits as u32
    }

    pub fn mine_nonce(mut self, required_bits: u8) -> Self {
        if required_bits == 0 {
            self.pow_nonce = 0;
            return self;
        }

        let mut nonce = 0u64;
        loop {
            let h = self.hash_without_nonce(nonce);
            if Self::leading_zero_bits(&h) >= required_bits as u32 {
                self.pow_nonce = nonce;
                return self;
            }
            nonce = nonce.wrapping_add(1);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefederationGuardConfig {
    pub base_pow_bits: u8,
    pub trusted_pow_bits: u8,
    pub max_ops_per_origin_per_round: u32,
    pub max_new_origins_per_host_per_round: u32,
    pub max_pending_per_origin: usize,
}

impl Default for DefederationGuardConfig {
    fn default() -> Self {
        Self {
            base_pow_bits: 8,
            trusted_pow_bits: 4,
            max_ops_per_origin_per_round: 96,
            max_new_origins_per_host_per_round: 12,
            max_pending_per_origin: 512,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DefederationNodeStats {
    pub accepted_ops: u64,
    pub rejected_old_or_duplicate: u64,
    pub rejected_pow: u64,
    pub rejected_rate_limit: u64,
    pub rejected_sybil: u64,
    pub queued_reordered: u64,
}

#[derive(Debug, Default, Clone)]
struct DefederationGuardState {
    round: u64,
    per_origin_counts: HashMap<NodeId, u32>,
    per_host_new_origins: HashMap<u8, u32>,
}

impl DefederationGuardState {
    fn roll_to(&mut self, round: u64) {
        if self.round == round {
            return;
        }
        self.round = round;
        self.per_origin_counts.clear();
        self.per_host_new_origins.clear();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefederationNode {
    pub node_id: NodeId,
    pub host_id: u8,
    pub local_site_id: SiteId,
    local_counter: u64,
    trusted_origins: HashSet<NodeId>,
    known_origins: HashSet<NodeId>,
    frontier: HashMap<NodeId, u64>,
    #[serde(skip)]
    pending: HashMap<NodeId, BTreeMap<u64, LensOp>>,
    pub graph: LensGraph,
    pub guard_cfg: DefederationGuardConfig,
    pub idle_bandwidth_bytes_per_sec: u64,
    #[serde(skip)]
    op_log: HashMap<NodeId, Vec<LensOp>>,
    #[serde(skip)]
    guard_state: DefederationGuardState,
    pub stats: DefederationNodeStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefederationStatus {
    pub node_id: NodeId,
    pub host_id: u8,
    pub local_site_id: SiteId,
    pub trusted_origins: usize,
    pub known_origins: usize,
    pub follows: usize,
    pub content_entries: usize,
    pub frontier_origins: usize,
    pub idle_bandwidth_bytes_per_sec: u64,
    pub stats: DefederationNodeStats,
}

impl DefederationNode {
    pub fn new(
        node_id: NodeId,
        host_id: u8,
        local_site_id: SiteId,
        mut trusted_origins: HashSet<NodeId>,
        guard_cfg: DefederationGuardConfig,
    ) -> Self {
        trusted_origins.insert(node_id);
        let mut known_origins = HashSet::new();
        known_origins.insert(node_id);
        Self {
            node_id,
            host_id,
            local_site_id,
            local_counter: 0,
            trusted_origins,
            known_origins,
            frontier: HashMap::new(),
            pending: HashMap::new(),
            graph: LensGraph::default(),
            guard_cfg,
            idle_bandwidth_bytes_per_sec: 100 * 1024,
            op_log: HashMap::new(),
            guard_state: DefederationGuardState::default(),
            stats: DefederationNodeStats::default(),
        }
    }

    pub fn set_idle_bandwidth_bytes_per_sec(&mut self, bytes_per_sec: u64) {
        self.idle_bandwidth_bytes_per_sec = bytes_per_sec.max(1);
    }

    pub fn status(&self) -> DefederationStatus {
        DefederationStatus {
            node_id: self.node_id,
            host_id: self.host_id,
            local_site_id: self.local_site_id,
            trusted_origins: self.trusted_origins.len(),
            known_origins: self.known_origins.len(),
            follows: self.graph.follows_len(),
            content_entries: self.graph.content_len(),
            frontier_origins: self.frontier.len(),
            idle_bandwidth_bytes_per_sec: self.idle_bandwidth_bytes_per_sec,
            stats: self.stats.clone(),
        }
    }

    fn required_pow_bits_for(&self, origin: NodeId) -> u8 {
        if self.trusted_origins.contains(&origin) {
            self.guard_cfg.trusted_pow_bits
        } else {
            self.guard_cfg.base_pow_bits
        }
    }

    pub fn trust_origin(&mut self, origin: NodeId, trusted: bool) {
        if trusted {
            self.trusted_origins.insert(origin);
        } else {
            self.trusted_origins.remove(&origin);
        }
    }

    fn next_local_op(&mut self, kind: LensOpKind) -> LensOp {
        self.local_counter = self.local_counter.saturating_add(1);
        let op = LensOp {
            origin: self.node_id,
            counter: self.local_counter,
            host_id: self.host_id,
            kind,
            pow_nonce: 0,
        };
        // Local ops must be portable to untrusted peers, so we always mine at
        // base difficulty (which is >= trusted difficulty in normal configs).
        op.mine_nonce(self.guard_cfg.base_pow_bits)
    }

    pub fn emit_local_follow(&mut self, target_site_id: SiteId, enabled: bool) -> LensOp {
        let op = self.next_local_op(LensOpKind::Follow {
            site_id: self.local_site_id,
            target_site_id,
            enabled,
        });
        self.apply_ordered_op(op.clone());
        op
    }

    pub fn emit_local_content(&mut self, content_slot: ContentSlot, present: bool) -> LensOp {
        let op = self.next_local_op(LensOpKind::Content {
            site_id: self.local_site_id,
            content_slot,
            present,
        });
        self.apply_ordered_op(op.clone());
        op
    }

    pub fn emit_local_trust(&mut self, origin: NodeId, trusted: bool) -> LensOp {
        let op = self.next_local_op(LensOpKind::TrustOrigin { origin, trusted });
        self.apply_ordered_op(op.clone());
        op
    }

    fn apply_kind(&mut self, op: &LensOp) {
        let ts = Lamport::new(op.counter, op.origin);
        match op.kind {
            LensOpKind::Follow {
                site_id,
                target_site_id,
                enabled,
            } => self
                .graph
                .update_follow(site_id, target_site_id, ts, enabled),
            LensOpKind::Content {
                site_id,
                content_slot,
                present,
            } => self.graph.update_content(site_id, content_slot, ts, present),
            LensOpKind::TrustOrigin { origin, trusted } => self.trust_origin(origin, trusted),
        }
    }

    fn apply_ordered_op(&mut self, op: LensOp) {
        self.apply_kind(&op);
        self.frontier.insert(op.origin, op.counter);
        let log = self.op_log.entry(op.origin).or_default();
        if log.len() == op.counter.saturating_sub(1) as usize {
            log.push(op);
        }
    }

    fn try_apply_with_reordering(&mut self, op: LensOp) {
        let op_origin = op.origin;
        let current = self.frontier.get(&op.origin).copied().unwrap_or(0);
        if op.counter <= current {
            self.stats.rejected_old_or_duplicate = self.stats.rejected_old_or_duplicate.saturating_add(1);
            return;
        }

        let expected = current.saturating_add(1);
        if op.counter != expected {
            let pending_for_origin = self.pending.entry(op.origin).or_default();
            if pending_for_origin.len() >= self.guard_cfg.max_pending_per_origin {
                self.stats.rejected_rate_limit = self.stats.rejected_rate_limit.saturating_add(1);
                return;
            }
            pending_for_origin.entry(op.counter).or_insert(op);
            self.stats.queued_reordered = self.stats.queued_reordered.saturating_add(1);
            return;
        }

        self.apply_ordered_op(op);
        self.stats.accepted_ops = self.stats.accepted_ops.saturating_add(1);

        loop {
            let frontier = self.frontier.get(&op_origin).copied().unwrap_or(0);
            let next_counter = frontier.saturating_add(1);
            let next_op = self
                .pending
                .get_mut(&op_origin)
                .and_then(|pending_for_origin| pending_for_origin.remove(&next_counter));
            let Some(next_op) = next_op else {
                break;
            };
            self.apply_ordered_op(next_op);
            self.stats.accepted_ops = self.stats.accepted_ops.saturating_add(1);
        }
    }

    pub fn ingest_batch<I>(&mut self, round: u64, ops: I) -> usize
    where
        I: IntoIterator<Item = LensOp>,
    {
        self.guard_state.roll_to(round);
        let mut accepted = 0usize;

        for op in ops {
            let is_known_origin = self.known_origins.contains(&op.origin);
            let is_trusted_origin = self.trusted_origins.contains(&op.origin);

            if !is_known_origin && !is_trusted_origin {
                let seen_for_host = self
                    .guard_state
                    .per_host_new_origins
                    .entry(op.host_id)
                    .or_insert(0);
                if *seen_for_host >= self.guard_cfg.max_new_origins_per_host_per_round {
                    self.stats.rejected_sybil = self.stats.rejected_sybil.saturating_add(1);
                    continue;
                }
            }

            let per_origin_seen = self
                .guard_state
                .per_origin_counts
                .get(&op.origin)
                .copied()
                .unwrap_or(0);
            if per_origin_seen >= self.guard_cfg.max_ops_per_origin_per_round {
                self.stats.rejected_rate_limit = self.stats.rejected_rate_limit.saturating_add(1);
                continue;
            }

            let required_bits = self.required_pow_bits_for(op.origin);
            if !op.valid_pow(required_bits) {
                self.stats.rejected_pow = self.stats.rejected_pow.saturating_add(1);
                continue;
            }

            if !is_known_origin {
                self.known_origins.insert(op.origin);
                if !is_trusted_origin {
                    *self
                        .guard_state
                        .per_host_new_origins
                        .entry(op.host_id)
                        .or_insert(0) += 1;
                }
            }

            *self
                .guard_state
                .per_origin_counts
                .entry(op.origin)
                .or_insert(0) += 1;
            let before = self.stats.accepted_ops;
            self.try_apply_with_reordering(op);
            if self.stats.accepted_ops > before {
                accepted += 1;
            }
        }

        accepted
    }

    pub fn view_digest_hex(&self, root_site_id: SiteId) -> String {
        self.graph.view_digest_hex(root_site_id)
    }

    pub fn frontier_snapshot(&self) -> HashMap<NodeId, u64> {
        self.frontier.clone()
    }

    pub fn missing_ops_for_frontier(
        &self,
        peer_frontier: &HashMap<NodeId, u64>,
        max_ops: usize,
    ) -> Vec<LensOp> {
        let mut out = Vec::new();
        if max_ops == 0 {
            return out;
        }

        let mut origins: Vec<NodeId> = self.op_log.keys().copied().collect();
        origins.sort_unstable();

        for origin in origins {
            if out.len() >= max_ops {
                break;
            }
            let src = self.frontier_for(origin);
            let dst = peer_frontier.get(&origin).copied().unwrap_or(0);
            if src <= dst {
                continue;
            }

            let Some(log) = self.op_log.get(&origin) else {
                continue;
            };
            let mut c = dst.saturating_add(1);
            while c <= src && out.len() < max_ops {
                let idx = (c as usize).saturating_sub(1);
                if let Some(op) = log.get(idx) {
                    out.push(op.clone());
                }
                c = c.saturating_add(1);
            }
        }
        out
    }

    pub fn frontier_for(&self, origin: NodeId) -> u64 {
        self.frontier.get(&origin).copied().unwrap_or(0)
    }

    pub fn missing_ops_for_peer(
        &self,
        peer_frontier: &HashMap<NodeId, u64>,
        op_store: &HashMap<NodeId, Vec<LensOp>>,
        writer_origins: &[NodeId],
        max_ops: usize,
    ) -> Vec<LensOp> {
        let mut out = Vec::new();
        for origin in writer_origins {
            if out.len() >= max_ops {
                break;
            }

            let src = self.frontier_for(*origin);
            let dst = peer_frontier.get(origin).copied().unwrap_or(0);
            if src <= dst {
                continue;
            }

            let Some(log) = op_store.get(origin) else {
                continue;
            };

            let mut c = dst.saturating_add(1);
            while c <= src && out.len() < max_ops {
                let idx = (c as usize).saturating_sub(1);
                if let Some(op) = log.get(idx) {
                    out.push(op.clone());
                }
                c = c.saturating_add(1);
            }
        }
        out
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CitadelSyncPullRequest {
    pub round: u64,
    #[serde(default)]
    pub frontier: HashMap<NodeId, u64>,
    #[serde(default)]
    pub max_ops: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CitadelSyncPullResponse {
    pub node_id: NodeId,
    pub round: u64,
    pub accepted_local_ops: usize,
    pub provided_ops: usize,
    pub frontier: HashMap<NodeId, u64>,
    pub ops: Vec<LensOp>,
    pub status: DefederationStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CitadelSyncPushRequest {
    pub round: u64,
    #[serde(default)]
    pub frontier: HashMap<NodeId, u64>,
    #[serde(default)]
    pub ops: Vec<LensOp>,
    #[serde(default)]
    pub max_ops: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CitadelSyncPushResponse {
    pub node_id: NodeId,
    pub round: u64,
    pub accepted_ops: usize,
    pub provided_ops: usize,
    pub frontier: HashMap<NodeId, u64>,
    pub ops: Vec<LensOp>,
    pub status: DefederationStatus,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FlagshipTrustSnapshot {
    #[serde(default)]
    pub trusted_origins: Vec<NodeId>,
    #[serde(default)]
    pub bootstrap_sites: Vec<SiteId>,
}

pub async fn fetch_flagship_trust_snapshot(url: &str) -> Result<FlagshipTrustSnapshot, String> {
    let resp = reqwest::get(url)
        .await
        .map_err(|e| format!("flagship fetch failed: {}", e))?;
    if !resp.status().is_success() {
        return Err(format!("flagship fetch failed with status {}", resp.status()));
    }
    resp.json::<FlagshipTrustSnapshot>()
        .await
        .map_err(|e| format!("flagship response parse failed: {}", e))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdleBandwidthGateConfig {
    /// Hard idle target (bytes/sec) for normal swarms.
    /// 100 KiB/s = 102_400 B/s.
    pub max_idle_bytes_per_sec: u64,
    /// Fixed control-plane beacon estimate.
    pub beacon_bytes: usize,
    /// Estimated per-op payload bytes on wire.
    pub op_bytes_estimate: usize,
    /// Minimum fanout for anti-entropy probes.
    pub min_repair_peers: usize,
    /// Maximum fanout for anti-entropy probes.
    pub max_repair_peers: usize,
    /// Above this size, we allow gentle logarithmic relaxation.
    pub huge_swarm_threshold: usize,
}

impl Default for IdleBandwidthGateConfig {
    fn default() -> Self {
        Self {
            max_idle_bytes_per_sec: 100 * 1024,
            beacon_bytes: 96,
            op_bytes_estimate: 160,
            min_repair_peers: 4,
            max_repair_peers: 16,
            huge_swarm_threshold: 100_000,
        }
    }
}

impl IdleBandwidthGateConfig {
    /// O(log N) probe fanout, clamped to a small constant range.
    pub fn repair_peers_for_swarm(&self, swarm_size: usize) -> usize {
        if swarm_size <= 1 {
            return 1;
        }
        let ln = (swarm_size as f64).log2().ceil() as usize;
        ln.clamp(self.min_repair_peers, self.max_repair_peers)
    }

    /// Idle budget policy:
    /// - <= huge threshold: fixed cap (O(1) per node).
    /// - > huge threshold: relaxed by O(log N), still sublinear.
    pub fn idle_budget_for_swarm(&self, swarm_size: usize) -> u64 {
        if swarm_size <= self.huge_swarm_threshold {
            return self.max_idle_bytes_per_sec;
        }
        let growth = ((swarm_size as f64 / self.huge_swarm_threshold as f64).log2().max(0.0))
            as u64;
        self.max_idle_bytes_per_sec
            .saturating_add(growth.saturating_mul(8 * 1024))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefederationSimulationConfig {
    pub node_count: usize,
    pub writer_count: usize,
    pub malicious_nodes: usize,
    pub rounds: u64,
    pub settle_rounds: u64,
    pub fanout: usize,
    pub honest_write_ops_per_round: usize,
    pub spam_ops_per_malicious_round: usize,
    pub sybil_ops_per_malicious_round: usize,
    pub max_gossip_batch: usize,
    pub target_site_id: SiteId,
    pub trusted_seed_origins: usize,
    pub guard: DefederationGuardConfig,
    pub idle_gate: IdleBandwidthGateConfig,
}

impl Default for DefederationSimulationConfig {
    fn default() -> Self {
        Self {
            node_count: 10_000,
            writer_count: 64,
            malicious_nodes: 256,
            rounds: 18,
            settle_rounds: 10,
            fanout: 8,
            honest_write_ops_per_round: 96,
            spam_ops_per_malicious_round: 2,
            sybil_ops_per_malicious_round: 1,
            max_gossip_batch: 24,
            target_site_id: 1,
            trusted_seed_origins: 16,
            guard: DefederationGuardConfig::default(),
            idle_gate: IdleBandwidthGateConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefederationSimulationResult {
    pub converged: bool,
    pub rounds_executed: u64,
    pub unique_honest_view_digests: usize,
    pub honest_nodes_off_majority_view: usize,
    pub generated_ops: u64,
    pub delivered_ops: u64,
    pub repair_messages: u64,
    pub total_accepted_ops: u64,
    pub total_rejected_pow: u64,
    pub total_rejected_sybil: u64,
    pub total_rejected_rate_limit: u64,
    pub max_idle_bytes_per_sec_observed: u64,
    pub avg_idle_bytes_per_sec_observed: u64,
    pub idle_budget_bytes_per_sec: u64,
}

fn build_two_host_topology(node_count: usize, fanout: usize) -> Vec<Vec<usize>> {
    let mut topology = vec![Vec::new(); node_count];
    if node_count < 2 {
        return topology;
    }

    let fanout = fanout.max(2);
    let half = node_count / 2;
    for i in 0..node_count {
        let mut peers = HashSet::new();
        let local_base = if i < half { 0 } else { half };
        let remote_base = if i < half { half } else { 0 };
        let local_len = if i < half { half.max(1) } else { (node_count - half).max(1) };
        let remote_len = if i < half { (node_count - half).max(1) } else { half.max(1) };

        let local_target = fanout / 2;
        let mut step = 1usize;
        while peers.len() < local_target {
            let offset = ((i.wrapping_mul(131)).wrapping_add(step.wrapping_mul(17))) % local_len;
            let peer = local_base + offset;
            if peer != i {
                peers.insert(peer);
            }
            step = step.wrapping_add(1);
        }

        let mut rstep = 1usize;
        while peers.len() < fanout {
            let offset = ((i.wrapping_mul(977)).wrapping_add(rstep.wrapping_mul(593))) % remote_len;
            let peer = remote_base + offset;
            if peer != i {
                peers.insert(peer);
            }
            rstep = rstep.wrapping_add(1);
        }

        topology[i] = peers.into_iter().collect();
    }
    topology
}

fn make_invalid_spam_op(origin: NodeId, counter: u64, host_id: u8, target_site_id: SiteId) -> LensOp {
    LensOp {
        origin,
        counter,
        host_id,
        kind: LensOpKind::Content {
            site_id: target_site_id,
            content_slot: 0,
            present: true,
        },
        pow_nonce: 0,
    }
}

fn make_sybil_op(origin: NodeId, counter: u64, host_id: u8, site_id: SiteId, bits: u8) -> LensOp {
    LensOp {
        origin,
        counter,
        host_id,
        kind: LensOpKind::Follow {
            site_id,
            target_site_id: 1,
            enabled: true,
        },
        pow_nonce: 0,
    }
    .mine_nonce(bits)
}

pub fn run_defederation_simulation(cfg: &DefederationSimulationConfig) -> DefederationSimulationResult {
    if cfg.node_count == 0 {
        return DefederationSimulationResult {
            converged: true,
            rounds_executed: 0,
            unique_honest_view_digests: 0,
            honest_nodes_off_majority_view: 0,
            generated_ops: 0,
            delivered_ops: 0,
            repair_messages: 0,
            total_accepted_ops: 0,
            total_rejected_pow: 0,
            total_rejected_sybil: 0,
            total_rejected_rate_limit: 0,
            max_idle_bytes_per_sec_observed: 0,
            avg_idle_bytes_per_sec_observed: 0,
            idle_budget_bytes_per_sec: cfg.idle_gate.max_idle_bytes_per_sec,
        };
    }

    let malicious_nodes = cfg.malicious_nodes.min(cfg.node_count.saturating_sub(1));
    let honest_count = cfg.node_count - malicious_nodes;
    let writer_count = cfg.writer_count.min(honest_count).max(1);
    let writer_origins: Vec<NodeId> = (0..writer_count).map(|i| i as NodeId).collect();
    let topology = build_two_host_topology(cfg.node_count, cfg.fanout);

    let mut nodes = Vec::with_capacity(cfg.node_count);
    for i in 0..cfg.node_count {
        let host_id = if i < cfg.node_count / 2 { 0 } else { 1 };
        let local_site_id = i as SiteId + 1;
        let mut trusted = HashSet::new();
        for origin in writer_origins.iter().take(cfg.trusted_seed_origins.min(writer_count)) {
            trusted.insert(*origin);
        }
        trusted.insert(i as NodeId);
        nodes.push(DefederationNode::new(
            i as NodeId,
            host_id,
            local_site_id,
            trusted,
            cfg.guard.clone(),
        ));
    }

    let mut op_store: HashMap<NodeId, Vec<LensOp>> = HashMap::new();
    let mut generated_ops = 0u64;
    let mut delivered_ops = 0u64;
    let mut repair_messages = 0u64;
    let idle_budget_bytes_per_sec = cfg.idle_gate.idle_budget_for_swarm(cfg.node_count);
    let repair_peers = cfg
        .idle_gate
        .repair_peers_for_swarm(cfg.node_count)
        .min(cfg.fanout.max(1));
    let mut max_idle_bytes_per_sec_observed = 0u64;
    let mut idle_bytes_accum: u128 = 0;
    let mut idle_rounds: u64 = 0;

    // Bootstrap a stable writer backbone.
    for writer in 0..writer_count {
        let target = ((writer + 1) % writer_count) as SiteId + 1;
        let follow = nodes[writer].emit_local_follow(target, true);
        op_store.entry(follow.origin).or_default().push(follow);

        let content = nodes[writer].emit_local_content(0, true);
        op_store.entry(content.origin).or_default().push(content);
        generated_ops = generated_ops.saturating_add(2);
    }

    let mut converged_round: Option<u64> = None;
    let total_rounds = cfg.rounds.saturating_add(cfg.settle_rounds);
    for round in 0..total_rounds {
        let mut bytes_sent_this_round = vec![0u64; cfg.node_count];

        // Honest writes (bounded).
        if round < cfg.rounds {
            for op_idx in 0..cfg.honest_write_ops_per_round {
                let writer = ((round as usize).wrapping_mul(4099).wrapping_add(op_idx * 131)) % writer_count;
                let target = ((writer + (round as usize % writer_count) + 1) % writer_count) as SiteId + 1;

                let op = if (round + op_idx as u64) % 5 == 0 {
                    nodes[writer].emit_local_follow(target, true)
                } else {
                    nodes[writer].emit_local_content(
                        0,
                        ((round + op_idx as u64) % 7) != 0,
                    )
                };

                op_store.entry(op.origin).or_default().push(op);
                generated_ops = generated_ops.saturating_add(1);
            }
        }

        let mut inboxes: Vec<Vec<LensOp>> = vec![Vec::new(); cfg.node_count];

        // Malicious spam/sybil pressure.
        for m in 0..malicious_nodes {
            let idx = honest_count + m;
            let host_id = nodes[idx].host_id;
            let site_id = nodes[idx].local_site_id;

            for i in 0..cfg.spam_ops_per_malicious_round {
                let target = (idx + i + round as usize * 3) % cfg.node_count;
                let op = make_invalid_spam_op(
                    idx as NodeId,
                    round.saturating_mul(1_000).saturating_add(i as u64 + 1),
                    host_id,
                    site_id,
                );
                inboxes[target].push(op);
                bytes_sent_this_round[idx] = bytes_sent_this_round[idx]
                    .saturating_add(cfg.idle_gate.op_bytes_estimate as u64);
                generated_ops = generated_ops.saturating_add(1);
            }

            for i in 0..cfg.sybil_ops_per_malicious_round {
                let target = (idx + i + round as usize * 11) % cfg.node_count;
                let sybil_origin = 1_000_000u32
                    .saturating_add((m as u32).saturating_mul(10_000))
                    .saturating_add(round as u32)
                    .saturating_add(i as u32);
                let op = make_sybil_op(
                    sybil_origin,
                    1,
                    host_id,
                    site_id,
                    cfg.guard.base_pow_bits,
                );
                inboxes[target].push(op);
                bytes_sent_this_round[idx] = bytes_sent_this_round[idx]
                    .saturating_add(cfg.idle_gate.op_bytes_estimate as u64);
                generated_ops = generated_ops.saturating_add(1);
            }
        }

        // Snapshot writer frontiers for cheap diffing.
        let mut frontier_snapshot = vec![vec![0u64; writer_count]; cfg.node_count];
        for (node_idx, node) in nodes.iter().enumerate() {
            for (w_idx, origin) in writer_origins.iter().enumerate() {
                frontier_snapshot[node_idx][w_idx] = node.frontier_for(*origin);
            }
        }

        // Anti-entropy repair gossip.
        for src in 0..cfg.node_count {
            let peers = &topology[src];
            if peers.is_empty() {
                continue;
            }
            let stride = peers.len().max(1);
            let start = ((src.wrapping_mul(17)).wrapping_add((round as usize).wrapping_mul(31))) % stride;
            let to_probe = repair_peers.min(peers.len());
            for step in 0..to_probe {
                let dst = peers[(start + step) % peers.len()];
                let idle_mode = round >= cfg.rounds;
                let beacon_bytes = cfg.idle_gate.beacon_bytes as u64;
                if idle_mode
                    && bytes_sent_this_round[src].saturating_add(beacon_bytes)
                        > idle_budget_bytes_per_sec
                {
                    continue;
                }
                bytes_sent_this_round[src] = bytes_sent_this_round[src].saturating_add(beacon_bytes);

                let mut missing_any = false;
                for (w_idx, origin) in writer_origins.iter().enumerate() {
                    if inboxes[dst].len() >= cfg.max_gossip_batch {
                        break;
                    }
                    let src_ctr = frontier_snapshot[src][w_idx];
                    let dst_ctr = frontier_snapshot[dst][w_idx];
                    if src_ctr <= dst_ctr {
                        continue;
                    }
                    let Some(log) = op_store.get(origin) else {
                        continue;
                    };

                    let mut next = dst_ctr.saturating_add(1);
                    while next <= src_ctr && inboxes[dst].len() < cfg.max_gossip_batch {
                        let op_bytes = cfg.idle_gate.op_bytes_estimate as u64;
                        if idle_mode
                            && bytes_sent_this_round[src].saturating_add(op_bytes)
                                > idle_budget_bytes_per_sec
                        {
                            break;
                        }
                        let idx = (next as usize).saturating_sub(1);
                        if let Some(op) = log.get(idx) {
                            inboxes[dst].push(op.clone());
                            bytes_sent_this_round[src] =
                                bytes_sent_this_round[src].saturating_add(op_bytes);
                            delivered_ops = delivered_ops.saturating_add(1);
                            missing_any = true;
                        }
                        next = next.saturating_add(1);
                    }
                }
                if missing_any {
                    repair_messages = repair_messages.saturating_add(1);
                }
            }
        }

        // Apply all incoming ops.
        for idx in 0..cfg.node_count {
            let ops = std::mem::take(&mut inboxes[idx]);
            nodes[idx].ingest_batch(round, ops);
        }

        if round >= cfg.rounds {
            idle_rounds = idle_rounds.saturating_add(1);
            let mut round_max = 0u64;
            for sent in bytes_sent_this_round.iter().take(honest_count) {
                round_max = round_max.max(*sent);
                idle_bytes_accum = idle_bytes_accum.saturating_add(*sent as u128);
            }
            max_idle_bytes_per_sec_observed = max_idle_bytes_per_sec_observed.max(round_max);
        }

        // Convergence check only in settle window.
        if round >= cfg.rounds {
            let mut digest_counts: HashMap<String, usize> = HashMap::new();
            for node in nodes.iter().take(honest_count) {
                let digest = node.view_digest_hex(cfg.target_site_id);
                *digest_counts.entry(digest).or_insert(0) += 1;
            }

            if digest_counts.len() == 1 {
                converged_round = Some(round);
                break;
            }
        }
    }

    let rounds_executed = converged_round
        .map(|r| r.saturating_add(1))
        .unwrap_or(total_rounds);

    let mut digest_counts: HashMap<String, usize> = HashMap::new();
    for node in nodes.iter().take(honest_count) {
        let digest = node.view_digest_hex(cfg.target_site_id);
        *digest_counts.entry(digest).or_insert(0) += 1;
    }
    let unique_honest_view_digests = digest_counts.len();
    let majority = digest_counts.values().copied().max().unwrap_or(0);
    let honest_nodes_off_majority_view = honest_count.saturating_sub(majority);

    let mut total_accepted_ops = 0u64;
    let mut total_rejected_pow = 0u64;
    let mut total_rejected_sybil = 0u64;
    let mut total_rejected_rate_limit = 0u64;
    for node in &nodes {
        total_accepted_ops = total_accepted_ops.saturating_add(node.stats.accepted_ops);
        total_rejected_pow = total_rejected_pow.saturating_add(node.stats.rejected_pow);
        total_rejected_sybil = total_rejected_sybil.saturating_add(node.stats.rejected_sybil);
        total_rejected_rate_limit =
            total_rejected_rate_limit.saturating_add(node.stats.rejected_rate_limit);
    }

    let avg_idle_bytes_per_sec_observed = if idle_rounds == 0 || honest_count == 0 {
        0
    } else {
        (idle_bytes_accum / ((idle_rounds as u128) * (honest_count as u128))) as u64
    };

    DefederationSimulationResult {
        converged: unique_honest_view_digests == 1,
        rounds_executed,
        unique_honest_view_digests,
        honest_nodes_off_majority_view,
        generated_ops,
        delivered_ops,
        repair_messages,
        total_accepted_ops,
        total_rejected_pow,
        total_rejected_sybil,
        total_rejected_rate_limit,
        max_idle_bytes_per_sec_observed,
        avg_idle_bytes_per_sec_observed,
        idle_budget_bytes_per_sec,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lww_merge_is_commutative_and_idempotent_for_views() {
        let mut a = LensGraph::default();
        let mut b = LensGraph::default();

        a.update_follow(1, 2, Lamport::new(1, 1), true);
        a.update_content(2, 0, Lamport::new(2, 1), true);

        b.update_follow(1, 3, Lamport::new(1, 2), true);
        b.update_content(3, 0, Lamport::new(2, 2), true);

        let mut ab = a.clone();
        ab.merge_from(&b);
        let mut ba = b.clone();
        ba.merge_from(&a);

        assert_eq!(ab.view_digest_hex(1), ba.view_digest_hex(1));

        let before = ab.view_digest_hex(1);
        ab.merge_from(&ab.clone());
        assert_eq!(before, ab.view_digest_hex(1));
    }

    #[test]
    fn guard_rejects_spam_and_sybil_pressure() {
        let guard = DefederationGuardConfig {
            base_pow_bits: 6,
            trusted_pow_bits: 2,
            max_ops_per_origin_per_round: 4,
            max_new_origins_per_host_per_round: 1,
            max_pending_per_origin: 16,
        };
        let mut node = DefederationNode::new(1, 0, 1, HashSet::from([1]), guard);

        // Invalid PoW spam.
        let spam = make_invalid_spam_op(999, 1, 7, 999);
        node.ingest_batch(1, vec![spam]);

        // Sybil with valid PoW but blocked by new-origin host cap.
        let sybil_a = make_sybil_op(5000, 1, 9, 999, node.guard_cfg.base_pow_bits);
        let sybil_b = make_sybil_op(5001, 1, 9, 999, node.guard_cfg.base_pow_bits);
        node.ingest_batch(1, vec![sybil_a, sybil_b]);

        assert!(node.stats.rejected_pow > 0);
        assert!(node.stats.rejected_sybil > 0);
    }

    #[test]
    fn ten_k_two_host_defederation_converges() {
        let cfg = DefederationSimulationConfig {
            node_count: 10_000,
            writer_count: 64,
            malicious_nodes: 256,
            rounds: 12,
            settle_rounds: 20,
            fanout: 10,
            honest_write_ops_per_round: 64,
            spam_ops_per_malicious_round: 1,
            sybil_ops_per_malicious_round: 1,
            max_gossip_batch: 32,
            target_site_id: 1,
            trusted_seed_origins: 64,
            guard: DefederationGuardConfig {
                base_pow_bits: 8,
                trusted_pow_bits: 4,
                max_ops_per_origin_per_round: 96,
                max_new_origins_per_host_per_round: 0,
                max_pending_per_origin: 128,
            },
            idle_gate: IdleBandwidthGateConfig {
                max_idle_bytes_per_sec: 100 * 1024,
                beacon_bytes: 96,
                op_bytes_estimate: 160,
                min_repair_peers: 4,
                max_repair_peers: 16,
                huge_swarm_threshold: 100_000,
            },
        };

        let out = run_defederation_simulation(&cfg);
        println!("10k simulation result: {:?}", out);
        assert!(
            out.converged,
            "expected convergence, got {:?}",
            out
        );
        assert!(out.total_rejected_pow > 0);
        assert!(out.total_rejected_sybil > 0);
        assert!(out.max_idle_bytes_per_sec_observed <= out.idle_budget_bytes_per_sec);
    }

    #[test]
    fn idle_gate_stays_under_100kib_for_normal_swarm() {
        let cfg = DefederationSimulationConfig {
            node_count: 2_000,
            writer_count: 32,
            malicious_nodes: 64,
            rounds: 8,
            settle_rounds: 8,
            fanout: 12,
            honest_write_ops_per_round: 32,
            spam_ops_per_malicious_round: 1,
            sybil_ops_per_malicious_round: 1,
            max_gossip_batch: 16,
            target_site_id: 1,
            trusted_seed_origins: 8,
            guard: DefederationGuardConfig::default(),
            idle_gate: IdleBandwidthGateConfig::default(),
        };
        let out = run_defederation_simulation(&cfg);
        println!("idle gate simulation result: {:?}", out);
        assert!(
            out.honest_nodes_off_majority_view <= 1,
            "unexpected drift under idle gate: {:?}",
            out
        );
        assert!(out.max_idle_bytes_per_sec_observed <= 100 * 1024);
    }
}
