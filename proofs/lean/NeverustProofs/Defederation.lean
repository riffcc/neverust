import Mathlib

/-!
Neverust Defederation lane.

This file formalizes two core invariants used by Neverust Citadel mode:

1) Convergence via pointwise-`max` merge over versioned state.
2) Idle control-plane gate bounds:
   - normal swarms stay at fixed cap,
   - probe fanout is bounded by a small constant range.
-/

namespace NeverustProofs.Defederation

abbrev VersionState := Nat → Nat

/-- Pointwise LWW-style join. -/
def merge (a b : VersionState) : VersionState := fun k => max (a k) (b k)

theorem merge_comm (a b : VersionState) : merge a b = merge b a := by
  funext k
  simp [merge, max_comm]

theorem merge_assoc (a b c : VersionState) : merge (merge a b) c = merge a (merge b c) := by
  funext k
  simp [merge, max_assoc]

theorem merge_idem (a : VersionState) : merge a a = a := by
  funext k
  simp [merge]

/-- Pull-sync cannot decrease any local frontier coordinate. -/
theorem merge_ge_left (a b : VersionState) : ∀ k, a k ≤ merge a b k := by
  intro k
  simp [merge]

/-- Pull-sync cannot decrease any remote frontier coordinate either. -/
theorem merge_ge_right (a b : VersionState) : ∀ k, b k ≤ merge a b k := by
  intro k
  simp [merge]

/-- If `g` is an upper bound for both states, their merge also stays under `g`. -/
theorem merge_le_upper_bound
    (a b g : VersionState)
    (ha : ∀ k, a k ≤ g k)
    (hb : ∀ k, b k ≤ g k) :
    ∀ k, merge a b k ≤ g k := by
  intro k
  exact max_le (ha k) (hb k)

/-- If `g` dominates `s`, one merge step with `g` converges `s` to `g`. -/
theorem merge_to_upper_bound
    (s g : VersionState)
    (h : ∀ k, s k ≤ g k) :
    merge s g = g := by
  funext k
  have hk : s k ≤ g k := h k
  simp [merge, max_eq_right hk]

/-- Once converged to `g`, extra pull steps with `g` are stable. -/
theorem merge_stable_after_convergence
    (s g : VersionState)
    (h : ∀ k, s k ≤ g k) :
    merge (merge s g) g = g := by
  have hsg : merge s g = g := merge_to_upper_bound s g h
  rw [hsg, merge_idem]

/-- Control fanout policy: logarithmic intent, clamped to fixed band. -/
def repairPeers (swarmSize minPeers maxPeers : Nat) : Nat :=
  max minPeers (min maxPeers (Nat.log2 (swarmSize + 1)))

theorem repairPeers_le_max
    (swarmSize minPeers maxPeers : Nat)
    (hminmax : minPeers ≤ maxPeers) :
    repairPeers swarmSize minPeers maxPeers ≤ maxPeers := by
  unfold repairPeers
  exact max_le hminmax (min_le_left _ _)

theorem repairPeers_ge_min
    (swarmSize minPeers maxPeers : Nat) :
    minPeers ≤ repairPeers swarmSize minPeers maxPeers := by
  unfold repairPeers
  exact le_max_left _ _

/-- Idle budget policy used in runtime/simulation model. -/
def idleBudget (baseCap hugeThreshold swarmSize : Nat) : Nat :=
  if swarmSize ≤ hugeThreshold then
    baseCap
  else
    baseCap + Nat.log2 (swarmSize / (hugeThreshold + 1) + 1) * 8192

theorem idleBudget_normal_swarm
    (baseCap hugeThreshold swarmSize : Nat)
    (h : swarmSize ≤ hugeThreshold) :
    idleBudget baseCap hugeThreshold swarmSize = baseCap := by
  simp [idleBudget, h]

theorem idleBudget_ge_base
    (baseCap hugeThreshold swarmSize : Nat) :
    baseCap ≤ idleBudget baseCap hugeThreshold swarmSize := by
  unfold idleBudget
  split
  · simp
  · exact Nat.le_add_right _ _

end NeverustProofs.Defederation
