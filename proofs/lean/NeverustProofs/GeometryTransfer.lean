import Mathlib
import NeverustProofs.StorageGeometry
import NeverustProofs.ProbeAllocation
import NeverustProofs.StripeClasses

/-!
Neverust geometry-transfer lane.

This module glues the existing storage proofs into a research-friendly bundle:

1) CID/path identity remains injective even when placement uses only a reduced
   geometry projection.
2) Affine counter offsets on probe start preserve deterministic first-free
   uniqueness (for finite occupancy).
3) MooseFS class mapping and first-free probe allocation compose into a total
   placement witness on bounded payload range.
-/

namespace NeverustProofs.GeometryTransfer

open NeverustProofs

noncomputable section

/-- Affine start update used for mutable split-pointer/counter routing. -/
def affineStart (base counter : ℕ) : ℕ := base + counter

theorem affineStart_injective_in_counter (base : ℕ) :
    Function.Injective (affineStart base) := by
  intro a b h
  unfold affineStart at h
  omega

/-- Full-CID filename identity keeps encode injective regardless of shard function. -/
theorem flatfs_geometry_identity_no_collision
    {Dir : Type}
    (shard : StorageGeometry.Rail16 → Dir)
    (c₁ c₂ : StorageGeometry.CID32) :
    StorageGeometry.flatfsEncode shard c₁ = StorageGeometry.flatfsEncode shard c₂ → c₁ = c₂ := by
  exact StorageGeometry.zero_collision_for_every_cid shard c₁ c₂

/-- Affine counter lane still has a unique first-free probe for finite occupancy. -/
theorem affine_probe_first_free_unique
    (S : Finset ℕ)
    (base counter step : ℕ)
    (hstep : 0 < step) :
    ∃! n,
      ProbeAllocation.probe (affineStart base counter) step n ∉ S ∧
      ∀ k < n, ProbeAllocation.probe (affineStart base counter) step k ∈ S := by
  simpa [affineStart] using
    (ProbeAllocation.zero_collision_first_free
      (S := S)
      (start := base + counter)
      (step := step)
      hstep)

/-- End-to-end totality for bounded payloads:
class selection is valid and probe allocation has a unique first free slot. -/
theorem class_and_probe_total
    (S : Finset ℕ)
    (payloadBytes base counter step : ℕ)
    (hmin : StripeClasses.minShardBytes ≤ payloadBytes)
    (hmax : payloadBytes ≤ StripeClasses.maxShardBytes)
    (hstep : 0 < step) :
    payloadBytes ≤ StripeClasses.classOfSize payloadBytes ∧
    StripeClasses.classOfSize payloadBytes ≤ StripeClasses.maxShardBytes ∧
    (∃! n,
      ProbeAllocation.probe (affineStart base counter) step n ∉ S ∧
      ∀ k < n, ProbeAllocation.probe (affineStart base counter) step k ∈ S) := by
  refine ⟨?_, ?_, ?_⟩
  · exact (StripeClasses.class_lane_covers_target_range hmin hmax).1
  · exact (StripeClasses.class_lane_covers_target_range hmin hmax).2
  · exact affine_probe_first_free_unique S base counter step hstep

end NeverustProofs.GeometryTransfer
