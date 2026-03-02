import Lake
open Lake DSL

package neverust_proofs

require mathlib from git
  "https://github.com/leanprover-community/mathlib4.git"

lean_lib NeverustProofs where
  roots := #[
    `NeverustProofs.StorageGeometry,
    `NeverustProofs.ProbeAllocation,
    `NeverustProofs.StripeClasses,
    `NeverustProofs.GeometryTransfer,
    `NeverustProofs.Defederation
  ]
