# Archivist Feature Parity Matrix

Target upstream: `durability-labs/archivist-node` (`main` @ `ccbc23923e16542b8af0905fff21a42c80f10a9c`)

This matrix compares Neverust against the current upstream Archivist API and major feature areas.

## API parity (`/api/archivist/v1`)

| Endpoint | Upstream behavior | Neverust status |
|---|---|---|
| `GET /data` | List local manifest CIDs + manifest metadata | Implemented |
| `POST /data` | Upload dataset stream, return manifest CID | Implemented |
| `GET /data/{cid}` | Local-only stream download | Implemented |
| `DELETE /data/{cid}` | Delete block/dataset from local repo | Implemented (best-effort dataset cleanup) |
| `POST /data/{cid}/network` | Start async network fetch, return manifest info | Partial (returns local manifest info only) |
| `GET /data/{cid}/network/stream` | Stream download via local+network retrieval | Implemented (best-effort peer HTTP fallback) |
| `GET /data/{cid}/network/manifest` | Fetch manifest from network/local | Partial (local manifest only) |
| `GET /space` | Repo/quota usage summary | Implemented |
| `GET /spr` | Node SPR | Implemented |
| `GET /peerid` | Node Peer ID (text/json) | Implemented (`peerid` + `peer-id` aliases) |
| `GET /connect/{peerId}` | Dial/connect to peer | Stubbed (`501 Not Implemented`) |
| `GET /sales/slots` | Marketplace sales slots | Stubbed (`503 Persistence is not enabled`) |
| `GET /sales/slots/{slotId}` | Marketplace slot details | Stubbed (`503 Persistence is not enabled`) |
| `GET /sales/availability` | Get sale availability | Stubbed (`503 Persistence is not enabled`) |
| `POST /sales/availability` | Set sale availability | Stubbed (`503 Persistence is not enabled`) |
| `POST /storage/request/{cid}` | Create purchase/storage request | Stubbed (`503 Persistence is not enabled`) |
| `GET /storage/purchases` | List purchases | Stubbed (`503 Persistence is not enabled`) |
| `GET /storage/purchases/{id}` | Purchase details | Stubbed (`503 Persistence is not enabled`) |
| `GET /debug/info` | Node/debug state dump | Partial (minimal compatibility response) |
| `POST /debug/chronicles/loglevel` | Runtime log-level update | Stubbed (`501 Not Implemented`) |
| `GET /debug/peer/{peerId}` | DHT peer lookup (testing mode) | Stubbed (`501 Not Implemented`) |
| `POST /debug/testing/option/{key}/{value}` | Testing toggles | Stubbed (`501 Not Implemented`) |

## Core protocol parity

| Area | Upstream | Neverust status |
|---|---|---|
| Block codec semantics (`codex-block`, manifest codecs) | Implemented | Implemented |
| Chunker (64KiB default) | Implemented | Implemented |
| Manifest encoding/decoding | Implemented | Implemented |
| Archivist Merkle tree/root/proofs | Implemented | Implemented (core tree/proof support present) |
| Block exchange protocol compatibility | Implemented | Partial (BlockExc + BoTG path present; parity not complete) |
| Peer discovery (DiscV5 + DHT provider flows) | Implemented | Partial (basic discovery wiring present; provider lookup coverage incomplete) |
| Marketplace (sales/purchases/contracts) | Implemented | Missing (HTTP compatibility stubs only) |
| Erasure coding purchase/sales flows | Implemented | Missing |

## Highest-impact remaining gaps

1. Marketplace runtime (sales, purchasing, persistence) behind current stubs.
2. Real peer connect + discovery-backed provider retrieval parity (not just local cache / fallback HTTP).
3. Network manifest fetch parity (`/data/{cid}/network`, `/network/manifest`) currently local-only behavior.
4. Debug parity endpoints currently compatibility stubs.

