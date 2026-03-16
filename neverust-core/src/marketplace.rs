//! Persistent marketplace state and Archivist-compatible marketplace models.

use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::sync::RwLock;

fn deserialize_decimal_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(s) => {
            if s.trim().is_empty() {
                Err(D::Error::custom("decimal string must not be empty"))
            } else {
                Ok(s)
            }
        }
        serde_json::Value::Number(n) => Ok(n.to_string()),
        other => Err(D::Error::custom(format!(
            "expected decimal string or integer, got {}",
            other
        ))),
    }
}

fn decimal_gt_zero(value: &str) -> bool {
    let trimmed = value.trim();
    !trimmed.is_empty()
        && trimmed.chars().all(|c| c.is_ascii_digit())
        && trimmed.chars().any(|c| c != '0')
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SaleAvailabilityInput {
    #[serde(deserialize_with = "deserialize_decimal_string")]
    pub minimum_price_per_byte_per_second: String,
    #[serde(deserialize_with = "deserialize_decimal_string")]
    pub maximum_collateral_per_byte: String,
    pub maximum_duration: u64,
    #[serde(default)]
    pub available_until: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SaleAvailabilityRecord {
    #[serde(skip)]
    pub total_size: u64,
    pub minimum_price_per_byte_per_second: String,
    pub maximum_collateral_per_byte: String,
    pub maximum_duration: u64,
    pub available_until: Option<i64>,
    #[serde(skip)]
    pub updated_at: u64,
}

impl SaleAvailabilityRecord {
    pub fn from_input(
        input: SaleAvailabilityInput,
        default_total_size: u64,
    ) -> Result<Self, String> {
        if default_total_size == 0 {
            return Err("not enough node storage quota available".to_string());
        }
        if input.maximum_duration == 0 {
            return Err("maximumDuration must be larger than zero".to_string());
        }
        if !decimal_gt_zero(&input.minimum_price_per_byte_per_second) {
            return Err("minimumPricePerBytePerSecond must be larger than zero".to_string());
        }
        if !decimal_gt_zero(&input.maximum_collateral_per_byte) {
            return Err("maximumCollateralPerByte must be larger than zero".to_string());
        }
        if let Some(available_until) = input.available_until {
            if available_until < 0 {
                return Err("availableUntil must not be negative".to_string());
            }
        }

        Ok(Self {
            total_size: default_total_size,
            minimum_price_per_byte_per_second: input.minimum_price_per_byte_per_second,
            maximum_collateral_per_byte: input.maximum_collateral_per_byte,
            maximum_duration: input.maximum_duration,
            available_until: input.available_until,
            updated_at: now_unix_secs(),
        })
    }

    pub fn to_input(&self) -> SaleAvailabilityInput {
        SaleAvailabilityInput {
            minimum_price_per_byte_per_second: self.minimum_price_per_byte_per_second.clone(),
            maximum_collateral_per_byte: self.maximum_collateral_per_byte.clone(),
            maximum_duration: self.maximum_duration,
            available_until: self.available_until,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageRequestInput {
    pub duration: u64,
    #[serde(deserialize_with = "deserialize_decimal_string")]
    pub proof_probability: String,
    #[serde(deserialize_with = "deserialize_decimal_string")]
    pub price_per_byte_per_second: String,
    #[serde(deserialize_with = "deserialize_decimal_string")]
    pub collateral_per_byte: String,
    pub expiry: u64,
    #[serde(default)]
    pub nodes: Option<u64>,
    #[serde(default)]
    pub tolerance: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StorageAskRecord {
    pub slots: u64,
    pub slot_size: u64,
    pub duration: u64,
    pub proof_probability: String,
    pub price_per_byte_per_second: String,
    pub collateral_per_byte: String,
    pub max_slot_loss: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StorageContentRecord {
    pub cid: String,
    pub merkle_root: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StorageRequestRecord {
    pub id: String,
    pub client: String,
    pub ask: StorageAskRecord,
    pub content: StorageContentRecord,
    pub expiry: u64,
    pub nonce: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PurchaseState {
    Cancelled,
    Errored,
    Failed,
    Finished,
    Pending,
    Started,
    Submitted,
    Unknown,
}

impl PurchaseState {
    pub fn as_api_str(&self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::Errored => "errored",
            Self::Failed => "failed",
            Self::Finished => "finished",
            Self::Pending => "pending",
            Self::Started => "started",
            Self::Submitted => "submitted",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SalesSlotState {
    SaleCancelled,
    SaleDownloading,
    SaleErrored,
    SaleFailed,
    SaleFilled,
    SaleFilling,
    SaleFinished,
    SaleIgnored,
    SaleInitialProving,
    SalePayout,
    SalePreparing,
    SaleProving,
    SaleUnknown,
}

impl SalesSlotState {
    pub fn as_api_str(&self) -> &'static str {
        match self {
            Self::SaleCancelled => "SaleCancelled",
            Self::SaleDownloading => "SaleDownloading",
            Self::SaleErrored => "SaleErrored",
            Self::SaleFailed => "SaleFailed",
            Self::SaleFilled => "SaleFilled",
            Self::SaleFilling => "SaleFilling",
            Self::SaleFinished => "SaleFinished",
            Self::SaleIgnored => "SaleIgnored",
            Self::SaleInitialProving => "SaleInitialProving",
            Self::SalePayout => "SalePayout",
            Self::SalePreparing => "SalePreparing",
            Self::SaleProving => "SaleProving",
            Self::SaleUnknown => "SaleUnknown",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PurchaseRecord {
    pub request_id: String,
    pub request: StorageRequestRecord,
    pub state: PurchaseState,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SalesSlotRecord {
    pub id: String,
    pub state: SalesSlotState,
    pub request_id: String,
    pub slot_index: u64,
    pub request: StorageRequestRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct MarketplaceStateFile {
    pub availability: Option<SaleAvailabilityRecord>,
    pub purchases: BTreeMap<String, PurchaseRecord>,
    pub slots: BTreeMap<String, SalesSlotRecord>,
    pub next_slot_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActiveSlotResponse {
    pub id: String,
    pub request: StorageRequestRecord,
    pub slot_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SalesSlotResponse {
    pub state: String,
    pub request_id: String,
    pub slot_index: u64,
    pub request: StorageRequestRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PurchaseResponse {
    pub request_id: String,
    pub request: Option<StorageRequestRecord>,
    pub state: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct MarketplaceRuntimeInfo {
    pub persistence_enabled: bool,
    pub quota_max_bytes: usize,
    pub eth_provider: Option<String>,
    pub eth_account: Option<String>,
    pub marketplace_address: Option<String>,
    pub contracts_addresses: Option<serde_json::Value>,
    pub validator: bool,
    pub prover: bool,
}

#[derive(Clone)]
pub struct MarketplaceStore {
    path: PathBuf,
    inner: Arc<RwLock<MarketplaceStateFile>>,
}

impl MarketplaceStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let state = match fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice::<MarketplaceStateFile>(&bytes).unwrap_or_default(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                MarketplaceStateFile::default()
            }
            Err(err) => return Err(err),
        };

        Ok(Self {
            path,
            inner: Arc::new(RwLock::new(state)),
        })
    }

    pub async fn availability(&self) -> Option<SaleAvailabilityRecord> {
        self.inner.read().await.availability.clone()
    }

    pub async fn set_availability(
        &self,
        input: SaleAvailabilityInput,
        default_total_size: u64,
    ) -> Result<SaleAvailabilityRecord, String> {
        let record = SaleAvailabilityRecord::from_input(input, default_total_size)?;
        let mut state = self.inner.write().await;
        state.availability = Some(record.clone());
        self.persist_locked(&state)
            .await
            .map_err(|e| format!("failed to persist marketplace availability: {}", e))?;
        Ok(record)
    }

    pub async fn list_purchase_ids(&self) -> Vec<String> {
        self.inner.read().await.purchases.keys().cloned().collect()
    }

    pub async fn get_purchase(&self, id: &str) -> Option<PurchaseRecord> {
        self.inner.read().await.purchases.get(id).cloned()
    }

    pub async fn list_active_slots(&self) -> Vec<ActiveSlotResponse> {
        self.inner
            .read()
            .await
            .slots
            .values()
            .map(|slot| ActiveSlotResponse {
                id: slot.id.clone(),
                request: slot.request.clone(),
                slot_index: slot.slot_index,
            })
            .collect()
    }

    pub async fn get_slot(&self, id: &str) -> Option<SalesSlotResponse> {
        self.inner
            .read()
            .await
            .slots
            .get(id)
            .map(|slot| SalesSlotResponse {
                state: slot.state.as_api_str().to_string(),
                request_id: slot.request_id.clone(),
                slot_index: slot.slot_index,
                request: slot.request.clone(),
            })
    }

    pub async fn reserve_request(
        &self,
        cid: String,
        request: StorageRequestInput,
        slot_size: u64,
        client_eth_address: Option<String>,
    ) -> Result<String, String> {
        if request.duration == 0 {
            return Err("duration must be greater than zero".to_string());
        }
        if request.expiry == 0 || request.expiry >= request.duration {
            return Err(
                "Expiry must be greater than zero and less than the request's duration".to_string(),
            );
        }
        if !decimal_gt_zero(&request.proof_probability) {
            return Err("Proof probability must be greater than zero".to_string());
        }
        if !decimal_gt_zero(&request.collateral_per_byte) {
            return Err("Collateral per byte must be greater than zero".to_string());
        }
        if !decimal_gt_zero(&request.price_per_byte_per_second) {
            return Err("Price per byte per second must be greater than zero".to_string());
        }

        let nodes = request.nodes.unwrap_or(3);
        let tolerance = request.tolerance.unwrap_or(1);
        if nodes < 3 {
            return Err("nodes must be at least 3".to_string());
        }
        if tolerance == 0 {
            return Err("Tolerance needs to be bigger then zero".to_string());
        }
        if tolerance > nodes {
            return Err(
                "Invalid parameters: `tolerance` cannot be greater than `nodes`".to_string(),
            );
        }

        let ec_k = nodes - tolerance;
        let ec_m = tolerance;
        if ec_k <= 1 || ec_k < ec_m {
            return Err(
                "Invalid parameters: parameters must satify `1 < (nodes - tolerance) ≥ tolerance`"
                    .to_string(),
            );
        }

        let request_id = random_hex_id();
        let client = client_eth_address
            .unwrap_or_else(|| "0x0000000000000000000000000000000000000000".to_string());
        let request_record = StorageRequestRecord {
            id: request_id.clone(),
            client,
            ask: StorageAskRecord {
                slots: nodes,
                slot_size,
                duration: request.duration,
                proof_probability: request.proof_probability,
                price_per_byte_per_second: request.price_per_byte_per_second,
                collateral_per_byte: request.collateral_per_byte.clone(),
                max_slot_loss: tolerance,
            },
            content: StorageContentRecord {
                cid: cid.clone(),
                // Neverust does not yet build the upstream verifiable request path here.
                merkle_root: cid,
            },
            expiry: request.expiry,
            nonce: random_hex_id(),
        };

        let purchase = PurchaseRecord {
            request_id: request_id.clone(),
            request: request_record.clone(),
            state: PurchaseState::Submitted,
            error: None,
        };

        let mut state = self.inner.write().await;
        let slot_index = state.next_slot_index;
        state.next_slot_index = state.next_slot_index.saturating_add(1);
        state.purchases.insert(request_id.clone(), purchase);

        if state.availability.is_some() {
            let slot = SalesSlotRecord {
                id: random_hex_id(),
                state: SalesSlotState::SaleDownloading,
                request_id: request_id.clone(),
                slot_index,
                request: request_record,
            };
            state.slots.insert(slot.id.clone(), slot);
        }

        self.persist_locked(&state)
            .await
            .map_err(|e| format!("failed to persist marketplace purchase: {}", e))?;

        Ok(request_id)
    }

    async fn persist_locked(&self, state: &MarketplaceStateFile) -> Result<(), std::io::Error> {
        let tmp_path = self.path.with_extension("tmp");
        let json = serde_json::to_vec_pretty(state)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        fs::write(&tmp_path, json).await?;
        fs::rename(&tmp_path, &self.path).await?;
        Ok(())
    }
}

fn random_hex_id() -> String {
    let mut bytes = [0u8; 16];
    use rand::RngCore;
    rand::thread_rng().fill_bytes(&mut bytes);
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(hex, "{:02x}", byte);
    }
    hex
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn marketplace_store_persists_availability_and_purchase() {
        let tmp = tempfile::tempdir().unwrap();
        let store = MarketplaceStore::open(tmp.path().join("marketplace.json"))
            .await
            .unwrap();

        let availability = store
            .set_availability(
                SaleAvailabilityInput {
                    minimum_price_per_byte_per_second: "5".to_string(),
                    maximum_collateral_per_byte: "9".to_string(),
                    maximum_duration: 3600,
                    available_until: Some(0),
                },
                2048,
            )
            .await
            .unwrap();
        assert_eq!(availability.total_size, 2048);

        let purchase_id = store
            .reserve_request(
                "bafytest".to_string(),
                StorageRequestInput {
                    duration: 3600,
                    proof_probability: "4".to_string(),
                    price_per_byte_per_second: "42".to_string(),
                    collateral_per_byte: "7".to_string(),
                    expiry: 300,
                    nodes: Some(3),
                    tolerance: Some(1),
                },
                99,
                Some("0xabc".to_string()),
            )
            .await
            .unwrap();
        let purchase = store.get_purchase(&purchase_id).await.unwrap();
        assert_eq!(purchase.state, PurchaseState::Submitted);

        let reloaded = MarketplaceStore::open(tmp.path().join("marketplace.json"))
            .await
            .unwrap();
        assert!(reloaded.availability().await.is_some());
        assert_eq!(reloaded.list_purchase_ids().await.len(), 1);
        assert_eq!(reloaded.list_active_slots().await.len(), 1);
    }
}
