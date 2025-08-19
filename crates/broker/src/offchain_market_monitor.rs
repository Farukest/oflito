use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use alloy::network::Ethereum;
use alloy::primitives::{Address, U256, Bytes};
use alloy::providers::Provider;
use alloy::signers::{local::PrivateKeySigner, Signer};
use boundless_market::order_stream_client::{order_stream, OrderStreamClient};
use futures_util::StreamExt;
use anyhow::{Context, Result};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use boundless_market::{
    contracts::{
        boundless_market::BoundlessMarketService, IBoundlessMarket,
    },
};
use alloy::consensus::Transaction;
use crate::config::ConfigLock;
use crate::order_monitor::OrderMonitorErr;
use crate::provers::ProverObj;
use chrono::{DateTime, Utc};
use serde_json::json;
use crate::{chain_monitor::ChainMonitorService, db::{DbError, DbObj},
            errors::{impl_coded_debug, CodedError}, task::{RetryRes, RetryTask, SupervisorErr},
            FulfillmentType, OrderRequest, OrderStateChange, storage::{upload_image_uri, upload_input_uri}, now_timestamp};
use crate::market_monitor::MarketMonitorErr;
use alloy::{
    network::{eip2718::Encodable2718, EthereumWallet, TransactionBuilder},
    providers::ProviderBuilder,
    rpc::types::TransactionRequest,
    primitives::TxKind,
    consensus::{TxEip1559, TxEnvelope},
    sol,
    sol_types::SolCall,
};
use alloy::consensus::SignableTransaction;
use alloy_primitives::Signature;
use boundless_market::ProofRequest;

#[derive(Error)]
pub enum OffchainMarketMonitorErr {
    #[error("WebSocket error: {0:?}")]
    WebSocketErr(anyhow::Error),

    #[error("{code} Receiver dropped", code = self.code())]
    ReceiverDropped,

    #[error("{code} Unexpected error: {0:?}", code = self.code())]
    UnexpectedErr(#[from] anyhow::Error),

    #[error("{code} Database error: {0:?}", code = self.code())]
    DatabaseErr(DbError),
}

impl_coded_debug!(OffchainMarketMonitorErr);

impl CodedError for OffchainMarketMonitorErr {
    fn code(&self) -> &str {
        match self {
            OffchainMarketMonitorErr::WebSocketErr(_) => "[B-OMM-001]",
            OffchainMarketMonitorErr::ReceiverDropped => "[B-OMM-002]",
            OffchainMarketMonitorErr::UnexpectedErr(_) => "[B-OMM-500]",
            OffchainMarketMonitorErr::DatabaseErr(_) => "[B-OMM-003]",
        }
    }
}

// Global cache'ler - Node.js'teki gibi
static CACHED_CHAIN_ID: AtomicU64 = AtomicU64::new(0);
static CURRENT_NONCE: AtomicU64 = AtomicU64::new(0);

pub struct OffchainMarketMonitor<P> {
    client: OrderStreamClient,
    signer: PrivateKeySigner,
    new_order_tx: tokio::sync::mpsc::Sender<Box<OrderRequest>>,
    prover_addr: Address,
    provider: Arc<P>,
    db: DbObj,
    prover: ProverObj,
    config: ConfigLock,
}

impl<P> OffchainMarketMonitor<P> where
    P: Provider<Ethereum> + 'static + Clone,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: OrderStreamClient,
        signer: PrivateKeySigner,
        new_order_tx: tokio::sync::mpsc::Sender<Box<OrderRequest>>,
        prover_addr: Address,
        provider: Arc<P>,
        db: DbObj,
        prover: ProverObj,
        config: ConfigLock,
    ) -> Self {
        Self {
            client, signer,
            new_order_tx,
            prover_addr,
            provider,
            db,
            prover,
            config
        }
    }

    fn format_time(dt: DateTime<Utc>) -> String {
        dt.format("%H:%M:%S%.3f").to_string()
    }

    async fn monitor_orders(
        client: OrderStreamClient,
        signer: PrivateKeySigner,
        new_order_tx: tokio::sync::mpsc::Sender<Box<OrderRequest>>,
        cancel_token: CancellationToken,
        prover_addr: Address,
        provider: Arc<P>,
        db_obj: DbObj,
        prover: ProverObj,
        config: ConfigLock,
    ) -> Result<(), OffchainMarketMonitorErr> {
        let socket =
            client.connect_async(&signer).await.map_err(OffchainMarketMonitorErr::WebSocketErr)?;

        let mut stream = order_stream(socket);

        // Config değerlerini önceden oku
        let (allowed_requestors_opt, max_capacity, lockin_priority_gas, min_allowed_lock_timeout_secs) = {
            let locked_conf = config.lock_all().context("Failed to read config")?;
            (
                locked_conf.market.allow_requestor_addresses.clone(),
                Some(1u32),
                locked_conf.market.lockin_priority_gas,
                locked_conf.market.min_lock_out_time,
            )
        };

        let http_rpc_url = {
            let conf = config.lock_all().context("Failed to read config")?;
            conf.market.my_rpc_url.clone()
        };

        // Chain ID ve initial nonce'u cache'le
        let chain_id = 8453u64;
        CACHED_CHAIN_ID.store(chain_id, Ordering::Relaxed);

        let initial_nonce = provider
            .get_transaction_count(signer.address())
            .pending()
            .await
            .context("Failed to get transaction count")?;

        CURRENT_NONCE.store(initial_nonce, Ordering::Relaxed);

        tracing::info!("✅ Cache initialized - ChainId: {}, Initial Nonce: {}", chain_id, initial_nonce);

        let mut is_processing = false;

        loop {
            tokio::select! {
                order_data = stream.next() => {
                    match order_data {
                        Some(order_data) => {
                            if is_processing {
                                tracing::warn!("⏳ Zaten bir transaction işleniyor, yeni order beklemede...");
                                continue;
                            }

                            let request_id = order_data.order.request.id;
                            let client_addr = order_data.order.request.client_address();



                            // İzin verilen adres kontrolü
                            if let Some(ref allow_addresses) = allowed_requestors_opt {
                                if !allow_addresses.contains(&client_addr) {
                                    // tracing::debug!("🚫 Client not in allowed requestors, skipping request: 0x{:x}", request_id);
                                    continue;
                                }
                            }

                            tracing::info!("THIS IS THE ORDER ID LISTENED ::::::::::::: 0x{:x} : ", request_id);

                            // Lock timeout kontrolü
                            if (order_data.order.request.offer.lockTimeout as u64) < min_allowed_lock_timeout_secs {
                                tracing::info!(
                                    "Skipping order {}: Lock Timeout ({} seconds) is less than minimum required ({} seconds).",
                                    order_data.order.request.id,
                                    order_data.order.request.offer.lockTimeout,
                                    min_allowed_lock_timeout_secs
                                );
                                continue;
                            }

                            // Kapasite kontrolü
                            if let Some(max_capacity) = max_capacity {
                                match db_obj.get_committed_orders().await {
                                    Ok(committed_orders) => {
                                        let committed_count = committed_orders.len();
                                        if committed_count as u32 >= max_capacity {
                                            tracing::debug!("Max capacity reached ({}), skipping request: 0x{:x}", max_capacity, request_id);
                                            continue;
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!("DB error fetching committed orders: {:?}", e);
                                        continue;
                                    }
                                }
                            }

                            is_processing = true;
                            tracing::info!("🔄 Processing transaction for request: 0x{:x}", request_id);

                            // ✅ CRITICAL FIX: İşlem sonucunu düzgün handle et
                            match Self::send_private_transaction(
                                &order_data,
                                &signer,
                                client.boundless_market_address,
                                http_rpc_url.clone(),
                                lockin_priority_gas.unwrap_or(0),
                                prover_addr,
                                provider.clone(),
                            ).await {
                                Ok(lock_block) => {
                                    tracing::info!("🔒 LOCK SUCCESS! Request: 0x{:x}, Block: {}", request_id, lock_block);

                                    // Block timestamp al
                                    let lock_timestamp = match provider
                                        .get_block_by_number(lock_block.into())
                                        .await
                                    {
                                        Ok(Some(block)) => block.header.timestamp,
                                        Ok(None) => {
                                            tracing::error!("Block {} not found", lock_block);
                                            is_processing = false;
                                            continue;
                                        }
                                        Err(e) => {
                                            tracing::error!("Failed to get block {}: {:?}", lock_block, e);
                                            is_processing = false;
                                            continue;
                                        }
                                    };

                                    // Lock price hesapla
                                    let lock_price = match order_data.order.request.offer.price_at(lock_timestamp) {
                                        Ok(price) => price,
                                        Err(e) => {
                                            tracing::error!("Failed to calculate lock price: {:?}", e);
                                            is_processing = false;
                                            continue;
                                        }
                                    };

                                    // Order oluştur
                                    let new_order = OrderRequest::new(
                                        order_data.order.request,
                                        order_data.order.signature.as_bytes().into(),
                                        FulfillmentType::LockAndFulfill,
                                        client.boundless_market_address,
                                        client.chain_id,
                                    );

                                    // DB'ye başarılı lock'ı kaydet
                                    if let Err(e) = db_obj.insert_accepted_request(&new_order, lock_price.clone()).await {
                                        tracing::error!("FATAL: Failed to insert accepted request: {:?}", e);
                                    } else {
                                        tracing::info!("✅ Lock successful, order saved with price: {}", lock_price);
                                    }
                                }
                                Err(err) => {
                                    // ✅ CRITICAL: Transaction hatası - ama önce kontrol et ki gerçekten başarısız mı?
                                    tracing::error!("❌ Transaction error for request: 0x{:x}, error: {}", request_id, err);

                                    // ✅ DOUBLE LOCKING PREVENTION: Chain'de lock var mı kontrol et
                                    match Self::check_if_already_locked(&provider, client.boundless_market_address, U256::from(request_id)).await {
                                        Ok(true) => {
                                            tracing::warn!("⚠️ Transaction error occurred, but request 0x{:x} is already locked on-chain!", request_id);

                                            // Zaten locked ise, DB'ye kaydet (çünkü lock başarılı)
                                            let current_block = match provider.get_block_number().await {
                                                Ok(block_num) => block_num,
                                                Err(e) => {
                                                    tracing::error!("Failed to get current block number: {:?}", e);
                                                    is_processing = false;
                                                    continue;
                                                }
                                            };

                                            let lock_timestamp = match provider
                                                .get_block_by_number(current_block.into())
                                                .await
                                            {
                                                Ok(Some(block)) => block.header.timestamp,
                                                Ok(None) => {
                                                    tracing::error!("Current block {} not found", current_block);
                                                    is_processing = false;
                                                    continue;
                                                }
                                                Err(e) => {
                                                    tracing::error!("Failed to get current block {}: {:?}", current_block, e);
                                                    is_processing = false;
                                                    continue;
                                                }
                                            };

                                            let lock_price = match order_data.order.request.offer.price_at(lock_timestamp) {
                                                Ok(price) => price,
                                                Err(e) => {
                                                    tracing::error!("Failed to calculate lock price for recovered lock: {:?}", e);
                                                    is_processing = false;
                                                    continue;
                                                }
                                            };

                                            let new_order = OrderRequest::new(
                                                order_data.order.request,
                                                order_data.order.signature.as_bytes().into(),
                                                FulfillmentType::LockAndFulfill,
                                                client.boundless_market_address,
                                                client.chain_id,
                                            );

                                            if let Err(e) = db_obj.insert_accepted_request(&new_order, lock_price).await {
                                                tracing::error!("FATAL: Failed to insert accepted request after chain verification: {:?}", e);
                                            } else {
                                                tracing::info!("✅ Request 0x{:x} found locked on-chain, saved to DB", request_id);
                                            }
                                        }
                                        Ok(false) => {
                                            tracing::info!("✅ Request 0x{:x} confirmed NOT locked on-chain, adding to skipped", request_id);

                                            // Gerçekten başarısız - skipped olarak kaydet
                                            let new_order = OrderRequest::new(
                                                order_data.order.request,
                                                order_data.order.signature.as_bytes().into(),
                                                FulfillmentType::LockAndFulfill,
                                                client.boundless_market_address,
                                                client.chain_id,
                                            );

                                            if let Err(e) = db_obj.insert_skipped_request(&new_order).await {
                                                tracing::error!("Failed to insert skipped request: {:?}", e);
                                            }
                                        }
                                        Err(check_err) => {
                                            tracing::error!("Failed to check lock status for 0x{:x}: {:?}", request_id, check_err);

                                            // Chain kontrol edemediysek, güvenli tarafta kal - skipped olarak kaydet
                                            let new_order = OrderRequest::new(
                                                order_data.order.request,
                                                order_data.order.signature.as_bytes().into(),
                                                FulfillmentType::LockAndFulfill,
                                                client.boundless_market_address,
                                                client.chain_id,
                                            );

                                            if let Err(e) = db_obj.insert_skipped_request(&new_order).await {
                                                tracing::error!("Failed to insert skipped request: {:?}", e);
                                            }
                                        }
                                    }
                                }
                            }

                            is_processing = false;
                            tracing::info!("✅ Transaction processing completed for request: 0x{:x}", request_id);
                        }
                        None => {
                            return Err(OffchainMarketMonitorErr::WebSocketErr(anyhow::anyhow!(
                                "Offchain order stream websocket exited, polling failed"
                            )));
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    return Ok(());
                }
            }
        }
    }

    // ✅ NEW: Chain'de request'in lock'lanıp lock'lanmadığını kontrol et
    async fn check_if_already_locked(
        provider: &Arc<P>,
        contract_address: Address,
        request_id: U256,
    ) -> Result<bool, anyhow::Error> {
        tracing::debug!("🔍 Checking if request 0x{:x} is already locked on-chain", request_id);

        // ✅ Orijinal kodunuzdaki gibi requestIsLocked metodunu kullan
        let call = IBoundlessMarket::requestIsLockedCall {
            requestId: request_id
        };

        let call_request = alloy::rpc::types::TransactionRequest::default()
            .to(contract_address)
            .input(call.abi_encode().into());

        let result = provider.call(call_request).await
            .context("Failed to call requestIsLocked")?;

        // ABI decode the boolean result
        let is_locked = IBoundlessMarket::requestIsLockedCall::abi_decode_returns(&result)
            .context("Failed to decode requestIsLocked result")?;

        tracing::debug!("🔍 Request 0x{:x} lock status: {}", request_id, is_locked);
        Ok(is_locked)
    }

    async fn send_private_transaction(
        order_data: &boundless_market::order_stream_client::OrderData,
        signer: &PrivateKeySigner,
        contract_address: Address,
        http_rpc_url: String,
        lockin_priority_gas: u64,
        prover_addr: Address,
        provider: Arc<P>,
    ) -> Result<u64, anyhow::Error> {
        tracing::info!("🚀 SENDING PRIVATE TRANSACTION...");

        let chain_id = CACHED_CHAIN_ID.load(Ordering::Relaxed);
        let current_nonce = CURRENT_NONCE.load(Ordering::Relaxed);
        CURRENT_NONCE.store(current_nonce + 1, Ordering::Relaxed);
        tracing::info!("📦 Using nonce: {} (next will be: {})", current_nonce, current_nonce + 1);

        let lock_call = IBoundlessMarket::lockRequestCall {
            request: order_data.order.request.clone(),
            clientSignature: order_data.order.signature.as_bytes().into(),
        };

        let lock_calldata = lock_call.abi_encode();
        tracing::info!("🔍 ENCODED CALLDATA: 0x{}", hex::encode(&lock_calldata));

        let max_priority_fee_per_gas = lockin_priority_gas.into();
        let min_competitive_gas = 60_000_000u128;
        let base_fee = min_competitive_gas;
        let max_fee_per_gas = base_fee + max_priority_fee_per_gas;

        let tx = TxEip1559 {
            chain_id,
            nonce: current_nonce,
            gas_limit: 500_000u64,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            to: TxKind::Call(contract_address),
            value: U256::ZERO,
            input: lock_calldata.into(),
            access_list: Default::default(),
        };

        let signature_hash = tx.signature_hash();
        let signature = signer.sign_hash(&signature_hash).await?;
        let tx_signed = tx.into_signed(signature);
        let tx_envelope: TxEnvelope = tx_signed.into();
        let tx_encoded = tx_envelope.encoded_2718();

        // ✅ Transaction hash'i önceden hesapla (debugging için)
        let expected_tx_hash = tx_envelope.tx_hash();
        tracing::info!("🎯 Expected transaction hash: 0x{}", hex::encode(expected_tx_hash.as_slice()));

        let rclient = reqwest::Client::new();
        let response = rclient
            .post(http_rpc_url)
            .header("Content-Type", "application/json")
            .json(&json!({
                "jsonrpc": "2.0",
                "method": "eth_sendPrivateTransaction",
                "params": [{
                    "tx": format!("0x{}", hex::encode(&tx_encoded)),
                    "maxBlockNumber": "0x0",
                    "source": "customer_farukest"
                }],
                "id": 1
            }))
            .send()
            .await
            .context("Failed to send private transaction request")?;

        let result: serde_json::Value = response.json().await
            .context("Failed to parse response JSON")?;

        if let Some(error) = result.get("error") {
            let error_message = error.to_string().to_lowercase();

            // Nonce hatası durumunda senkronize et
            if error_message.contains("nonce") {
                tracing::error!("❌ Nonce hatası: {}", error);

                let fresh_nonce = provider
                    .get_transaction_count(signer.address())
                    .pending()
                    .await
                    .context("Failed to get fresh transaction count")?;

                CURRENT_NONCE.store(fresh_nonce, Ordering::Relaxed);
                tracing::info!("🔄 Nonce resynchronized from {} to {}", current_nonce, fresh_nonce);

                return Err(anyhow::anyhow!("Nonce error - resynchronized: {}", error));
            }

            // Diğer hatalar için nonce geri al
            let prev_nonce = current_nonce;
            CURRENT_NONCE.store(prev_nonce, Ordering::Relaxed);
            tracing::warn!("⚠️ Transaction failed, rolled back nonce to: {}", prev_nonce);

            return Err(anyhow::anyhow!("Private transaction failed: {}", error));
        }

        let tx_hash = result["result"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("No transaction hash in response"))?
            .to_string();

        let tx_hash_parsed = tx_hash.parse()
            .context("Failed to parse transaction hash")?;
        tracing::info!("🎯 Private transaction hash: {}", tx_hash);

        // ✅ CRITICAL: Receipt bekleme sırasında hata olursa, transaction yine de başarılı olmuş olabilir
        let tx_receipt = Self::wait_for_transaction_receipt(provider.clone(), tx_hash_parsed)
            .await
            .context("Failed to get transaction receipt")?;

        if !tx_receipt.status() {
            tracing::warn!("⚠️ İşlem {} REVERT oldu. Lock alınamadı.", tx_hash);
            // Transaction revert oldu - gerçekten başarısız
            return Err(anyhow::anyhow!("Transaction reverted on chain"));
        }

        let lock_block = tx_receipt.block_number
            .ok_or_else(|| anyhow::anyhow!("No block number in receipt"))?;

        tracing::info!("✅ İşlem {} başarıyla onaylandı. Lock alındı. Block: {}", tx_hash, lock_block);

        Ok(lock_block)
    }

    async fn wait_for_transaction_receipt(
        provider: Arc<P>,
        tx_hash: alloy_primitives::TxHash,
    ) -> Result<alloy::rpc::types::TransactionReceipt, anyhow::Error> {
        tracing::info!("⏳ İşlem onayını bekliyor: 0x{}", hex::encode(tx_hash.as_slice()));

        const RECEIPT_TIMEOUT: Duration = Duration::from_secs(60);
        const POLL_INTERVAL: Duration = Duration::from_millis(500);

        let start_time = Instant::now();

        loop {
            if start_time.elapsed() > RECEIPT_TIMEOUT {
                return Err(anyhow::anyhow!(
                    "Transaction 0x{} timeout after {} seconds",
                    hex::encode(tx_hash.as_slice()),
                    RECEIPT_TIMEOUT.as_secs()
                ));
            }

            match provider.get_transaction_receipt(tx_hash).await {
                Ok(Some(receipt)) => {
                    let elapsed = start_time.elapsed();
                    tracing::info!("✅ İşlem 0x{} başarıyla onaylandı ({:.1}s sonra)",
                                 hex::encode(tx_hash.as_slice()), elapsed.as_secs_f64());
                    return Ok(receipt);
                }
                Ok(None) => {
                    tokio::time::sleep(POLL_INTERVAL).await;
                    continue;
                }
                Err(e) => {
                    tracing::debug!("Error getting transaction receipt: {:?}, retrying...", e);
                    tokio::time::sleep(POLL_INTERVAL).await;
                    continue;
                }
            }
        }
    }
}

impl<P> RetryTask for OffchainMarketMonitor<P>
where
    P: Provider<Ethereum> + 'static + Clone,
{
    type Error = OffchainMarketMonitorErr;

    fn spawn(&self, cancel_token: CancellationToken) -> RetryRes<Self::Error> {
        let client = self.client.clone();
        let signer = self.signer.clone();
        let new_order_tx = self.new_order_tx.clone();
        let prover_addr = self.prover_addr;
        let provider = self.provider.clone();
        let db = self.db.clone();
        let prover = self.prover.clone();
        let config = self.config.clone();

        Box::pin(async move {
            tracing::info!("Starting up offchain market monitor");
            Self::monitor_orders(client, signer, new_order_tx, cancel_token, prover_addr, provider, db, prover, config)
                .await
                .map_err(SupervisorErr::Recover)?;
            Ok(())
        })
    }
}