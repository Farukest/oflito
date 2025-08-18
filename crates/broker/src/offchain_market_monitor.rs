// Copyright (c) 2025 RISC Zero, Inc.
//
// All rights reserved.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
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

        // ✅ Provider'ın built-in metodunu kullan - İLK NONCE
        let initial_nonce = provider
            .get_transaction_count(signer.address())
            .pending()
            .await
            .context("Failed to get transaction count")?;

        CURRENT_NONCE.store(initial_nonce, Ordering::Relaxed);

        tracing::info!("✅ Cache initialized - ChainId: {}, Initial Nonce: {}", chain_id, initial_nonce);

        // ✅ SERI İŞLEM İÇİN MUTEX/FLAG - PARALELLİKTEN KAÇINMAK İÇİN
        let mut is_processing = false;

        loop {
            tokio::select! {
                order_data = stream.next() => {
                    match order_data {
                        Some(order_data) => {
                            // ✅ PARALEL İŞLEM ENGELLEME - Bir transaction işleniyorsa yenisini bekle
                            if is_processing {
                                tracing::warn!("⏳ Zaten bir transaction işleniyor, yeni order beklemede...");
                                continue;
                            }

                            let request_id = order_data.order.request.id;
                            let client_addr = order_data.order.request.client_address();

                            // Hızlı filtreleme
                            tracing::info!("THIS IS THE ORDER ID LISTENED ::::::::::::: 0x{:x} : ", request_id);

                            // İzin verilen adres kontrolü
                            if let Some(ref allow_addresses) = allowed_requestors_opt {
                                if !allow_addresses.contains(&client_addr) {
                                    tracing::debug!("🚫 Client not in allowed requestors, skipping request: 0x{:x}", request_id);
                                    continue;
                                }
                            }

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

                            // Kapasite kontrolü - PARALEL SPAWN KALDIRDIK, SERİ YAPIYORUZ
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

                            // ✅ İŞLEM BAŞLATILIYOR - FLAG SET ET
                            is_processing = true;
                            tracing::info!("🔄 Processing transaction for request: 0x{:x}", request_id);

                            // ✅ PARALEL SPAWN KALDIRDIK - SERİ OLARAK İŞLE
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

                                    // ✅ DB'ye başarılı lock'ı kaydet
                                    if let Err(e) = db_obj.insert_accepted_request(&new_order, lock_price.clone()).await {
                                        tracing::error!("FATAL: Failed to insert accepted request: {:?}", e);
                                    } else {
                                        tracing::info!("✅ Lock successful, order saved with price: {}", lock_price);
                                    }
                                }
                                Err(err) => {
                                    tracing::error!("❌ Lock failed for request: 0x{:x}, error: {}", request_id, err);

                                    // ✅ Failed lock'ı kaydet
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

                            // ✅ İŞLEM BİTTİ - FLAG RESET ET
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

    async fn send_private_transaction(
        order_data: &boundless_market::order_stream_client::OrderData,
        signer: &PrivateKeySigner,
        contract_address: Address,
        http_rpc_url: String,
        lockin_priority_gas: u64,
        prover_addr: Address,
        provider: Arc<P>,
    ) -> Result<u64, anyhow::Error> {  // ✅ u64 (block number) return et
        tracing::info!("🚀 SENDING PRIVATE TRANSACTION...");

        // Cache'den değerleri al
        let chain_id = CACHED_CHAIN_ID.load(Ordering::Relaxed);

        // ✅ NONCE MANTIGI DEĞİŞTİRİLDİ - Node.js'teki gibi MEVCUT NONCE KULLAN
        let current_nonce = CURRENT_NONCE.load(Ordering::Relaxed);
        tracing::info!("📦 Using nonce: {}", current_nonce);

        // ✅ Doğru field name: clientSignature
        let lock_call = IBoundlessMarket::lockRequestCall {
            request: order_data.order.request.clone(),
            clientSignature: order_data.order.signature.as_bytes().into(),
        };

        // ✅ ABI encode et
        let lock_calldata = lock_call.abi_encode();

        // 🔍 CALLDATA'yı logla
        tracing::info!("🔍 ENCODED CALLDATA: 0x{}", hex::encode(&lock_calldata));
        tracing::info!("📏 CALLDATA Length: {} bytes", lock_calldata.len());

        // Method ID'yi ayrı logla (ilk 4 byte)
        if lock_calldata.len() >= 4 {
            tracing::info!("🎯 METHOD ID: 0x{}", hex::encode(&lock_calldata[0..4]));
            tracing::info!("📦 PARAMETERS: 0x{}", hex::encode(&lock_calldata[4..]));
        }

        // Gas ayarları
        let max_priority_fee_per_gas = lockin_priority_gas.into();
        let min_competitive_gas = 60_000_000u128;
        let base_fee = min_competitive_gas;
        let max_fee_per_gas = base_fee + max_priority_fee_per_gas;

        // Transaction oluştur
        let tx = TxEip1559 {
            chain_id,
            nonce: current_nonce,  // ✅ Mevcut nonce kullan
            gas_limit: 500_000u64,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            to: TxKind::Call(contract_address),
            value: U256::ZERO,
            input: lock_calldata.into(),
            access_list: Default::default(),
        };

        // Transaction'ı imzala
        let signature_hash = tx.signature_hash();
        let signature = signer.sign_hash(&signature_hash).await?;
        let tx_signed = tx.into_signed(signature);
        let tx_envelope: TxEnvelope = tx_signed.into();
        let tx_encoded = tx_envelope.encoded_2718();

        // Private transaction gönder
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
            .await?;

        let result: serde_json::Value = response.json().await?;

        if let Some(error) = result.get("error") {
            // ✅ NONCE HATA KONTROLÜ - Node.js'teki gibi
            let error_message = error.to_string().to_lowercase();
            if error_message.contains("nonce") {
                tracing::error!("❌ Nonce hatası: {}", error);
                // Nonce'u yeniden senkronize et
                let fresh_nonce = provider
                    .get_transaction_count(signer.address())
                    .pending()
                    .await
                    .context("Failed to get fresh transaction count")?;

                CURRENT_NONCE.store(fresh_nonce, Ordering::Relaxed);
                tracing::info!("🔄 Nonce yeniden senkronize edildi: {}", fresh_nonce);
            }
            return Err(anyhow::anyhow!("Private transaction failed: {}", error));
        }

        let tx_hash = result["result"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("No transaction hash in response"))?
            .to_string();

        tracing::info!("🔒 Private transaction sent: {}", tx_hash);

        // ✅ Transaction'ı bekle ve block number al
        let tx_hash_parsed = tx_hash.parse()?;
        let tx_receipt = provider
            .get_transaction_receipt(tx_hash_parsed)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Transaction receipt not found"))?;

        if !tx_receipt.status() {
            // ✅ REVERT DURUMUNDA DA NONCE ARTTIR - Node.js'teki gibi
            let next_nonce = CURRENT_NONCE.load(Ordering::Relaxed) + 1;
            CURRENT_NONCE.store(next_nonce, Ordering::Relaxed);
            tracing::warn!("⚠️ Transaction reverted but nonce consumed. Next nonce: {}", next_nonce);
            return Err(anyhow::anyhow!("Transaction failed on chain"));
        }

        let lock_block = tx_receipt.block_number
            .ok_or_else(|| anyhow::anyhow!("No block number in receipt"))?;

        // ✅ BAŞARILI TRANSACTION SONRASI NONCE ARTTIR - Node.js'teki gibi
        let next_nonce = CURRENT_NONCE.load(Ordering::Relaxed) + 1;
        CURRENT_NONCE.store(next_nonce, Ordering::Relaxed);
        tracing::info!("✅ LOCK SUCCESS at block: {}, Next nonce: {}", lock_block, next_nonce);

        // ✅ Block number return et
        Ok(lock_block)
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