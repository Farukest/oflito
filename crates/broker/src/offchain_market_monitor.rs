// Copyright (c) 2025 RISC Zero, Inc.
//
// All rights reserved.

use std::sync::Arc;
use std::time::Instant;
use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::signers::{local::PrivateKeySigner, Signer};
use boundless_market::order_stream_client::{order_stream, OrderStreamClient};
use futures_util::StreamExt;
use anyhow::{Context, Result};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use boundless_market::BoundlessMarketService;
use crate::config::ConfigLock;
use crate::order_monitor::OrderMonitorErr;
use crate::provers::ProverObj;
use chrono::{DateTime, Utc};
use crate::{chain_monitor::ChainMonitorService, db::{DbError, DbObj},
            errors::{impl_coded_debug, CodedError}, task::{RetryRes, RetryTask, SupervisorErr},
            FulfillmentType, OrderRequest, OrderStateChange, storage::{upload_image_uri, upload_input_uri}, now_timestamp};
use crate::market_monitor::MarketMonitorErr;

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

pub struct OffchainMarketMonitor<P> {
    client: OrderStreamClient,
    signer: PrivateKeySigner,
    new_order_tx: tokio::sync::mpsc::Sender<Box<OrderRequest>>,
    prover_addr : Address,
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
        prover_addr : Address,
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
        signer: &impl Signer,
        new_order_tx: tokio::sync::mpsc::Sender<Box<OrderRequest>>,
        cancel_token: CancellationToken,
        prover_addr : Address,
        provider: Arc<P>,
        db_obj: DbObj,
        prover: ProverObj,
        config: ConfigLock,
    ) -> Result<(), OffchainMarketMonitorErr> {
        let socket =
            client.connect_async(signer).await.map_err(OffchainMarketMonitorErr::WebSocketErr)?;

        let mut stream = order_stream(socket);

        // Config değerlerini önceden oku (her spawn'da okumak yerine)
        let (allowed_requestors_opt, max_capacity, lockin_priority_gas) = {
            let locked_conf = config.lock_all().context("Failed to read config").unwrap();
            (
                locked_conf.market.allow_requestor_addresses.clone(),
                Some(1u32), // Konfigürasyondan alınabilir
                locked_conf.market.lockin_priority_gas
            )
        };

        loop {
            tokio::select! {
            order_data = stream.next() => {
                match order_data {
                    Some(order_data) => {
                        let request_id = order_data.order.request.id;
                        let client_addr = order_data.order.request.client_address();

                        // 🚀 HIZLI FİLTRELEME: Async spawn yapmadan önce hızlı kontroller
                        tracing::info!("THIS IS THE ORDER ID LISTENED ::::::::::::: 0x{:x} : ", request_id);
                        // 1. İzin verilen adres kontrolü (eğer varsa)
                        if let Some(ref allow_addresses) = allowed_requestors_opt {
                            if !allow_addresses.contains(&client_addr) {
                                tracing::debug!("🚫 Client not in allowed requestors, skipping request: 0x{:x}", request_id);
                                continue; // Hızlıca skip et, spawn bile yapma
                            }
                        }

                        // Her spawn için değişkenleri klonla
                        let client_clone = client.clone();
                        let db_obj_clone = db_obj.clone();
                        let provider_clone = provider.clone();

                        tokio::spawn(async move {
                            // 2. Kapasite kontrolü - sadece DB'ye sorgu yap, config okuma
                            if let Some(max_capacity) = max_capacity {
                                match db_obj_clone.get_committed_orders().await {
                                    Ok(committed_orders) => {
                                        let committed_count = committed_orders.len();
                                        if committed_count as u32 >= max_capacity {
                                            tracing::debug!("Max capacity reached ({}), skipping request: 0x{:x}", max_capacity, request_id);
                                            return;
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!("DB error fetching committed orders: {:?}", e);
                                        return;
                                    }
                                }
                            }

                            // 🚀 HIZLI LOCK ATTEMPT: Mümkün olduğunca hızlı lock yapmaya çalış
                            let boundless = BoundlessMarketService::new(
                                client_clone.boundless_market_address,
                                provider_clone.clone(),
                                prover_addr.clone()
                            );


                            tracing::error!("SENDIING LOCK REQUEST BE REDYYYYYYYYYYYYY");
                            // Lock attempt - bu en kritik kısım, hiç gecikmeden yap
                            match boundless.lock_request(
                                &order_data.order.request,
                                order_data.order.signature.as_bytes(),
                                lockin_priority_gas
                            ).await {
                                Ok(lock_block) => {
                                    tracing::info!("🔒 LOCKED! Request: 0x{:x} at block {}", request_id, lock_block);

                                    // Lock sonrası işlemler - bunlar artık acil değil
                                    tokio::spawn(async move {
                                        // RPC sync beklemesi
                                        tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await; // 5s -> 3s

                                        // Lock price hesaplama ve DB kaydetme
                                        match provider_clone.get_block_by_number(lock_block.into()).await {
                                            Ok(Some(block)) => {
                                                let lock_timestamp = block.header.timestamp;

                                                match order_data.order.request.offer.price_at(lock_timestamp) {
                                                    Ok(lock_price) => {
                                                        let mut updated_order = OrderRequest::new(
                                                            order_data.order.request,
                                                            order_data.order.signature.as_bytes().into(),
                                                            FulfillmentType::LockAndFulfill,
                                                            client_clone.boundless_market_address,
                                                            client_clone.chain_id,
                                                        );
                                                        updated_order.target_timestamp = Some(updated_order.request.lock_expires_at());
                                                        updated_order.expire_timestamp = Some(updated_order.request.expires_at());

                                                        if let Err(e) = db_obj_clone.insert_accepted_request(&updated_order, lock_price).await {
                                                            tracing::error!("Failed to insert accepted request: {:?}", e);
                                                        }
                                                    }
                                                    Err(e) => {
                                                        tracing::error!("Failed to calculate lock price: {:?}", e);
                                                    }
                                                }
                                            }
                                            Ok(None) => {
                                                tracing::error!("Lock block not found: {}", lock_block);
                                            }
                                            Err(e) => {
                                                tracing::error!("Failed to get lock block: {:?}", e);
                                            }
                                        }
                                    });
                                }
                                Err(err) => {
                                    tracing::debug!("❌ Lock failed for request: 0x{:x}, error: {}", request_id, err);

                                    // Failed lock'ı kaydetme de ayrı task'ta yap
                                    tokio::spawn(async move {
                                        let new_order = OrderRequest::new(
                                            order_data.order.request,
                                            order_data.order.signature.as_bytes().into(),
                                            FulfillmentType::LockAndFulfill,
                                            client_clone.boundless_market_address,
                                            client_clone.chain_id,
                                        );

                                        if let Err(e) = db_obj_clone.insert_skipped_request(&new_order).await {
                                            tracing::error!("Failed to insert skipped request: {:?}", e);
                                        }
                                    });
                                }
                            }
                        });
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
            Self::monitor_orders(client, &signer, new_order_tx, cancel_token, prover_addr, provider, db, prover, config)
                .await
                .map_err(SupervisorErr::Recover)?;
            Ok(())
        })
    }
    // fn spawn(&self, _cancel_token: CancellationToken) -> RetryRes<Self::Error> {
    //     Box::pin(async { Ok(()) })
    // }
}
