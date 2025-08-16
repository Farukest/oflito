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
        // tracing::info!("Connecting to off-chain market: {}", client.base_url);

        let socket =
            client.connect_async(signer).await.map_err(OffchainMarketMonitorErr::WebSocketErr)?;

        let mut stream = order_stream(socket);
        // tracing::info!("OFFCHAIN START - monitor_orders - Subscribed to offchain Order stream");

        loop {
            tokio::select! {
            order_data = stream.next() => {
                match order_data {
                    Some(order_data) => {
                        let client = client.clone();
                        let prover_addr = prover_addr.clone();
                        let provider = provider.clone();
                        let db_obj = db_obj.clone();
                        let prover = prover.clone();
                        let config = config.clone();
                        let new_order_tx = new_order_tx.clone();
                        let signer = signer.clone();

                        tokio::spawn(async move {
                            // tracing::info!(
                            //     "Detected new order with stream id {:x}, request id: {:x}",
                            //     order_data.id,
                            //     order_data.order.request.id
                            // );


                            let request_id = order_data.order.request.id;

                            // Check if client is allowed (if filter is configured)
                            let (allowed_requestors_opt, lock_delay_ms) = {
                                let locked_conf = config.lock_all().context("Failed to read config")?;
                                (
                                    locked_conf.market.allow_requestor_addresses.clone(),
                                    locked_conf.market.lock_delay_ms
                                )
                            };


                            let client_addr = order_data.order.request.client_address();

                            if let Some(allow_addresses) = allowed_requestors_opt {
                                if !allow_addresses.contains(&client_addr) {
                                    tracing::debug!("🚫 Client not in allowed requestors, skipping");
                                    return Ok(());
                                }
                            }


                            let committed_orders = match db_obj.get_committed_orders().await {
                                Ok(list) => list,
                                Err(e) => {
                                    tracing::error!("DB error fetching committed orders: {:?}", e);
                                    return;
                                }
                            };

                            let committed_count = committed_orders.len();

                            let max_capacity = Some(1); // Konfigürasyondan da alınabilir
                            if let Some(max_capacity) = max_capacity {
                                if committed_count as u32 >= max_capacity {
                                    tracing::info!("committed_count as u32 >= max_capacity");
                                    tracing::info!("Committed orders count ({}) reached max concurrency limit ({}), skipping lock for order {:?}",
                                        committed_count,
                                        max_capacity,
                                        request_id
                                    );
                                    tracing::info!("return Ok(())");
                                    return Ok(()); // Yeni order locklama yapılmaz
                                }
                            }
                            // BoundlessMarketService oluştur
                            let boundless = BoundlessMarketService::new(client.boundless_market_address, provider.clone(), prover_addr.clone());


                            let mut new_order = OrderRequest::new(
                                order_data.order.request.clone(),
                                order_data.order.signature.as_bytes().into(),
                                FulfillmentType::LockAndFulfill,
                                client.boundless_market_address,
                                client.chain_id,
                            );

                            // Try to lock the request
                            let lockin_priority_gas = {
                                let locked_conf = config.lock_all().context("Failed to read config")?;
                                locked_conf.market.lockin_priority_gas
                            };

                            match boundless.lock_request(&order_data.order.request, order_data.order.signature.as_bytes().into(), lockin_priority_gas).await {
                                Ok(lock_block) => {
                                    tracing::info!("✅ Successfully locked request: 0x{:x} at block {}", order_data.order.request.id, lock_block);

                                    // RPC senkronizasyonu için küçük bir gecikme ekle
                                    tracing::info!("⏳⏳⏳⏳⏳⏳⏳⏳⏳ Waiting for RPC to sync lock block... ⏳⏳⏳⏳⏳⏳⏳⏳");
                                    tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await; // 500 ms bekleme

                                    // Calculate lock price and save to DB
                                    let lock_timestamp = provider
                                        .get_block_by_number(lock_block.into())
                                        .await
                                        .context("------------------------ lock_timestamp : Failed to get lock block.....")?
                                        .context("Lock block not found")?
                                        .header
                                        .timestamp;

                                    let lock_price = new_order
                                        .request
                                        .offer
                                        .price_at(lock_timestamp)
                                        .context("------------------------ lock_price : Failed to calculate lock price.....")?;

                                        // Confirmed data ile yeni order oluştur
                                        let mut updated_order = OrderRequest::new(
                                            order_data.order.request,
                                            order_data.order.clientSignature.clone(),
                                            FulfillmentType::LockAndFulfill,
                                            client.boundless_market_address,
                                            client.chain_id,
                                        );
                                        updated_order.target_timestamp = Some(updated_order.request.lock_expires_at());
                                        updated_order.expire_timestamp = Some(updated_order.request.expires_at());

                                        if let Err(e) = db_obj.insert_accepted_request(&updated_order, lock_price).await {
                                            tracing::error!("Failed to insert accepted request: {:?}", e);
                                        }

                                }
                                Err(err) => {
                                    tracing::info!("❌ Failed to lock request: 0x{:x}, error: {}", request_id, err);

                                    if let Err(e) = db_obj.insert_skipped_request(&new_order).await {
                                        tracing::info!("Failed to insert skipped request: {:?}", e);
                                    }
                                }
                            }

                            // JSON pretty print
                            // match serde_json::to_string_pretty(&new_order) {
                            //     Ok(json_str) => {
                            //         tracing::info!("New order JSON :\n{}", json_str);
                            //     }
                            //     Err(e) => {
                            //         tracing::error!("Failed to serialize order to JSON: {}", e);
                            //     }
                            // }

                            // İstersen kanala da gönderebilirsin, şu an yorumlu
                            // if let Err(e) = new_order_tx.send(Box::new(new_order)).await {
                            //     tracing::error!("Failed to send new off-chain order {} to OrderPicker: {}", order_id, e);
                            // }
                        });
                    }
                    None => {
                        // tracing::info!("NONEeeeeeeee");
                        return Err(OffchainMarketMonitorErr::WebSocketErr(anyhow::anyhow!(
                            "Offchain order stream websocket exited, polling failed"
                        )));
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                // tracing::info!("Offchain market monitor received cancellation, shutting down gracefully");
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
