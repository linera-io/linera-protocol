// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use sqlx::postgres::PgPool;
use sqlx::Row;

use crate::models::*;

pub async fn get_blocks(pool: &PgPool, limit: i64, offset: i64) -> Result<Vec<BlockSummary>, sqlx::Error> {
    sqlx::query_as::<_, BlockSummary>(
        r#"
        SELECT hash, chain_id, height, timestamp, LENGTH(data) as size
        FROM blocks
        ORDER BY timestamp DESC
        LIMIT $1 OFFSET $2
        "#,
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await
}

pub async fn get_block_by_hash(pool: &PgPool, hash: &str) -> Result<Option<Block>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT hash, chain_id, height, data, timestamp
        FROM blocks
        WHERE hash = $1
        "#,
    )
    .bind(hash)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| {
        let data: Option<Vec<u8>> = r.get("data");
        Block {
            hash: r.get("hash"),
            chain_id: r.get("chain_id"),
            height: r.get("height"),
            timestamp: r.get("timestamp"),
            data: data.map(|d| BASE64.encode(&d)),
        }
    }))
}

pub async fn get_blocks_by_chain(
    pool: &PgPool,
    chain_id: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<BlockSummary>, sqlx::Error> {
    sqlx::query_as::<_, BlockSummary>(
        r#"
        SELECT hash, chain_id, height, timestamp, LENGTH(data) as size
        FROM blocks
        WHERE chain_id = $1
        ORDER BY height DESC
        LIMIT $2 OFFSET $3
        "#,
    )
    .bind(chain_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await
}

pub async fn get_incoming_bundles(pool: &PgPool, block_hash: &str) -> Result<Vec<IncomingBundle>, sqlx::Error> {
    sqlx::query_as::<_, IncomingBundle>(
        r#"
        SELECT id, block_hash, bundle_index, origin_chain_id, action,
               source_height, source_timestamp, source_cert_hash,
               transaction_index, created_at::text
        FROM incoming_bundles
        WHERE block_hash = $1
        ORDER BY bundle_index ASC
        "#,
    )
    .bind(block_hash)
    .fetch_all(pool)
    .await
}

pub async fn get_posted_messages(pool: &PgPool, bundle_id: i64) -> Result<Vec<PostedMessage>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT id, bundle_id, message_index, authenticated_signer, grant_amount,
               refund_grant_to, message_kind, message_type, application_id,
               system_message_type, system_target, system_amount, system_source,
               system_owner, system_recipient, message_data, created_at::text
        FROM posted_messages
        WHERE bundle_id = $1
        ORDER BY message_index ASC
        "#,
    )
    .bind(bundle_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|r| row_to_posted_message(&r)).collect())
}

pub async fn get_block_with_bundles_and_messages(
    pool: &PgPool,
    block_hash: &str,
) -> Result<Vec<BundleWithMessages>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT
            ib.id as bundle_id,
            ib.bundle_index,
            ib.origin_chain_id,
            ib.action,
            ib.source_height,
            ib.source_timestamp,
            ib.source_cert_hash,
            ib.transaction_index as bundle_transaction_index,
            ib.created_at::text as bundle_created_at,
            pm.id as message_id,
            pm.message_index,
            pm.authenticated_signer,
            pm.grant_amount,
            pm.refund_grant_to,
            pm.message_kind,
            pm.message_type,
            pm.application_id,
            pm.system_message_type,
            pm.system_target,
            pm.system_amount,
            pm.system_source,
            pm.system_owner,
            pm.system_recipient,
            pm.message_data,
            pm.created_at::text as message_created_at
        FROM incoming_bundles ib
        LEFT JOIN posted_messages pm ON ib.id = pm.bundle_id
        WHERE ib.block_hash = $1
        ORDER BY ib.bundle_index, pm.message_index
        "#,
    )
    .bind(block_hash)
    .fetch_all(pool)
    .await?;

    let mut bundles_map: std::collections::HashMap<i64, BundleWithMessages> =
        std::collections::HashMap::new();

    for row in rows {
        let bundle_id: i64 = row.get("bundle_id");

        bundles_map.entry(bundle_id).or_insert_with(|| BundleWithMessages {
            id: bundle_id,
            block_hash: block_hash.to_string(),
            bundle_index: row.get("bundle_index"),
            origin_chain_id: row.get("origin_chain_id"),
            action: row.get("action"),
            source_height: row.get("source_height"),
            source_timestamp: row.get("source_timestamp"),
            source_cert_hash: row.get("source_cert_hash"),
            transaction_index: row.get("bundle_transaction_index"),
            created_at: row.get("bundle_created_at"),
            messages: Vec::new(),
        });

        let message_id: Option<i64> = row.get("message_id");
        if let Some(msg_id) = message_id {
            let authenticated_signer: Option<Vec<u8>> = row.get("authenticated_signer");
            let refund_grant_to: Option<Vec<u8>> = row.get("refund_grant_to");
            let message_data: Option<Vec<u8>> = row.get("message_data");

            let message = PostedMessage {
                id: msg_id,
                bundle_id,
                message_index: row.get("message_index"),
                authenticated_signer: authenticated_signer.map(|d| BASE64.encode(&d)),
                grant_amount: row.get("grant_amount"),
                refund_grant_to: refund_grant_to.map(|d| BASE64.encode(&d)),
                message_kind: row.get("message_kind"),
                message_type: row.get("message_type"),
                application_id: row.get("application_id"),
                system_message_type: row.get("system_message_type"),
                system_target: row.get("system_target"),
                system_amount: row.get("system_amount"),
                system_source: row.get("system_source"),
                system_owner: row.get("system_owner"),
                system_recipient: row.get("system_recipient"),
                message_data: message_data.map(|d| BASE64.encode(&d)),
                created_at: row.get("message_created_at"),
            };

            bundles_map.get_mut(&bundle_id).unwrap().messages.push(message);
        }
    }

    let mut bundles: Vec<BundleWithMessages> = bundles_map.into_values().collect();
    bundles.sort_by_key(|b| b.bundle_index);
    Ok(bundles)
}

pub async fn get_chains(pool: &PgPool, limit: Option<i64>, offset: i64) -> Result<Vec<ChainStats>, sqlx::Error> {
    match limit {
        Some(lim) => {
            sqlx::query_as::<_, ChainStats>(
                r#"
                SELECT
                    chain_id,
                    COUNT(*) as block_count,
                    MAX(height) as latest_height,
                    (SELECT hash FROM blocks b2 WHERE b2.chain_id = b1.chain_id ORDER BY height DESC LIMIT 1) as latest_block_hash
                FROM blocks b1
                GROUP BY chain_id
                ORDER BY latest_height DESC
                LIMIT $1 OFFSET $2
                "#,
            )
            .bind(lim)
            .bind(offset)
            .fetch_all(pool)
            .await
        }
        None => {
            sqlx::query_as::<_, ChainStats>(
                r#"
                SELECT
                    chain_id,
                    COUNT(*) as block_count,
                    MAX(height) as latest_height,
                    (SELECT hash FROM blocks b2 WHERE b2.chain_id = b1.chain_id ORDER BY height DESC LIMIT 1) as latest_block_hash
                FROM blocks b1
                GROUP BY chain_id
                ORDER BY latest_height DESC
                "#,
            )
            .fetch_all(pool)
            .await
        }
    }
}

pub async fn get_chains_count(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row = sqlx::query("SELECT COUNT(DISTINCT chain_id) as count FROM blocks")
        .fetch_one(pool)
        .await?;
    Ok(row.get("count"))
}

pub async fn get_chain_by_id(pool: &PgPool, chain_id: &str) -> Result<Option<ChainStats>, sqlx::Error> {
    sqlx::query_as::<_, ChainStats>(
        r#"
        SELECT
            chain_id,
            COUNT(*) as block_count,
            MAX(height) as latest_height,
            (SELECT hash FROM blocks b2 WHERE b2.chain_id = b1.chain_id ORDER BY height DESC LIMIT 1) as latest_block_hash
        FROM blocks b1
        WHERE chain_id = $1
        GROUP BY chain_id
        "#,
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
}

pub async fn get_total_block_count(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row = sqlx::query("SELECT COUNT(*) as count FROM blocks")
        .fetch_one(pool)
        .await?;
    Ok(row.get("count"))
}

pub async fn get_chain_block_count(pool: &PgPool, chain_id: &str) -> Result<i64, sqlx::Error> {
    let row = sqlx::query("SELECT COUNT(*) as count FROM blocks WHERE chain_id = $1")
        .bind(chain_id)
        .fetch_one(pool)
        .await?;
    Ok(row.get("count"))
}

pub async fn get_operations(pool: &PgPool, block_hash: &str) -> Result<Vec<Operation>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT id, block_hash, operation_index, transaction_index,
               application_id, operation_type, data, created_at::text
        FROM operations
        WHERE block_hash = $1
        ORDER BY operation_index ASC
        "#,
    )
    .bind(block_hash)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| {
            let data: Option<Vec<u8>> = r.get("data");
            Operation {
                id: r.get("id"),
                block_hash: r.get("block_hash"),
                operation_index: r.get("operation_index"),
                transaction_index: r.get("transaction_index"),
                application_id: r.get("application_id"),
                operation_type: r.get("operation_type"),
                data: data.map(|d| BASE64.encode(&d)),
                created_at: r.get("created_at"),
            }
        })
        .collect())
}

pub async fn get_messages(pool: &PgPool, block_hash: &str) -> Result<Vec<OutgoingMessage>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT id, block_hash, transaction_index, message_index, destination,
               authenticated_signer, grant_amount, refund_grant_to, message_kind,
               message_type, application_id, system_message_type, system_target,
               system_amount, system_source, system_owner, system_recipient,
               data, created_at::text
        FROM outgoing_messages
        WHERE block_hash = $1
        ORDER BY transaction_index ASC, message_index ASC
        "#,
    )
    .bind(block_hash)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| {
            let data: Option<Vec<u8>> = r.get("data");
            OutgoingMessage {
                id: r.get("id"),
                block_hash: r.get("block_hash"),
                transaction_index: r.get("transaction_index"),
                message_index: r.get("message_index"),
                destination: r.get("destination"),
                authenticated_signer: r.get("authenticated_signer"),
                grant_amount: r.get("grant_amount"),
                refund_grant_to: r.get("refund_grant_to"),
                message_kind: r.get("message_kind"),
                message_type: r.get("message_type"),
                application_id: r.get("application_id"),
                system_message_type: r.get("system_message_type"),
                system_target: r.get("system_target"),
                system_amount: r.get("system_amount"),
                system_source: r.get("system_source"),
                system_owner: r.get("system_owner"),
                system_recipient: r.get("system_recipient"),
                data: data.map(|d| BASE64.encode(&d)),
                created_at: r.get("created_at"),
            }
        })
        .collect())
}

pub async fn get_events(pool: &PgPool, block_hash: &str) -> Result<Vec<Event>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT id, block_hash, transaction_index, event_index,
               stream_name, event_key, data, created_at::text
        FROM events
        WHERE block_hash = $1
        ORDER BY transaction_index ASC, event_index ASC
        "#,
    )
    .bind(block_hash)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| {
            let data: Option<Vec<u8>> = r.get("data");
            Event {
                id: r.get("id"),
                block_hash: r.get("block_hash"),
                transaction_index: r.get("transaction_index"),
                event_index: r.get("event_index"),
                stream_name: r.get("stream_name"),
                event_key: r.get("event_key"),
                data: data.map(|d| BASE64.encode(&d)),
                created_at: r.get("created_at"),
            }
        })
        .collect())
}

pub async fn get_oracle_responses(pool: &PgPool, block_hash: &str) -> Result<Vec<OracleResponse>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT id, block_hash, transaction_index, response_index,
               response_type, data, created_at::text
        FROM oracle_responses
        WHERE block_hash = $1
        ORDER BY transaction_index ASC, response_index ASC
        "#,
    )
    .bind(block_hash)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| {
            let data: Option<Vec<u8>> = r.get("data");
            OracleResponse {
                id: r.get("id"),
                block_hash: r.get("block_hash"),
                transaction_index: r.get("transaction_index"),
                response_index: r.get("response_index"),
                response_type: r.get("response_type"),
                data: data.map(|d| BASE64.encode(&d)),
                created_at: r.get("created_at"),
            }
        })
        .collect())
}

fn row_to_posted_message(row: &sqlx::postgres::PgRow) -> PostedMessage {
    let authenticated_signer: Option<Vec<u8>> = row.get("authenticated_signer");
    let refund_grant_to: Option<Vec<u8>> = row.get("refund_grant_to");
    let message_data: Option<Vec<u8>> = row.get("message_data");

    PostedMessage {
        id: row.get("id"),
        bundle_id: row.get("bundle_id"),
        message_index: row.get("message_index"),
        authenticated_signer: authenticated_signer.map(|d| BASE64.encode(&d)),
        grant_amount: row.get("grant_amount"),
        refund_grant_to: refund_grant_to.map(|d| BASE64.encode(&d)),
        message_kind: row.get("message_kind"),
        message_type: row.get("message_type"),
        application_id: row.get("application_id"),
        system_message_type: row.get("system_message_type"),
        system_target: row.get("system_target"),
        system_amount: row.get("system_amount"),
        system_source: row.get("system_source"),
        system_owner: row.get("system_owner"),
        system_recipient: row.get("system_recipient"),
        message_data: message_data.map(|d| BASE64.encode(&d)),
        created_at: row.get("created_at"),
    }
}
