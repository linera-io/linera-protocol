// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Common utilities for database implementations.

use linera_base::data_types::Amount;
use linera_execution::{Message, MessageKind, SystemMessage};

/// Classification result for a Message with denormalized SystemMessage fields
#[derive(Debug)]
pub struct MessageClassification {
    pub message_type: String,
    pub application_id: Option<String>,
    pub system_message_type: Option<String>,
    pub system_target: Option<String>,
    pub system_amount: Option<Amount>,
    pub system_source: Option<String>,
    pub system_owner: Option<String>,
    pub system_recipient: Option<String>,
}

/// Classify a Message into database fields with denormalized SystemMessage fields
pub fn classify_message(message: &Message) -> MessageClassification {
    match message {
        Message::System(sys_msg) => {
            let (
                sys_msg_type,
                system_target,
                system_amount,
                system_source,
                system_owner,
                system_recipient,
            ) = match sys_msg {
                SystemMessage::Credit {
                    target,
                    amount,
                    source,
                } => (
                    "Credit",
                    Some(target.to_string()),
                    Some(*amount),
                    Some(source.to_string()),
                    None,
                    None,
                ),
                SystemMessage::Withdraw {
                    owner,
                    amount,
                    recipient,
                } => (
                    "Withdraw",
                    None,
                    Some(*amount),
                    None,
                    Some(owner.to_string()),
                    Some(recipient.to_string()),
                ),
            };

            MessageClassification {
                message_type: "System".to_string(),
                application_id: None,
                system_message_type: Some(sys_msg_type.to_string()),
                system_target,
                system_amount,
                system_source,
                system_owner,
                system_recipient,
            }
        }
        Message::User { application_id, .. } => MessageClassification {
            message_type: "User".to_string(),
            application_id: Some(application_id.to_string()),
            system_message_type: None,
            system_target: None,
            system_amount: None,
            system_source: None,
            system_owner: None,
            system_recipient: None,
        },
    }
}

/// Convert MessageKind to string
pub fn message_kind_to_string(kind: &MessageKind) -> String {
    format!("{:?}", kind)
}

/// Parse MessageKind from string
pub fn parse_message_kind(kind_str: &str) -> Result<MessageKind, String> {
    match kind_str {
        "Simple" => Ok(MessageKind::Simple),
        "Tracked" => Ok(MessageKind::Tracked),
        "Bouncing" => Ok(MessageKind::Bouncing),
        "Protected" => Ok(MessageKind::Protected),
        _ => Err(format!("Unknown message kind: {}", kind_str)),
    }
}
