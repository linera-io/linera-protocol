// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A machine-readable description of how storage lays out its keys and values.
//!
//! Storage is divided into partitions, each addressed by the BCS encoding of a [`RootKey`](crate::RootKey).
//! Within a partition, keys either form the key space of a root view or a fixed table of
//! entries. This module describes that layout so that raw key-value pairs can be interpreted
//! without reference to the code that wrote them.
//!
//! The description is only as good as its agreement with the writers, so it names the same
//! constants they use, and `tests::root_key_variants_match_declaration` checks the declared
//! partition tags against the encoding of real [`RootKey`](crate::RootKey) values.
//!
//! The description starts at the root key. Backends wrap it further: ScyllaDB prepends a zero
//! byte because it requires non-empty partition keys (`get_big_root_key`), and RocksDB prefixes
//! the data domain with `ROOT_KEY_DOMAIN` to separate it from its root-key registry. Those
//! belong to `linera-views` and are not described here.

use serde::Serialize;

use crate::db_storage::EntryKey;

/// How the bytes of a key are produced.
#[derive(Debug, Serialize)]
pub enum KeyFormat {
    /// A fixed entry, named by its [`EntryKey`](crate::EntryKey) variant.
    Fixed {
        /// The name of the variant.
        variant: String,
        /// Its stored bytes.
        bytes: Vec<u8>,
    },
    /// The BCS encoding of a value of the named type.
    Bcs {
        /// The name of the encoded type.
        type_name: &'static str,
    },
}

/// How the bytes of a value are produced.
#[derive(Debug, Serialize)]
pub enum ValueFormat {
    /// The BCS encoding of a value of the named type.
    Bcs {
        /// The name of the encoded type.
        type_name: &'static str,
    },
    /// Bytes whose interpretation this schema does not determine.
    Opaque {
        /// What the bytes are, for a human reader.
        description: &'static str,
    },
}

/// A single entry of a partition holding a fixed table.
#[derive(Debug, Serialize)]
pub struct FlatEntry {
    /// The key, relative to the root key.
    pub key: KeyFormat,
    /// The value stored under it.
    pub value: ValueFormat,
}

/// What a partition holds.
#[derive(Debug, Serialize)]
pub enum PartitionBody {
    /// The key space of a root view of the named type, as laid out by `linera-views`.
    RootView {
        /// The name of the view type.
        type_name: &'static str,
    },
    /// A fixed table of entries.
    Flat {
        /// The entries, in no particular order.
        entries: Vec<FlatEntry>,
    },
}

/// One partition of the key space.
#[derive(Debug, Serialize)]
pub struct Partition {
    /// The name of the [`RootKey`](crate::RootKey) variant addressing this partition.
    pub name: &'static str,
    /// The BCS variant index of that root key, which is the first byte of every key's
    /// partition address.
    pub variant: u32,
    /// The payload the root key carries, which is context for the keys inside: the chain a
    /// partition belongs to is not repeated in its keys.
    pub payload: Option<&'static str>,
    /// What the partition holds.
    pub body: PartitionBody,
}

/// The layout of storage.
#[derive(Debug, Serialize)]
pub struct StorageFormat {
    /// The type whose BCS encoding addresses a partition.
    pub root_key_type: &'static str,
    /// The partitions, in the declaration order of [`RootKey`](crate::RootKey).
    pub partitions: Vec<Partition>,
}

/// Describes a fixed entry key by asking it for its own bytes.
fn fixed(key: EntryKey) -> KeyFormat {
    KeyFormat::Fixed {
        // The derived `Debug` of a fieldless variant is its name, so there is no second
        // name-to-variant mapping to keep in step.
        variant: format!("{key:?}"),
        bytes: key.as_bytes().to_vec(),
    }
}

impl StorageFormat {
    /// Describes the layout this build of `linera-storage` reads and writes.
    pub fn current() -> Self {
        StorageFormat {
            root_key_type: "RootKey",
            partitions: vec![
                Partition {
                    name: "NetworkDescription",
                    variant: 0,
                    payload: None,
                    body: PartitionBody::Flat {
                        entries: vec![FlatEntry {
                            key: fixed(EntryKey::NetworkDescription),
                            value: ValueFormat::Bcs {
                                type_name: "NetworkDescription",
                            },
                        }],
                    },
                },
                Partition {
                    name: "BlockExporterState",
                    variant: 1,
                    payload: Some("u32"),
                    body: PartitionBody::RootView {
                        type_name: "BlockExporterStateView",
                    },
                },
                Partition {
                    name: "ChainState",
                    variant: 2,
                    payload: Some("ChainId"),
                    body: PartitionBody::RootView {
                        type_name: "ChainStateView",
                    },
                },
                Partition {
                    name: "BlockHash",
                    variant: 3,
                    payload: Some("CryptoHash"),
                    body: PartitionBody::Flat {
                        entries: vec![
                            FlatEntry {
                                key: fixed(EntryKey::LiteCertificate),
                                value: ValueFormat::Bcs {
                                    type_name: "LiteCertificate",
                                },
                            },
                            FlatEntry {
                                key: fixed(EntryKey::Block),
                                value: ValueFormat::Bcs {
                                    type_name: "ConfirmedBlock",
                                },
                            },
                        ],
                    },
                },
                Partition {
                    name: "BlobId",
                    variant: 4,
                    payload: Some("BlobId"),
                    body: PartitionBody::Flat {
                        entries: vec![
                            FlatEntry {
                                key: fixed(EntryKey::Blob),
                                value: ValueFormat::Opaque {
                                    description: "the contents of the blob",
                                },
                            },
                            FlatEntry {
                                key: fixed(EntryKey::BlobState),
                                value: ValueFormat::Bcs {
                                    type_name: "BlobState",
                                },
                            },
                        ],
                    },
                },
                Partition {
                    name: "Event",
                    variant: 5,
                    payload: Some("ChainId"),
                    body: PartitionBody::Flat {
                        entries: vec![FlatEntry {
                            // The chain is carried by the root key, so only the stream and
                            // index remain: together they reconstitute an `EventId`.
                            key: KeyFormat::Bcs {
                                type_name: "RestrictedEventId",
                            },
                            value: ValueFormat::Opaque {
                                description: "the contents of the event",
                            },
                        }],
                    },
                },
                Partition {
                    name: "BlockByHeight",
                    variant: 6,
                    payload: Some("ChainId"),
                    body: PartitionBody::Flat {
                        entries: vec![FlatEntry {
                            key: KeyFormat::Bcs {
                                type_name: "BlockHeight",
                            },
                            value: ValueFormat::Bcs {
                                type_name: "CryptoHash",
                            },
                        }],
                    },
                },
                Partition {
                    name: "EventBlockHeight",
                    variant: 7,
                    payload: Some("ChainId"),
                    body: PartitionBody::Flat {
                        entries: vec![FlatEntry {
                            key: KeyFormat::Bcs {
                                type_name: "RestrictedEventId",
                            },
                            value: ValueFormat::Bcs {
                                type_name: "BlockHeight",
                            },
                        }],
                    },
                },
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::CryptoHash,
        identifiers::{BlobId, BlobType, ChainId},
    };

    use super::*;
    use crate::RootKey;

    /// The declared partition tags must be the ones `RootKey` actually encodes to. This is what
    /// makes the description trustworthy: reordering the enum fails here rather than silently
    /// invalidating the schema.
    #[test]
    fn root_key_variants_match_declaration() {
        let hash = CryptoHash::default();
        let chain_id = ChainId(hash);
        let blob_id = BlobId {
            blob_type: BlobType::Data,
            hash,
        };
        let samples = [
            RootKey::NetworkDescription,
            RootKey::BlockExporterState(0),
            RootKey::ChainState(chain_id),
            RootKey::BlockHash(hash),
            RootKey::BlobId(blob_id),
            RootKey::Event(chain_id),
            RootKey::BlockByHeight(chain_id),
            RootKey::EventBlockHeight(chain_id),
        ];
        let format = StorageFormat::current();
        assert_eq!(format.partitions.len(), samples.len());
        for (partition, sample) in format.partitions.iter().zip(samples) {
            let bytes = sample.bytes();
            // Every variant index so far fits in one ULEB128 byte.
            assert_eq!(
                u32::from(bytes[0]),
                partition.variant,
                "declared tag for {} does not match its encoding",
                partition.name
            );
        }
    }

    /// The spelled-out bytes must be the BCS encoding, or reads and the description disagree
    /// with what a decoder would compute.
    #[test]
    fn entry_keys_match_bcs() {
        for key in EntryKey::ALL {
            assert_eq!(
                key.as_bytes(),
                bcs::to_bytes(key).unwrap(),
                "{key:?} does not encode to its stated bytes"
            );
        }
    }

    #[test]
    fn test_format() {
        insta::assert_yaml_snapshot!("storage_format.yaml", StorageFormat::current());
    }
}
