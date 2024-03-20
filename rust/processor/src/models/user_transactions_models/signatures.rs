// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    schema::signatures::{self},
    utils::{counters::PROCESSOR_UNKNOWN_TYPE_COUNT, util::standardize_address},
};
use anyhow::{Context, Result};
use aptos_protos::transaction::v1::{
    account_signature::Signature as AccountSignatureEnum,
    any_signature::{SignatureVariant, Type as AnySignatureTypeEnumPb},
    signature::Signature as SignatureEnum,
    AccountSignature as ProtoAccountSignature, Ed25519Signature as Ed25519SignaturePB,
    FeePayerSignature as ProtoFeePayerSignature, MultiAgentSignature as ProtoMultiAgentSignature,
    MultiEd25519Signature as MultiEd25519SignaturePb, MultiKeySignature as MultiKeySignaturePb,
    Signature as TransactionSignaturePb, SingleKeySignature as SingleKeySignaturePb,
    SingleSender as SingleSenderPb,
};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(
    transaction_version,
    multi_agent_index,
    multi_sig_index,
    is_sender_primary
))]
#[diesel(table_name = signatures)]
pub struct Signature {
    pub transaction_version: i64,
    pub multi_agent_index: i64,
    pub multi_sig_index: i64,
    pub transaction_block_height: i64,
    pub signer: String,
    pub is_sender_primary: bool,
    pub type_: String,
    pub public_key: String,
    pub signature: String,
    pub threshold: i64,
    pub public_key_indices: serde_json::Value,
}

impl Signature {
    /// Returns a flattened list of signatures. If signature is a Ed25519Signature, then return a vector of 1 signature
    pub fn from_user_transaction(
        s: &TransactionSignaturePb,
        sender: &String,
        transaction_version: i64,
        transaction_block_height: i64,
    ) -> Result<Vec<Self>> {
        match s.signature.as_ref().unwrap() {
            SignatureEnum::Ed25519(sig) => Ok(vec![Self::parse_ed25519_signature(
                sig,
                sender,
                transaction_version,
                transaction_block_height,
                true,
                0,
                None,
            )]),
            SignatureEnum::MultiEd25519(sig) => Ok(Self::parse_multi_ed25519_signature(
                sig,
                sender,
                transaction_version,
                transaction_block_height,
                true,
                0,
                None,
            )),
            SignatureEnum::MultiAgent(sig) => Self::parse_multi_agent_signature(
                sig,
                sender,
                transaction_version,
                transaction_block_height,
            ),
            SignatureEnum::FeePayer(sig) => Self::parse_fee_payer_signature(
                sig,
                sender,
                transaction_version,
                transaction_block_height,
            ),
            SignatureEnum::SingleSender(s) => Ok(Self::parse_single_sender(
                s,
                sender,
                transaction_version,
                transaction_block_height,
            )),
        }
    }

    pub fn get_signature_type(t: &TransactionSignaturePb) -> String {
        match t.signature.as_ref().unwrap() {
            SignatureEnum::Ed25519(_) => String::from("ed25519_signature"),
            SignatureEnum::MultiEd25519(_) => String::from("multi_ed25519_signature"),
            SignatureEnum::MultiAgent(_) => String::from("multi_agent_signature"),
            SignatureEnum::FeePayer(_) => String::from("fee_payer_signature"),
            SignatureEnum::SingleSender(sender) => {
                let account_signature = sender.sender.as_ref().unwrap();
                let signature = account_signature.signature.as_ref().unwrap();
                match signature {
                    AccountSignatureEnum::Ed25519(_) => String::from("ed25519_signature"),
                    AccountSignatureEnum::MultiEd25519(_) => {
                        String::from("multi_ed25519_signature")
                    },
                    AccountSignatureEnum::SingleKeySignature(_) => {
                        String::from("single_key_signature")
                    },
                    AccountSignatureEnum::MultiKeySignature(_) => {
                        String::from("multi_key_signature")
                    },
                }
            },
        }
    }

    pub fn get_fee_payer_address(
        t: &TransactionSignaturePb,
        transaction_version: i64,
    ) -> Option<String> {
        let sig = t.signature.as_ref().unwrap_or_else(|| {
            tracing::error!(
                transaction_version = transaction_version,
                "Transaction signature is missing"
            );
            panic!("Transaction signature is missing");
        });
        match sig {
            SignatureEnum::FeePayer(sig) => Some(standardize_address(&sig.fee_payer_address)),
            _ => None,
        }
    }

    fn parse_ed25519_signature(
        s: &Ed25519SignaturePB,
        sender: &String,
        transaction_version: i64,
        transaction_block_height: i64,
        is_sender_primary: bool,
        multi_agent_index: i64,
        override_address: Option<&String>,
    ) -> Self {
        let signer = standardize_address(override_address.unwrap_or(sender));
        Self {
            transaction_version,
            transaction_block_height,
            signer,
            is_sender_primary,
            type_: String::from("ed25519_signature"),
            public_key: format!("0x{}", hex::encode(s.public_key.as_slice())),
            threshold: 1,
            public_key_indices: serde_json::Value::Array(vec![]),
            signature: format!("0x{}", hex::encode(s.signature.as_slice())),
            multi_agent_index,
            multi_sig_index: 0,
        }
    }

    fn parse_multi_ed25519_signature(
        s: &MultiEd25519SignaturePb,
        sender: &String,
        transaction_version: i64,
        transaction_block_height: i64,
        is_sender_primary: bool,
        multi_agent_index: i64,
        override_address: Option<&String>,
    ) -> Vec<Self> {
        let mut signatures = Vec::default();
        let signer = standardize_address(override_address.unwrap_or(sender));

        let public_key_indices: Vec<usize> = s
            .public_key_indices
            .iter()
            .map(|index| *index as usize)
            .collect();
        for (index, signature) in s.signatures.iter().enumerate() {
            let public_key = s
                .public_keys
                .get(public_key_indices.clone()[index])
                .unwrap()
                .clone();
            signatures.push(Self {
                transaction_version,
                transaction_block_height,
                signer: signer.clone(),
                is_sender_primary,
                type_: String::from("multi_ed25519_signature"),
                public_key: format!("0x{}", hex::encode(public_key.as_slice())),
                threshold: s.threshold as i64,
                signature: format!("0x{}", hex::encode(signature.as_slice())),
                public_key_indices: serde_json::Value::Array(
                    public_key_indices
                        .iter()
                        .map(|index| {
                            serde_json::Value::Number(serde_json::Number::from(*index as i64))
                        })
                        .collect(),
                ),
                multi_agent_index,
                multi_sig_index: index as i64,
            });
        }
        signatures
    }

    fn parse_multi_agent_signature(
        s: &ProtoMultiAgentSignature,
        sender: &String,
        transaction_version: i64,
        transaction_block_height: i64,
    ) -> Result<Vec<Self>> {
        let mut signatures = Vec::default();
        // process sender signature
        signatures.append(&mut Self::parse_multi_agent_signature_helper(
            s.sender.as_ref().unwrap(),
            sender,
            transaction_version,
            transaction_block_height,
            true,
            0,
            None,
        ));
        for (index, address) in s.secondary_signer_addresses.iter().enumerate() {
            let secondary_sig = s.secondary_signers.get(index).context(format!(
                "Failed to parse index {} for multi agent secondary signers",
                index
            ))?;
            signatures.append(&mut Self::parse_multi_agent_signature_helper(
                secondary_sig,
                sender,
                transaction_version,
                transaction_block_height,
                false,
                index as i64,
                Some(&address.to_string()),
            ));
        }
        Ok(signatures)
    }

    fn parse_fee_payer_signature(
        s: &ProtoFeePayerSignature,
        sender: &String,
        transaction_version: i64,
        transaction_block_height: i64,
    ) -> Result<Vec<Self>> {
        let mut signatures = Vec::default();
        // process sender signature
        signatures.append(&mut Self::parse_multi_agent_signature_helper(
            s.sender.as_ref().unwrap(),
            sender,
            transaction_version,
            transaction_block_height,
            true,
            0,
            None,
        ));
        for (index, address) in s.secondary_signer_addresses.iter().enumerate() {
            let secondary_sig = s.secondary_signers.get(index).context(format!(
                "Failed to parse index {} for multi agent secondary signers",
                index
            ))?;
            signatures.append(&mut Self::parse_multi_agent_signature_helper(
                secondary_sig,
                sender,
                transaction_version,
                transaction_block_height,
                false,
                index as i64,
                Some(&address.to_string()),
            ));
        }
        Ok(signatures)
    }

    fn parse_multi_agent_signature_helper(
        s: &ProtoAccountSignature,
        sender: &String,
        transaction_version: i64,
        transaction_block_height: i64,
        is_sender_primary: bool,
        multi_agent_index: i64,
        override_address: Option<&String>,
    ) -> Vec<Self> {
        let signature = s.signature.as_ref().unwrap();
        match signature {
            AccountSignatureEnum::Ed25519(sig) => vec![Self::parse_ed25519_signature(
                sig,
                sender,
                transaction_version,
                transaction_block_height,
                is_sender_primary,
                multi_agent_index,
                override_address,
            )],
            AccountSignatureEnum::MultiEd25519(sig) => Self::parse_multi_ed25519_signature(
                sig,
                sender,
                transaction_version,
                transaction_block_height,
                is_sender_primary,
                multi_agent_index,
                override_address,
            ),
            AccountSignatureEnum::SingleKeySignature(sig) => {
                vec![Self::parse_single_key_signature(
                    sig,
                    sender,
                    transaction_version,
                    transaction_block_height,
                    is_sender_primary,
                    multi_agent_index,
                    override_address,
                )]
            },
            AccountSignatureEnum::MultiKeySignature(sig) => Self::parse_multi_key_signature(
                sig,
                sender,
                transaction_version,
                transaction_block_height,
                is_sender_primary,
                multi_agent_index,
                override_address,
            ),
        }
    }

    #[allow(deprecated)]
    fn parse_single_key_signature(
        s: &SingleKeySignaturePb,
        sender: &String,
        transaction_version: i64,
        transaction_block_height: i64,
        is_sender_primary: bool,
        multi_agent_index: i64,
        override_address: Option<&String>,
    ) -> Self {
        let signer = standardize_address(override_address.unwrap_or(sender));
        let signature = s.signature.as_ref().unwrap();
        let signature_bytes =
            Self::get_any_signature_bytes(&signature.signature_variant, transaction_version)
                // old way of getting signature bytes prior to node 1.10
                .unwrap_or(signature.signature.clone());
        let type_ = if let Some(t) =
            Self::get_any_signature_type(&signature.signature_variant, true, transaction_version)
        {
            t
        } else {
            // old way of getting signature type prior to node 1.10
            match AnySignatureTypeEnumPb::try_from(signature.r#type) {
                Ok(AnySignatureTypeEnumPb::Ed25519) => String::from("single_key_ed25519_signature"),
                Ok(AnySignatureTypeEnumPb::Secp256k1Ecdsa) => {
                    String::from("single_key_secp256k1_ecdsa_signature")
                },
                wildcard => {
                    tracing::warn!(
                        transaction_version = transaction_version,
                        "Unspecified signature type or un-recognized type is not supported: {:?}",
                        wildcard
                    );
                    PROCESSOR_UNKNOWN_TYPE_COUNT
                        .with_label_values(&["unspecified_signature_type"])
                        .inc();
                    "".to_string()
                },
            }
        };
        Self {
            transaction_version,
            transaction_block_height,
            signer,
            is_sender_primary,
            type_,
            public_key: format!(
                "0x{}",
                hex::encode(s.public_key.as_ref().unwrap().public_key.as_slice())
            ),
            threshold: 1,
            public_key_indices: serde_json::Value::Array(vec![]),
            signature: format!("0x{}", hex::encode(signature_bytes.as_slice())),
            multi_agent_index,
            multi_sig_index: 0,
        }
    }

    #[allow(deprecated)]
    fn parse_multi_key_signature(
        s: &MultiKeySignaturePb,
        sender: &String,
        transaction_version: i64,
        transaction_block_height: i64,
        is_sender_primary: bool,
        multi_agent_index: i64,
        override_address: Option<&String>,
    ) -> Vec<Self> {
        let signer = standardize_address(override_address.unwrap_or(sender));
        let mut signatures = Vec::default();

        let public_key_indices: Vec<usize> =
            s.signatures.iter().map(|key| key.index as usize).collect();

        for (index, signature) in s.signatures.iter().enumerate() {
            let public_key = s
                .public_keys
                .as_slice()
                .get(index)
                .unwrap()
                .public_key
                .clone();
            let signature_bytes = Self::get_any_signature_bytes(
                &signature.signature.as_ref().unwrap().signature_variant,
                transaction_version,
            )
            // old way of getting signature bytes prior to node 1.10
            .unwrap_or(signature.signature.as_ref().unwrap().signature.clone());

            let type_ = if let Some(t) = Self::get_any_signature_type(
                &signature.signature.as_ref().unwrap().signature_variant,
                false,
                transaction_version,
            ) {
                t
            } else {
                // old way of getting signature type prior to node 1.10
                match AnySignatureTypeEnumPb::try_from(signature.signature.as_ref().unwrap().r#type)
                {
                    Ok(AnySignatureTypeEnumPb::Ed25519) => {
                        String::from("multi_key_ed25519_signature")
                    },
                    Ok(AnySignatureTypeEnumPb::Secp256k1Ecdsa) => {
                        String::from("multi_key_secp256k1_ecdsa_signature")
                    },
                    wildcard => {
                        tracing::warn!(
                            transaction_version = transaction_version,
                            "Unspecified signature type or un-recognized type is not supported: {:?}",
                            wildcard
                        );
                        PROCESSOR_UNKNOWN_TYPE_COUNT
                            .with_label_values(&["unspecified_signature_type"])
                            .inc();
                        "unknown".to_string()
                    },
                }
            };
            signatures.push(Self {
                transaction_version,
                transaction_block_height,
                signer: signer.clone(),
                is_sender_primary,
                type_,
                public_key: format!("0x{}", hex::encode(public_key.as_slice())),
                threshold: s.signatures_required as i64,
                signature: format!("0x{}", hex::encode(signature_bytes.as_slice())),
                public_key_indices: serde_json::Value::Array(
                    public_key_indices
                        .iter()
                        .map(|index| {
                            serde_json::Value::Number(serde_json::Number::from(*index as i64))
                        })
                        .collect(),
                ),
                multi_agent_index,
                multi_sig_index: index as i64,
            });
        }
        signatures
    }

    fn get_any_signature_bytes(
        signature_variant: &Option<SignatureVariant>,
        transaction_version: i64,
    ) -> Option<Vec<u8>> {
        match signature_variant {
            Some(SignatureVariant::Ed25519(sig)) => Some(sig.signature.clone()),
            Some(SignatureVariant::Keyless(sig)) => Some(sig.signature.clone()),
            Some(SignatureVariant::Webauthn(sig)) => Some(sig.signature.clone()),
            Some(SignatureVariant::Secp256k1Ecdsa(sig)) => Some(sig.signature.clone()),
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["SignatureVariant"])
                    .inc();
                tracing::warn!(
                    transaction_version = transaction_version,
                    "Signature variant doesn't exist",
                );
                None
            },
        }
    }

    fn get_any_signature_type(
        signature_variant: &Option<SignatureVariant>,
        is_single_sender: bool,
        transaction_version: i64,
    ) -> Option<String> {
        let prefix = if is_single_sender {
            "single_sender"
        } else {
            "multi_key"
        };
        match signature_variant {
            Some(SignatureVariant::Ed25519(_)) => Some(format!("{}_ed25519_signature", prefix)),
            Some(SignatureVariant::Keyless(_)) => Some(format!("{}_keyless_signature", prefix)),
            Some(SignatureVariant::Webauthn(_)) => Some(format!("{}_webauthn_signature", prefix)),
            Some(SignatureVariant::Secp256k1Ecdsa(_)) => {
                Some(format!("{}_secp256k1_ecdsa_signature", prefix))
            },
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["SignatureVariant"])
                    .inc();
                tracing::warn!(
                    transaction_version = transaction_version,
                    "Signature variant doesn't exist",
                );
                None
            },
        }
    }

    fn parse_single_sender(
        s: &SingleSenderPb,
        sender: &String,
        transaction_version: i64,
        transaction_block_height: i64,
    ) -> Vec<Self> {
        let signature = s.sender.as_ref().unwrap();
        match signature.signature.as_ref() {
            Some(AccountSignatureEnum::SingleKeySignature(s)) => {
                vec![Self::parse_single_key_signature(
                    s,
                    sender,
                    transaction_version,
                    transaction_block_height,
                    true,
                    0,
                    None,
                )]
            },
            Some(AccountSignatureEnum::MultiKeySignature(s)) => Self::parse_multi_key_signature(
                s,
                sender,
                transaction_version,
                transaction_block_height,
                true,
                0,
                None,
            ),
            Some(AccountSignatureEnum::Ed25519(s)) => vec![Self::parse_ed25519_signature(
                s,
                sender,
                transaction_version,
                transaction_block_height,
                true,
                0,
                None,
            )],
            Some(AccountSignatureEnum::MultiEd25519(s)) => Self::parse_multi_ed25519_signature(
                s,
                sender,
                transaction_version,
                transaction_block_height,
                true,
                0,
                None,
            ),
            None => vec![],
        }
    }
}
