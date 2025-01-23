// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    db::postgres::models::account_restoration_models::{
        auth_key_account_addresses::AuthKeyAccountAddress,
        auth_key_multikey_layout::AuthKeyMultikeyLayout, public_key_auth_keys::PublicKeyAuthKey,
    },
    utils::util::sha3_256,
};
use aptos_protos::transaction::v1::{
    account_signature::Signature as AccountSignature, signature::Signature, transaction::TxnData,
    Transaction,
};
use tracing::warn;

trait AuthKeyScheme {
    const SCHEME: u8;

    fn auth_key(&self) -> Option<String>;
}

struct Ed25519AuthKeyScheme {
    public_key: Vec<u8>,
}

impl AuthKeyScheme for Ed25519AuthKeyScheme {
    const SCHEME: u8 = 0x00;

    fn auth_key(&self) -> Option<String> {
        let mut preimage = self.public_key.clone();
        preimage.push(Self::SCHEME);
        Some(format!("0x{}", hex::encode(sha3_256(&preimage))))
    }
}

struct MultiEd25519AuthKeyScheme {
    threshold: u32,
    public_keys: Vec<Vec<u8>>,
    verified: Vec<bool>,
}

impl AuthKeyScheme for MultiEd25519AuthKeyScheme {
    const SCHEME: u8 = 0x01;

    fn auth_key(&self) -> Option<String> {
        let mut preimage = vec![];
        for public_key in &self.public_keys {
            preimage.extend_from_slice(public_key);
        }
        preimage.push(self.threshold.try_into().ok()?);
        preimage.push(Self::SCHEME);
        Some(format!("0x{}", hex::encode(sha3_256(&preimage))))
    }
}

/// Key type prefix for generalized key scheme
#[derive(Clone, Copy)]
#[repr(u8)]
enum AnyPublicKeyType {
    Ed25519 = 0x00,
    Secp256k1Ecdsa = 0x01,
    Secp256r1Ecdsa = 0x02,
    Keyless = 0x03,
}

impl AnyPublicKeyType {
    fn from_i32(key_type: Option<i32>) -> Option<Self> {
        match key_type {
            Some(0x00) => Some(AnyPublicKeyType::Ed25519),
            Some(0x01) => Some(AnyPublicKeyType::Secp256k1Ecdsa),
            Some(0x02) => Some(AnyPublicKeyType::Secp256r1Ecdsa),
            Some(0x03) => Some(AnyPublicKeyType::Keyless),
            _ => None,
        }
    }

    fn to_u8(self) -> u8 {
        self as u8
    }

    fn key_type_string(&self) -> String {
        match self {
            Self::Ed25519 => String::from("ed25519"),
            Self::Secp256k1Ecdsa => String::from("secp256k1_ecdsa"),
            Self::Secp256r1Ecdsa => String::from("secp256r1_ecdsa"),
            Self::Keyless => String::from("keyless"),
        }
    }
}

struct SingleKeyAuthKeyScheme {
    key_type: Option<AnyPublicKeyType>,
    public_key: Vec<u8>,
}

impl AuthKeyScheme for SingleKeyAuthKeyScheme {
    const SCHEME: u8 = 0x02;

    fn auth_key(&self) -> Option<String> {
        if let Some(key_type) = &self.key_type {
            let mut preimage = vec![key_type.to_u8()];
            preimage.extend_from_slice(&self.public_key);
            preimage.push(Self::SCHEME);
            Some(format!("0x{}", hex::encode(sha3_256(&preimage))))
        } else {
            None
        }
    }
}

struct MultiKeyAuthKeyScheme {
    threshold: u32,
    key_types: Vec<Option<AnyPublicKeyType>>,
    public_keys: Vec<Vec<u8>>,
    verified: Vec<bool>,
}

impl AuthKeyScheme for MultiKeyAuthKeyScheme {
    const SCHEME: u8 = 0x03;

    fn auth_key(&self) -> Option<String> {
        if self.key_types.iter().any(|key_type| key_type.is_none()) {
            return None;
        }

        if self.key_types.len() != self.public_keys.len() {
            return None;
        }

        let total_keys = self.key_types.len().try_into().ok()?;
        let mut preimage = vec![total_keys];

        for (key_type, public_key) in self.key_types.iter().zip(&self.public_keys) {
            preimage.push(key_type.expect("should not be None").to_u8());
            preimage.extend_from_slice(public_key);
        }

        preimage.push(self.threshold.try_into().ok()?);
        preimage.push(Self::SCHEME);
        Some(format!("0x{}", hex::encode(sha3_256(&preimage))))
    }
}

enum SignatureInfo {
    Ed25519(Ed25519AuthKeyScheme),
    MultiEd25519(MultiEd25519AuthKeyScheme),
    SingleKey(SingleKeyAuthKeyScheme),
    MultiKey(MultiKeyAuthKeyScheme),
}

impl SignatureInfo {
    fn ed25519(public_key: Vec<u8>) -> Self {
        Self::Ed25519(Ed25519AuthKeyScheme { public_key })
    }

    fn multi_ed25519(threshold: u32, public_keys: Vec<Vec<u8>>, verified: Vec<bool>) -> Self {
        Self::MultiEd25519(MultiEd25519AuthKeyScheme {
            threshold,
            public_keys,
            verified,
        })
    }

    fn single_key(key_type: Option<i32>, public_key: Vec<u8>) -> Self {
        Self::SingleKey(SingleKeyAuthKeyScheme {
            key_type: AnyPublicKeyType::from_i32(key_type),
            public_key,
        })
    }

    fn signature_type_string(&self) -> String {
        match self {
            Self::Ed25519(_) => String::from("ed25519"),
            Self::MultiEd25519(_) => String::from("multi_ed25519"),
            Self::SingleKey(_) => String::from("single_key"),
            Self::MultiKey(_) => String::from("multi_key"),
        }
    }

    fn multi_key(
        threshold: u32,
        key_types: Vec<Option<i32>>,
        public_keys: Vec<Vec<u8>>,
        verified: Vec<bool>,
    ) -> Self {
        Self::MultiKey(MultiKeyAuthKeyScheme {
            threshold,
            key_types: key_types
                .into_iter()
                .map(AnyPublicKeyType::from_i32)
                .collect(),
            public_keys,
            verified,
        })
    }

    fn is_multikey(&self) -> bool {
        matches!(self, Self::MultiEd25519(_) | Self::MultiKey(_))
    }

    fn auth_key(&self) -> Option<String> {
        match self {
            Self::Ed25519(info) => info.auth_key(),
            Self::MultiEd25519(info) => info.auth_key(),
            Self::SingleKey(info) => info.auth_key(),
            Self::MultiKey(info) => info.auth_key(),
        }
    }

    fn multikey_public_keys(&self) -> Vec<Vec<u8>> {
        match self {
            Self::Ed25519(_) => vec![],
            Self::MultiEd25519(info) => info.public_keys.clone(),
            Self::SingleKey(_) => vec![],
            Self::MultiKey(info) => info.public_keys.clone(),
        }
    }

    fn multikey_key_types(&self) -> Vec<Option<AnyPublicKeyType>> {
        match self {
            Self::Ed25519(_) => vec![],
            Self::MultiEd25519(info) => vec![None; info.public_keys.len()],
            Self::SingleKey(_) => vec![],
            Self::MultiKey(info) => info.key_types.clone(),
        }
    }

    fn multikey_threshold(&self) -> Option<u32> {
        match self {
            Self::Ed25519(_) => None,
            Self::MultiEd25519(info) => Some(info.threshold),
            Self::SingleKey(_) => None,
            Self::MultiKey(info) => Some(info.threshold),
        }
    }

    fn multikey_verified(&self) -> Vec<bool> {
        match self {
            Self::Ed25519(_) => vec![],
            Self::MultiEd25519(info) => info.verified.clone(),
            Self::SingleKey(_) => vec![],
            Self::MultiKey(info) => info.verified.clone(),
        }
    }

    fn any_public_key_type_prefix(type_enum_value: i32) -> Option<i32> {
        match type_enum_value {
            1 => Some(0x00), // Generalized Ed25519
            2 => Some(0x01), // Generalized Secp256k1Ecdsa
            3 => Some(0x02), // Generalized Secp256r1Ecdsa (WebAuthn)
            4 => Some(0x03), // Generalized Keyless
            _ => None,
        }
    }

    fn from_transaction_signature(signature: &Signature, transaction_version: i64) -> Option<Self> {
        let info = match signature {
            Signature::Ed25519(sig) => Self::ed25519(sig.public_key.clone()),
            Signature::MultiEd25519(sigs) => {
                let mut verified = vec![false; sigs.public_keys.len()];
                sigs.public_key_indices
                    .iter()
                    .for_each(|idx| verified[*idx as usize] = true);
                Self::multi_ed25519(sigs.threshold, sigs.signatures.clone(), verified)
            },
            Signature::SingleSender(single_sender) => {
                let account_signature = single_sender.sender.as_ref().unwrap();
                if account_signature.signature.is_none() {
                    warn!(
                        transaction_version = transaction_version,
                        "Transaction signature is unknown"
                    );
                    return None;
                }
                let signature_info = match account_signature.signature.as_ref().unwrap() {
                    AccountSignature::Ed25519(sig) => Self::ed25519(sig.public_key.clone()),
                    AccountSignature::MultiEd25519(sigs) => {
                        let mut verified = vec![false; sigs.public_keys.len()];
                        sigs.public_key_indices
                            .iter()
                            .for_each(|idx| verified[*idx as usize] = true);
                        Self::multi_ed25519(sigs.threshold, sigs.public_keys.clone(), verified)
                    },
                    AccountSignature::SingleKeySignature(sig) => Self::single_key(
                        Self::any_public_key_type_prefix(sig.public_key.as_ref().unwrap().r#type),
                        sig.public_key.as_ref().unwrap().public_key.clone(),
                    ),
                    AccountSignature::MultiKeySignature(sigs) => {
                        let mut verified = vec![false; sigs.public_keys.len()];
                        sigs.signatures.iter().for_each(|idx_sig| {
                            let idx = idx_sig.index as usize;
                            if idx < verified.len() {
                                verified[idx] = true;
                            }
                        });

                        let threshold = sigs.signatures_required;
                        let prefixes = sigs
                            .public_keys
                            .iter()
                            .map(|pk| Self::any_public_key_type_prefix(pk.r#type))
                            .collect::<Vec<_>>();
                        let public_keys = sigs
                            .public_keys
                            .iter()
                            .map(|pk| pk.public_key.clone())
                            .collect::<Vec<_>>();
                        Self::multi_key(threshold, prefixes, public_keys, verified)
                    },
                    AccountSignature::Abstraction(_sig) => return None,
                };
                signature_info
            },
            _ => return None, // TODO: Handle `MultiAgent` and `FeePayer` cases
        };

        Some(info)
    }
}

pub fn parse_account_restoration_models_from_transaction(
    txn: &Transaction,
) -> Option<(
    AuthKeyAccountAddress,
    Vec<PublicKeyAuthKey>,
    Option<AuthKeyMultikeyLayout>,
)> {
    let user_txn = match txn.txn_data.as_ref()? {
        TxnData::User(user_txn) => user_txn,
        _ => return None,
    };
    let txn_version = txn.version as i64;
    let address = user_txn.request.as_ref()?.sender.clone();
    let signature_info = SignatureInfo::from_transaction_signature(
        user_txn
            .request
            .as_ref()?
            .signature
            .as_ref()?
            .signature
            .as_ref()?,
        txn_version,
    )?;
    let auth_key = signature_info.auth_key().unwrap_or_default();

    let auth_key_account_address = AuthKeyAccountAddress {
        auth_key: auth_key.clone(),
        address,
        verified: true,
        last_transaction_version: txn_version,
    };

    let (auth_key_multikey_layout, public_key_auth_keys) = if signature_info.is_multikey() {
        let multikey_layouts = signature_info
            .multikey_public_keys()
            .iter()
            .zip(signature_info.multikey_key_types().iter())
            .map(|(pk, prefix)| {
                let pk_with_prefix = prefix.map_or_else(
                    || pk.clone(),
                    |key_type| {
                        let mut extended = vec![key_type.to_u8()]; // Public key type prefix
                        extended.extend(pk);
                        extended
                    },
                );
                format!("0x{}", hex::encode(pk_with_prefix))
            })
            .collect::<Vec<_>>();

        let multikey_pk_types = match &signature_info {
            SignatureInfo::MultiEd25519(_) => {
                vec![String::from("ed25519"); multikey_layouts.len()]
            },
            SignatureInfo::MultiKey(scheme) => scheme
                .key_types
                .iter()
                .map(|maybe_key_type| match maybe_key_type {
                    Some(key_type) => key_type.key_type_string(),
                    None => String::new(),
                })
                .collect(),
            _ => vec![],
        };

        let multikey_verified = signature_info.multikey_verified();
        let multikey_threshold = signature_info.multikey_threshold();

        let multikey_layout_with_prefixes = match serde_json::to_value(&multikey_layouts) {
            Ok(value) => value,
            Err(_) => {
                return Some((auth_key_account_address, vec![], None));
            },
        };

        let mut public_key_auth_keys = vec![];
        for ((pk, pk_type), verified) in signature_info
            .multikey_public_keys()
            .iter()
            .zip(multikey_pk_types.iter())
            .zip(multikey_verified.iter())
        {
            public_key_auth_keys.push(PublicKeyAuthKey {
                public_key: format!("0x{}", hex::encode(pk)),
                public_key_type: pk_type.clone(),
                auth_key: auth_key.clone(),
                verified: *verified,
                last_transaction_version: txn_version,
            });
        }

        (
            Some(AuthKeyMultikeyLayout {
                auth_key: auth_key.clone(),
                signatures_required: multikey_threshold.expect("should not be None") as i64,
                multikey_layout_with_prefixes,
                multikey_type: signature_info.signature_type_string(),
                last_transaction_version: txn_version,
            }),
            public_key_auth_keys,
        )
    } else {
        (None, vec![])
    };

    Some((
        auth_key_account_address,
        public_key_auth_keys,
        auth_key_multikey_layout,
    ))
}
