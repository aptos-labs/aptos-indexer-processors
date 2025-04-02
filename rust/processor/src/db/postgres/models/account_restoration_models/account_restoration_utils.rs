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
    MultiEd25519Signature, Transaction, UserTransactionRequest,
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

fn get_auth_key_preimage_for_public_key(
    key_type: &AnyPublicKeyType,
    public_key: Vec<u8>,
) -> Vec<u8> {
    let mut preimage = vec![key_type.to_u8()];
    match key_type {
        AnyPublicKeyType::Ed25519 => preimage.push(0x20),
        AnyPublicKeyType::Secp256k1Ecdsa => preimage.push(0x41),
        _ => {},
    }
    preimage.extend_from_slice(&public_key);
    preimage
}

struct SingleKeyAuthKeyScheme {
    key_type: Option<AnyPublicKeyType>,
    public_key: Vec<u8>,
}

impl AuthKeyScheme for SingleKeyAuthKeyScheme {
    const SCHEME: u8 = 0x02;

    fn auth_key(&self) -> Option<String> {
        if let Some(key_type) = &self.key_type {
            let mut preimage =
                get_auth_key_preimage_for_public_key(key_type, self.public_key.clone());
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
            preimage.extend_from_slice(&get_auth_key_preimage_for_public_key(
                &key_type.expect("should not be None"),
                public_key.clone(),
            ));
        }

        preimage.push(self.threshold.try_into().ok()?);
        preimage.push(Self::SCHEME);
        println!("preimage: {:?}", hex::encode(preimage.clone()));
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

    fn multi_ed25519_from_transaction_signature(signature: &MultiEd25519Signature) -> Self {
        let mut verified = vec![false; signature.public_keys.len()];
        signature
            .public_key_indices
            .iter()
            .for_each(|idx| verified[*idx as usize] = true);
        Self::multi_ed25519(signature.threshold, signature.public_keys.clone(), verified)
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

    fn from_account_signature(account_signature: &AccountSignature) -> Option<Self> {
        match account_signature {
            AccountSignature::Ed25519(sig) => Some(Self::ed25519(sig.public_key.clone())),
            AccountSignature::MultiEd25519(sig) => {
                Some(Self::multi_ed25519_from_transaction_signature(sig))
            },
            AccountSignature::SingleKeySignature(sig) => Some(Self::single_key(
                Self::any_public_key_type_prefix(sig.public_key.as_ref().unwrap().r#type),
                sig.public_key.as_ref().unwrap().public_key.clone(),
            )),
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
                Some(Self::multi_key(threshold, prefixes, public_keys, verified))
            },
            AccountSignature::Abstraction(_sig) => None,
        }
    }
}

fn get_signature_infos_from_user_txn_request(
    user_txn_request: &UserTransactionRequest,
    transaction_version: i64,
) -> Vec<(String, SignatureInfo)> {
    let signature = match &user_txn_request.signature {
        Some(sig) => match &sig.signature {
            Some(s) => s,
            None => return vec![],
        },
        None => return vec![],
    };
    let sender_address = user_txn_request.sender.clone();
    match signature {
        Signature::Ed25519(sig) => vec![(
            sender_address,
            SignatureInfo::ed25519(sig.public_key.clone()),
        )],
        Signature::MultiEd25519(sig) => vec![(
            sender_address,
            SignatureInfo::multi_ed25519_from_transaction_signature(sig),
        )],
        Signature::SingleSender(single_sender) => {
            let sender_signature = single_sender.sender.as_ref().unwrap();
            if sender_signature.signature.is_none() {
                warn!(
                    transaction_version = transaction_version,
                    "Transaction signature is unknown"
                );
                return vec![];
            };
            let account_signature = sender_signature.signature.as_ref().unwrap();

            if let Some(sender_info) = SignatureInfo::from_account_signature(account_signature) {
                vec![(sender_address, sender_info)]
            } else {
                vec![]
            }
        },

        Signature::FeePayer(sig) => {
            let account_signature = sig.sender.as_ref().unwrap().signature.as_ref().unwrap();
            let fee_payer_address = sig.fee_payer_address.clone();
            let fee_payer_signature = sig
                .fee_payer_signer
                .as_ref()
                .unwrap()
                .signature
                .as_ref()
                .unwrap();

            let mut signature_infos = vec![];

            // Add sender signature if valid
            if let Some(sender_info) = SignatureInfo::from_account_signature(account_signature) {
                signature_infos.push((sender_address, sender_info));
            }

            // Add fee payer signature if valid
            if let Some(fee_payer_info) = SignatureInfo::from_account_signature(fee_payer_signature)
            {
                signature_infos.push((fee_payer_address, fee_payer_info));
            }

            // Add secondary signer signatures
            for (address, signer) in sig
                .secondary_signer_addresses
                .iter()
                .zip(sig.secondary_signers.iter())
            {
                if let Some(signature) = signer.signature.as_ref() {
                    if let Some(signature_info) = SignatureInfo::from_account_signature(signature) {
                        signature_infos.push((address.clone(), signature_info));
                    }
                }
            }
            signature_infos
        },
        _ => vec![], // TODO: Handle `MultiAgent`
    }
}

pub fn parse_account_restoration_models_from_transaction(
    txn: &Transaction,
) -> Vec<(
    AuthKeyAccountAddress,
    Vec<PublicKeyAuthKey>,
    Option<AuthKeyMultikeyLayout>,
)> {
    let user_txn = match txn.txn_data.as_ref() {
        Some(TxnData::User(user_txn)) => user_txn,
        _ => return vec![],
    };
    let user_txn_request = match &user_txn.request {
        Some(req) => req,
        None => return vec![],
    };
    let txn_version = txn.version as i64;
    let signature_infos = get_signature_infos_from_user_txn_request(user_txn_request, txn_version);

    let mut results = vec![];
    for (address, signature_info) in signature_infos {
        let auth_key = signature_info.auth_key().unwrap_or_default();
        let txn_version = txn.version as i64;

        let auth_key_account_address = AuthKeyAccountAddress {
            auth_key: auth_key.clone(),
            address: address.clone(),
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
                    results.push((auth_key_account_address, vec![], None));
                    continue;
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

        results.push((
            auth_key_account_address,
            public_key_auth_keys,
            auth_key_multikey_layout,
        ));
    }

    results
}

#[cfg(test)]
mod tests {
    use crate::schema::auth_key_account_addresses::auth_key;

    use super::*;

    #[test]
    fn test_signature_info_auth_key_single_key() {
        let pk = hex::decode("c5eba39b323f488de5087353914f7149af93a260011a595ba568ec6f86003dc1")
            .unwrap();
        let signature_info = SignatureInfo::single_key(Some(0x00), pk.clone().into());

        let authkey: String =
            "0x64d95d138a390d9d83f2da145e9d5024c64df039cc10f97b1cc80f5b354aaa50".to_string();

        assert_eq!(signature_info.auth_key(), Some(authkey));
    }

    #[test]
    fn test_signature_info_auth_key_ed25519() {
        let pk = hex::decode("6bfcf20b8379714e4964c8d4571f37b580bedec3f7d158c2fbfe2f062b0a38ee")
            .unwrap();
        let signature_info = SignatureInfo::ed25519(pk.clone().into());

        let authkey: String =
            "0x96e02acc341cae64968049bb933359c08181f82820bfdf71f31c2778e4636c7a".to_string();

        assert_eq!(signature_info.auth_key(), Some(authkey));
    }

    #[test]
    fn test_signature_info_auth_key_multi_key() {
        let pk1 = hex::decode("4b00b5e1bd5738bc744bb59e25e5050e8c9aedfbd4ea20f994d9c6753adebc59")
            .unwrap();
        let pk2 = hex::decode("9f38f6a18300f77d652627abf8eedacae756748c145e1dfcd8ee3b62c8d189ad")
            .unwrap();
        let pk3 = hex::decode("95e2326a4d53ea79b6b97d8ed0b97dbf257cb34e80681031ed358176c36cd00f")
            .unwrap();
        let signature_info =
            SignatureInfo::multi_ed25519(2, vec![pk1, pk2, pk3], vec![true, true, true]);

        let authkey: String =
            "0x4f63487b2133fbca2c4fe1cb4aeb4ef1386d8a1ffd12a62bc3d82de0c04a8578".to_string();

        assert_eq!(signature_info.auth_key(), Some(authkey));
    }

    #[test]
    fn test_signature_info_auth_key_multi_ed25519() {
        let pk1 = hex::decode("f7a77d79ec5966e81bdd13d49b13f192e298e2ab7bcef1dc59f5bbcc901b93b0")
            .unwrap();
        let pk2 = hex::decode("6e390d64f6e34ef6c9755d33b47f914621129bd9a2e55ad3752e2179fcbf27d9")
            .unwrap();
        let pk3 = hex::decode("046d2ab40ad4efcacce374fdd32b552d440b93c640a02bd2db18780527a05ef55e2fa41510e016342d1bc47af1112c2ec040005eed482ce74bdb7dbc5138261354").unwrap();
        let signature_info = SignatureInfo::multi_key(
            2,
            vec![Some(0x00), Some(0x00), Some(0x01)],
            vec![pk1, pk2, pk3],
            vec![true, true, true],
        );

        let authkey: String =
            "0xc244c6fc130ee5d1def33f4c37402d795e2e2124fb3c925c542af36c2b1667bf".to_string();

        assert_eq!(signature_info.auth_key(), Some(authkey));
    }
}
