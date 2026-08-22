//! The client's settlement identity: one secp256k1 key serving as the
//! libp2p identity, the BZZ-address signer, and the chequebook issuer,
//! plus a persisted overlay nonce.
//!
//! The overlay nonce is ours (fresh, persisted next to nothing else in
//! the data dir); reusing the Phase-0 spike's *key* keeps its funded
//! chequebook without needing the spike's Go statestore — the
//! chequebook contract is bound to the issuer EOA, not to an overlay.

use anyhow::{Context, Result};
use libp2p::identity as p2p_identity;
use std::path::Path;

/// A loaded settlement identity.
pub struct Identity {
    /// secp256k1 secret: libp2p key, BZZ signer, cheque issuer.
    pub secret: [u8; 32],
    /// Ethereum EOA derived from `secret`.
    pub eth: [u8; 20],
    /// Overlay nonce (persisted; random on first run).
    pub nonce: [u8; 32],
    /// Swarm overlay = `keccak(eth ‖ network_id_le ‖ nonce)`.
    pub overlay: [u8; 32],
    /// libp2p keypair wrapping the same secret.
    pub keypair: p2p_identity::Keypair,
}

impl Identity {
    /// Load from a Web3 v3 keystore file plus a nonce file (created
    /// with fresh randomness if absent).
    ///
    /// # Errors
    /// Fails on unreadable/undecryptable keystore or unwritable nonce
    /// path.
    pub fn load(
        swarm_key_path: &Path,
        password: &str,
        nonce_path: &Path,
        network_id: u64,
    ) -> Result<Self> {
        let json = std::fs::read_to_string(swarm_key_path)
            .with_context(|| format!("read {}", swarm_key_path.display()))?;
        let secret = crate::keystore::decrypt_v3(&json, password)?;

        let mut nonce = [0u8; 32];
        if let Ok(text) = std::fs::read_to_string(nonce_path) {
            hex::decode_to_slice(text.trim(), &mut nonce)
                .with_context(|| format!("parse nonce file {}", nonce_path.display()))?;
        } else {
            getrandom::fill(&mut nonce).context("generate overlay nonce")?;
            if let Some(parent) = nonce_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(nonce_path, hex::encode(nonce))
                .with_context(|| format!("persist nonce to {}", nonce_path.display()))?;
        }

        let signing = k256::ecdsa::SigningKey::from_bytes((&secret).into())
            .context("secret is not a valid secp256k1 scalar")?;
        let eth = ant_crypto::ethereum_address_from_public_key(signing.verifying_key());
        let overlay = ant_crypto::overlay_from_ethereum_address(&eth, network_id, &nonce);

        let mut secret_copy = secret;
        let sk = p2p_identity::secp256k1::SecretKey::try_from_bytes(&mut secret_copy)
            .context("libp2p secret")?;
        let keypair = p2p_identity::Keypair::from(p2p_identity::secp256k1::Keypair::from(sk));

        Ok(Self {
            secret,
            eth,
            nonce,
            overlay,
            keypair,
        })
    }
}
