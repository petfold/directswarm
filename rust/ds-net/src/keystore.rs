//! Web3 Secret Storage v3 keystore decryption (bee's `swarm.key`
//! format).
//!
//! Vendored from `ant` (`crates/antd/src/keystore.rs`, MIT/Apache-2.0,
//! solardev-xyz/ant @ c526a33) — `antd` is a binary crate, so the
//! module can't be depended on directly. Logic unchanged.
//!
//! 1. Derive a key from the password (`scrypt` or `pbkdf2` with
//!    `hmac-sha256`).
//! 2. Verify `keccak256(derived[16..32] ‖ ciphertext) == mac`; a
//!    mismatch means the wrong password.
//! 3. AES-128-CTR-decrypt the ciphertext with `derived[0..16]` as key
//!    and the stored `iv` as the initial counter. The plaintext is the
//!    32-byte private key.

use aes::Aes128;
use anyhow::{anyhow, bail, Context, Result};
use ctr::cipher::{KeyIvInit, StreamCipher};
use ctr::Ctr128BE;
use serde::Deserialize;
use sha2::Sha256;

type Aes128Ctr = Ctr128BE<Aes128>;

#[derive(Deserialize)]
struct Keystore {
    #[serde(alias = "Crypto")]
    crypto: Crypto,
    version: u32,
}

#[derive(Deserialize)]
struct Crypto {
    cipher: String,
    cipherparams: CipherParams,
    ciphertext: String,
    kdf: String,
    kdfparams: serde_json::Value,
    mac: String,
}

#[derive(Deserialize)]
struct CipherParams {
    iv: String,
}

/// Decrypt a Web3 v3 keystore JSON blob, returning the 32-byte private
/// key.
///
/// # Errors
/// Distinguishes a wrong password (MAC mismatch) from a malformed /
/// unsupported keystore so the operator gets an actionable message.
pub fn decrypt_v3(json: &str, password: &str) -> Result<[u8; 32]> {
    let ks: Keystore = serde_json::from_str(json).context("parse v3 keystore json")?;
    if ks.version != 3 {
        bail!(
            "unsupported keystore version {} (only v3 is supported)",
            ks.version
        );
    }
    let c = &ks.crypto;
    if !c.cipher.eq_ignore_ascii_case("aes-128-ctr") {
        bail!(
            "unsupported keystore cipher {:?} (only aes-128-ctr)",
            c.cipher
        );
    }

    let ciphertext = decode_hex(&c.ciphertext).context("ciphertext")?;
    let iv = decode_hex(&c.cipherparams.iv).context("cipher iv")?;
    if iv.len() != 16 {
        bail!("cipher iv must be 16 bytes, got {}", iv.len());
    }

    let derived = derive_key(&c.kdf, &c.kdfparams, password.as_bytes())?;
    if derived.len() < 32 {
        bail!("derived key too short ({} bytes)", derived.len());
    }

    // MAC = keccak256(derived[16..32] ‖ ciphertext).
    let mut mac_input = Vec::with_capacity(16 + ciphertext.len());
    mac_input.extend_from_slice(&derived[16..32]);
    mac_input.extend_from_slice(&ciphertext);
    let mac = ant_crypto::keccak256(&mac_input);
    let want_mac = decode_hex(&c.mac).context("mac")?;
    if mac.as_slice() != want_mac.as_slice() {
        bail!("keystore MAC mismatch — wrong password or corrupt keystore");
    }

    // AES-128-CTR decrypt in place.
    let mut buf = ciphertext;
    let mut cipher = Aes128Ctr::new_from_slices(&derived[..16], &iv)
        .map_err(|e| anyhow!("init aes-128-ctr: {e}"))?;
    cipher.apply_keystream(&mut buf);

    if buf.len() != 32 {
        bail!("decrypted secret is {} bytes, expected 32", buf.len());
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&buf);
    Ok(out)
}

/// Derive the symmetric key from the password per the `kdf` field.
fn derive_key(kdf: &str, params: &serde_json::Value, password: &[u8]) -> Result<Vec<u8>> {
    let dklen = usize::try_from(
        params
            .get("dklen")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(32),
    )
    .context("dklen out of range")?;
    if dklen < 32 {
        bail!("kdf dklen {dklen} too small (need >= 32)");
    }
    let salt = decode_hex(
        params
            .get("salt")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow!("kdfparams.salt missing"))?,
    )
    .context("kdf salt")?;

    let mut out = vec![0u8; dklen];
    match kdf.to_ascii_lowercase().as_str() {
        "scrypt" => {
            let n = u64_param(params, "n")?;
            let r = u32::try_from(u64_param(params, "r")?).context("scrypt r")?;
            let p = u32::try_from(u64_param(params, "p")?).context("scrypt p")?;
            if !n.is_power_of_two() {
                bail!("scrypt n={n} is not a power of two");
            }
            let log_n = u8::try_from(n.trailing_zeros()).context("scrypt n")?;
            let sparams = scrypt::Params::new(log_n, r, p, dklen)
                .map_err(|e| anyhow!("invalid scrypt params: {e}"))?;
            scrypt::scrypt(password, &salt, &sparams, &mut out)
                .map_err(|e| anyhow!("scrypt: {e}"))?;
        }
        "pbkdf2" => {
            let prf = params
                .get("prf")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("hmac-sha256");
            if !prf.eq_ignore_ascii_case("hmac-sha256") {
                bail!("unsupported pbkdf2 prf {prf:?} (only hmac-sha256)");
            }
            let c = u32::try_from(u64_param(params, "c")?).context("pbkdf2 c")?;
            pbkdf2::pbkdf2_hmac::<Sha256>(password, &salt, c, &mut out);
        }
        other => bail!("unsupported kdf {other:?}"),
    }
    Ok(out)
}

fn u64_param(params: &serde_json::Value, key: &str) -> Result<u64> {
    params
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| anyhow!("kdfparams.{key} missing or not an integer"))
}

fn decode_hex(s: &str) -> Result<Vec<u8>> {
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    hex::decode(s).map_err(|e| anyhow!("invalid hex: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Canonical Web3 Secret Storage pbkdf2 test vector (password
    // "testpassword"). The canonical scrypt vector uses `r=1`, which
    // violates RustCrypto's strict scrypt bound; real bee keystores use
    // `r=8`.
    const EXPECTED_KEY: &str = "7a28b5ba57c53603b0b07b56bba752f7784bf506fa95edc395f5cf6c7514fe9d";

    const PBKDF2_KEYSTORE: &str = r#"{
        "crypto": {
            "cipher": "aes-128-ctr",
            "cipherparams": { "iv": "6087dab2f9fdbbfaddc31a909735c1e6" },
            "ciphertext": "5318b4d5bcd28de64ee5559e671353e16f075ecae9f99c7a79a38af5f869aa46",
            "kdf": "pbkdf2",
            "kdfparams": {
                "c": 262144,
                "dklen": 32,
                "prf": "hmac-sha256",
                "salt": "ae3cd4e7013836a3df6bd7241b12db061dbe2c6785853cce422d148a624ce0bd"
            },
            "mac": "517ead924a9d0dc3124507e3393d175ce3ff7c1e96529c6c555ce9e51205e9b2"
        },
        "version": 3
    }"#;

    #[test]
    fn decrypts_canonical_pbkdf2_vector() {
        let key = decrypt_v3(PBKDF2_KEYSTORE, "testpassword").expect("decrypt");
        assert_eq!(hex::encode(key), EXPECTED_KEY);
    }

    #[test]
    fn wrong_password_is_mac_mismatch() {
        let err = decrypt_v3(PBKDF2_KEYSTORE, "nope").unwrap_err();
        assert!(err.to_string().contains("MAC mismatch"), "{err}");
    }
}
