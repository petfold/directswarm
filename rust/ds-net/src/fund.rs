//! Wallet → chequebook funding: a plain ERC-20 BZZ transfer to the
//! chequebook contract (a chequebook's balance IS its ERC-20 balance).
//! Spends real xBZZ; the caller reports balances and the tx hash.

use ant_chain::chequebook::GNOSIS_BZZ_TOKEN_BYTES;
use ant_chain::tx::{
    erc20_transfer_calldata, sign_legacy_tx, LegacyTx, DEFAULT_GAS_PRICE_WEI, ERC20_TRANSFER_GAS,
    GNOSIS_CHAIN_ID,
};
use ant_chain::ChainClient;
use anyhow::{anyhow, bail, Result};
use primitive_types::U256;
use std::time::Duration;

use crate::identity::Identity;

#[derive(Debug)]
pub struct FundOutcome {
    pub tx_hash: [u8; 32],
    pub block_number: u64,
    pub wallet_before_plur: u128,
    pub wallet_after_plur: u128,
    pub book_before_plur: u128,
    pub book_after_plur: u128,
}

/// Transfer `amount_plur` BZZ from the identity's wallet to
/// `chequebook`, waiting for the receipt.
///
/// # Errors
/// Fails on RPC errors, an insufficient wallet balance, or a receipt
/// that does not arrive within 90 s (the tx may still mine — the
/// error carries its hash).
pub async fn fund_chequebook(
    id: &Identity,
    chequebook: [u8; 20],
    amount_plur: u128,
    rpc_url: &str,
) -> Result<FundOutcome> {
    let client = ChainClient::new(rpc_url.to_owned());
    let token_hex = format!("0x{}", hex::encode(GNOSIS_BZZ_TOKEN_BYTES));

    let wallet_before = client
        .erc20_balance_of_lower128(&token_hex, &id.eth)
        .await
        .map_err(|e| anyhow!("wallet balance read: {e}"))?;
    let book_before = client
        .erc20_balance_of_lower128(&token_hex, &chequebook)
        .await
        .map_err(|e| anyhow!("chequebook balance read: {e}"))?;
    if wallet_before < amount_plur {
        bail!(
            "wallet holds {wallet_before} PLUR, below the requested deposit of {amount_plur} PLUR"
        );
    }

    let nonce = client
        .eth_get_transaction_count_pending(&id.eth)
        .await
        .map_err(|e| anyhow!("nonce: {e}"))?;
    let tx = LegacyTx {
        nonce,
        gas_price_wei: DEFAULT_GAS_PRICE_WEI,
        gas_limit: ERC20_TRANSFER_GAS,
        to: GNOSIS_BZZ_TOKEN_BYTES,
        value_wei: U256::zero(),
        data: erc20_transfer_calldata(&chequebook, &U256::from(amount_plur)),
        chain_id: GNOSIS_CHAIN_ID,
    };
    let (raw, tx_hash) = sign_legacy_tx(&id.secret, &tx).map_err(|e| anyhow!("sign: {e}"))?;
    client
        .eth_send_raw_transaction(&raw)
        .await
        .map_err(|e| anyhow!("send: {e}"))?;
    let receipt = client
        .wait_for_receipt(&tx_hash, Duration::from_secs(90))
        .await
        .map_err(|e| anyhow!("receipt for 0x{}: {e}", hex::encode(tx_hash)))?;

    let wallet_after = client
        .erc20_balance_of_lower128(&token_hex, &id.eth)
        .await
        .unwrap_or(wallet_before);
    let book_after = client
        .erc20_balance_of_lower128(&token_hex, &chequebook)
        .await
        .unwrap_or(book_before);
    Ok(FundOutcome {
        tx_hash,
        block_number: receipt.block_number,
        wallet_before_plur: wallet_before,
        wallet_after_plur: wallet_after,
        book_before_plur: book_before,
        book_after_plur: book_after,
    })
}
