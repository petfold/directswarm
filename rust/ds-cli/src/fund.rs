//! `directswarm fund-chequebook` — move BZZ from the settlement
//! wallet into its chequebook. Spends real xBZZ — prints balances
//! before and after and waits for the receipt. Wallet→chequebook
//! deposits are covered by the user's standing grant (2026-08-22);
//! the run that needs them still gets its own sign-off.

use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct FundArgs {
    /// Amount to deposit, in PLUR (1e16 PLUR = 1 xBZZ).
    #[arg(long)]
    pub amount_plur: u128,
    /// Web3 v3 keystore holding the settlement (issuer) key.
    #[arg(long, default_value = "../.phase0/reach-data/keys/swarm.key")]
    pub swarm_key: PathBuf,
    #[arg(long, default_value = "directswarm-reach")]
    pub password: String,
    #[arg(long, default_value = "../.phase1/identity/overlay-nonce.hex")]
    pub nonce_file: PathBuf,
    /// Destination chequebook contract (0x hex).
    #[arg(long, default_value = "0xE8C7aD1Af8CAb91E2695EfD1a12dBfCc186dFD41")]
    pub chequebook: String,
    #[arg(long, default_value = "https://rpc.gnosischain.com")]
    pub rpc_url: String,
}

fn parse_hex20(s: &str) -> Result<[u8; 20], String> {
    let s = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    let mut out = [0u8; 20];
    hex::decode_to_slice(s, &mut out).map_err(|e| format!("chequebook: {e}"))?;
    Ok(out)
}

#[allow(clippy::cast_precision_loss)]
fn xbzz(plur: u128) -> f64 {
    plur as f64 / 1e16
}

pub async fn run(args: FundArgs) -> i32 {
    let chequebook = match parse_hex20(&args.chequebook) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: {e}");
            return 2;
        }
    };
    let identity =
        match ds_net::Identity::load(&args.swarm_key, &args.password, &args.nonce_file, 1) {
            Ok(id) => id,
            Err(e) => {
                eprintln!("error: identity: {e:#}");
                return 1;
            }
        };
    eprintln!(
        "depositing {:.4} xBZZ: wallet 0x{} -> chequebook 0x{}",
        xbzz(args.amount_plur),
        hex::encode(identity.eth),
        hex::encode(chequebook)
    );
    match ds_net::fund::fund_chequebook(&identity, chequebook, args.amount_plur, &args.rpc_url)
        .await
    {
        Ok(o) => {
            println!(
                "deposit done (tx 0x{}, block {}): wallet {:.4} -> {:.4} xBZZ; chequebook {:.4} -> {:.4} xBZZ",
                hex::encode(o.tx_hash),
                o.block_number,
                xbzz(o.wallet_before_plur),
                xbzz(o.wallet_after_plur),
                xbzz(o.book_before_plur),
                xbzz(o.book_after_plur)
            );
            0
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            1
        }
    }
}
