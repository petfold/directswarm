//! directswarm CLI. M1 delivers `directswarm fetch <ref> [-o file]`
//! over the forwarding-fallback path; the fast plane layers in from M2.

fn main() {
    eprintln!(
        "directswarm {}: fetch is not implemented yet (Phase 1, M1)",
        env!("CARGO_PKG_VERSION")
    );
    std::process::exit(2);
}
