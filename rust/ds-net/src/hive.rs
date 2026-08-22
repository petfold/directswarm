//! hive gossip decode: parse a `/swarm/hive/{1.1.0,2.0.0}/peers`
//! payload into peer hints (overlay + underlays + peer id).
//!
//! The protobuf shape and the bee-2.8 multi-underlay `0x99` list
//! framing mirror ant's private `sinks`/`underlay` modules (which
//! aren't re-exported); the wire is small and stable, so we replicate
//! it here rather than fork ant. Signatures/nonces are deliberately
//! NOT verified — a hint is only a dial candidate; the BZZ handshake
//! re-derives and verifies the overlay when we actually connect.

use libp2p::{Multiaddr, PeerId};
use prost::Message;

/// hive.proto `message Peers { repeated BzzAddress peers = 1; }`.
#[derive(Clone, PartialEq, Message)]
struct PeersPb {
    #[prost(message, repeated, tag = "1")]
    peers: Vec<BzzAddressPb>,
}

/// hive.proto `message BzzAddress`. Tags 1–4 are bee 2.7; tags 5
/// (timestamp) and 6 (chequebook) arrived in 2.8 and prost tolerates
/// them as unknown fields even on the 1.1.0 path.
#[derive(Clone, PartialEq, Message)]
struct BzzAddressPb {
    #[prost(bytes = "vec", tag = "1")]
    underlay: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    signature: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    overlay: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    nonce: Vec<u8>,
}

/// A dial candidate learned from hive gossip.
#[derive(Debug, Clone)]
pub struct PeerHint {
    pub peer_id: PeerId,
    pub overlay: [u8; 32],
    pub underlays: Vec<Multiaddr>,
}

const UNDERLAY_LIST_PREFIX: u8 = 0x99;
const MAX_UNDERLAY_BYTES: usize = 2048;
const MAX_UNDERLAYS_PER_PEER: usize = 20;

/// Decode a hive `Peers` payload. Malformed entries are skipped, not
/// fatal (a single bad record must not drop a whole gossip batch).
#[must_use]
pub fn decode_peers(body: &[u8]) -> Vec<PeerHint> {
    let Ok(msg) = PeersPb::decode(body) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(msg.peers.len());
    for p in msg.peers {
        if p.overlay.len() != 32 {
            continue;
        }
        let Ok(underlays) = deserialize_underlays(&p.underlay) else {
            continue;
        };
        let Some(peer_id) = underlays.iter().find_map(extract_peer_id) else {
            continue;
        };
        let mut overlay = [0u8; 32];
        overlay.copy_from_slice(&p.overlay);
        out.push(PeerHint {
            peer_id,
            overlay,
            underlays,
        });
    }
    out
}

fn extract_peer_id(addr: &Multiaddr) -> Option<PeerId> {
    addr.iter().find_map(|p| match p {
        libp2p::multiaddr::Protocol::P2p(peer) => Some(peer),
        _ => None,
    })
}

/// Deserialize the `underlay` field: either a single multiaddr, or a
/// `0x99`-prefixed varint-length-delimited list (bee 2.8).
fn deserialize_underlays(data: &[u8]) -> Result<Vec<Multiaddr>, UnderlayError> {
    if data.is_empty() {
        return Err(UnderlayError::Empty);
    }
    if data.len() > MAX_UNDERLAY_BYTES {
        return Err(UnderlayError::TooLarge);
    }
    if data[0] == UNDERLAY_LIST_PREFIX {
        return deserialize_list(&data[1..]);
    }
    Ok(vec![Multiaddr::try_from(data.to_vec())?])
}

fn deserialize_list(mut data: &[u8]) -> Result<Vec<Multiaddr>, UnderlayError> {
    let mut out = Vec::new();
    while !data.is_empty() {
        if out.len() >= MAX_UNDERLAYS_PER_PEER {
            return Err(UnderlayError::TooMany);
        }
        let (len, rest) = unsigned_varint::decode::u64(data).map_err(|_| UnderlayError::Varint)?;
        let len = usize::try_from(len).map_err(|_| UnderlayError::Varint)?;
        if rest.len() < len {
            return Err(UnderlayError::Empty);
        }
        out.push(Multiaddr::try_from(rest[..len].to_vec())?);
        data = &rest[len..];
    }
    Ok(out)
}

#[derive(Debug)]
enum UnderlayError {
    Empty,
    TooLarge,
    TooMany,
    Varint,
    Multiaddr,
}

impl From<libp2p::multiaddr::Error> for UnderlayError {
    fn from(_: libp2p::multiaddr::Error) -> Self {
        UnderlayError::Multiaddr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A real mainnet BzzAddress underlay from Phase-0 reach.csv:
    // /ip4/116.202.173.4/tcp/32286/p2p/Qme...
    fn sample_addr() -> Multiaddr {
        "/ip4/116.202.173.4/tcp/32286/p2p/QmeEpqxyATA9n9Qag1iqKENsCHmSPy3WdqQhd4M467Dr6V"
            .parse()
            .unwrap()
    }

    #[test]
    fn decodes_single_underlay_record() {
        let addr = sample_addr();
        let pb = PeersPb {
            peers: vec![BzzAddressPb {
                underlay: addr.to_vec(),
                signature: vec![],
                overlay: vec![0x11; 32],
                nonce: vec![],
            }],
        };
        let hints = decode_peers(&pb.encode_to_vec());
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].overlay, [0x11; 32]);
        assert_eq!(hints[0].underlays, vec![addr]);
    }

    #[test]
    fn decodes_099_underlay_list() {
        let addr = sample_addr();
        let bytes = addr.to_vec();
        let mut framed = vec![UNDERLAY_LIST_PREFIX];
        let mut lenbuf = unsigned_varint::encode::u64_buffer();
        framed.extend_from_slice(unsigned_varint::encode::u64(
            bytes.len() as u64,
            &mut lenbuf,
        ));
        framed.extend_from_slice(&bytes);
        let got = deserialize_underlays(&framed).unwrap();
        assert_eq!(got, vec![addr]);
    }

    #[test]
    fn skips_wrong_overlay_len_and_underlayless() {
        let pb = PeersPb {
            peers: vec![
                BzzAddressPb {
                    underlay: sample_addr().to_vec(),
                    signature: vec![],
                    overlay: vec![0x11; 20], // wrong length
                    nonce: vec![],
                },
                BzzAddressPb {
                    underlay: "/ip4/1.2.3.4/tcp/5".parse::<Multiaddr>().unwrap().to_vec(),
                    signature: vec![],
                    overlay: vec![0x22; 32], // valid overlay but no /p2p
                    nonce: vec![],
                },
            ],
        };
        assert!(decode_peers(&pb.encode_to_vec()).is_empty());
    }
}
