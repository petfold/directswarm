#!/usr/bin/env python3
# M6.3 — stake-registry sweep: enumerate every staked overlay from the
# mainnet staking contract's StakeUpdated events (public Gnosis RPC,
# zero Swarm-network load). Output: .phase1/stake-registry.csv with the
# LATEST event per overlay (potential_stake 0 = withdrawn/inactive).
import json, time, urllib.request, sys, os

RPC = "https://rpc.gnosischain.com"
STAKING = "0xda2a16ee889e7f04980a8d597b48c8d51b9518f4"
OUT = os.path.join(os.path.dirname(__file__), "stake-registry.csv")
START_BLOCK = 30_000_000  # well before the current staking contract era


def rpc(method, params, retries=6):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    for attempt in range(retries):
        try:
            req = urllib.request.Request(RPC, body, {"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:
                v = json.load(r)
            if "error" in v:
                raise RuntimeError(v["error"])
            return v["result"]
        except Exception as e:  # noqa: BLE001 — rate limits / transient
            if attempt == retries - 1:
                raise
            time.sleep(2 * (attempt + 1))
    return None


topic0 = rpc("web3_sha3", ["0x" + "StakeUpdated(address,uint256,uint256,bytes32,uint256,uint8)".encode().hex()])
tip = int(rpc("eth_blockNumber", []), 16)
print(f"topic0 {topic0}, tip {tip}", flush=True)

overlays = {}  # overlay_hex -> (owner, potential_stake, last_block, height)
frm, window, calls = START_BLOCK, 400_000, 0
while frm <= tip:
    to = min(frm + window - 1, tip)
    try:
        logs = rpc("eth_getLogs", [{
            "address": STAKING,
            "topics": [topic0],
            "fromBlock": hex(frm),
            "toBlock": hex(to),
        }])
    except Exception as e:  # window too big for the endpoint → shrink
        if window > 10_000:
            window //= 2
            print(f"shrink window -> {window} ({e})", flush=True)
            continue
        raise
    calls += 1
    for lg in logs:
        data = lg["data"][2:]
        words = [data[i:i + 64] for i in range(0, len(data), 64)]
        owner = "0x" + lg["topics"][1][-40:]
        committed, potential, overlay = int(words[0], 16), int(words[1], 16), words[2]
        blk = int(lg["blockNumber"], 16)
        prev = overlays.get(overlay)
        if prev is None or blk >= prev[2]:
            overlays[overlay] = (owner, potential, blk, int(words[4], 16))
    if calls % 10 == 0:
        print(f"block {to}/{tip}: {len(overlays)} overlays", flush=True)
    frm = to + 1
    if window < 400_000 and calls % 20 == 0:
        window = min(window * 2, 400_000)  # recover after transient shrink
    time.sleep(0.35)  # politeness to the public endpoint

active = {k: v for k, v in overlays.items() if v[1] > 0}
with open(OUT, "w") as f:
    f.write("overlay_hex,owner,potential_stake_plur,last_updated_block,height\n")
    for ov, (owner, pot, blk, h) in sorted(overlays.items()):
        f.write(f"{ov},{owner},{pot},{blk},{h}\n")
print(f"DONE: {len(overlays)} staked overlays ever, {len(active)} with active stake -> {OUT}", flush=True)
