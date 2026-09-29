"""Verify the Robinhood UniversalRouter command set on an anvil fork.

Run:  anvil --fork-url http://127.0.0.1:8547 --port 29545 --chain-id 4663
      .venv/bin/python ur_probe.py http://127.0.0.1:29545

Every assertion here is a chain fact the TxRouter design relies on. The script
sends real transactions on the fork from anvil's unlocked dev account and reads
balances back, so "verified" means "executed and reconciled to the wei".
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request

from eth_abi import encode
from eth_keys import keys
from eth_utils import keccak

RPC = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:29545"
UR = "0x8876789976decbfcbbbe364623c63652db8c0904"
PERMIT2 = "0x000000000022d473030f116ddee9f6b43ac78ba3"
WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
V4_QUOTER = "0x8dc178efb8111bb0973dd9d722ebeff267c98f94"
PONS_HOOK = "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044"
MEME = "0xb3377f953994c6fa04383a64d88aa8122e81fd62"  # ITH, Pons pool eda3…3fb0 (native/ITH, fee 0, spacing 200)
NATIVE = "0x0000000000000000000000000000000000000000"
FEE_TO = "0x00000000000000000000000000000000000fee75"
# A fresh key per run: anvil's dev account 0 carries an EIP-7702 delegation on chain 4663
# (code 0xef0100…), which makes Permit2 take the ERC-1271 path and reject EOA signatures.
_PROBE_KEY = keys.PrivateKey(os.urandom(32))
USER = _PROBE_KEY.public_key.to_checksum_address().lower()
USER_KEY = _PROBE_KEY.to_hex()

MSG_SENDER = "0x0000000000000000000000000000000000000001"
ADDRESS_THIS = "0x0000000000000000000000000000000000000002"
CONTRACT_BALANCE = 1 << 255
OPEN_DELTA = 0
FEE_BPS = 75

# UniversalRouter command bytes (Uniswap universal-router Commands.sol).
V3_SWAP_EXACT_IN, PERMIT2_TRANSFER_FROM, SWEEP, PAY_PORTION = 0x00, 0x02, 0x04, 0x06
V2_SWAP_EXACT_IN, PERMIT2_PERMIT, WRAP_ETH, UNWRAP_WETH, V4_SWAP = 0x08, 0x0A, 0x0B, 0x0C, 0x10
# V4 router actions (v4-periphery Actions.sol).
SWAP_EXACT_IN_SINGLE, SWAP_EXACT_IN, SETTLE, SETTLE_ALL, TAKE, TAKE_ALL = 0x06, 0x07, 0x0B, 0x0C, 0x0E, 0x0F

POOL_KEY = (NATIVE, MEME, 0, 200, PONS_HOOK)
POOL_ID = "0x" + keccak(encode(["(address,address,uint24,int24,address)"], [POOL_KEY])).hex()
assert POOL_ID == "0xeda3dbcd4b745a70d04a92293bb7dcb3d9de234bd59e2773574a259cfe6f3fb0", POOL_ID

_id = 0


def rpc(method, params):
    global _id
    _id += 1
    body = json.dumps({"jsonrpc": "2.0", "id": _id, "method": method, "params": params}).encode()
    req = urllib.request.Request(RPC, body, {"content-type": "application/json"})
    out = json.loads(urllib.request.urlopen(req, timeout=120).read())
    if "error" in out:
        raise RuntimeError(out["error"])
    return out["result"]


def sel(sig):
    return keccak(text=sig)[:4]


def call(to, data, frm=None, value=0, block="latest"):
    tx = {"to": to, "data": "0x" + data.hex()}
    if frm:
        tx["from"] = frm
    if value:
        tx["value"] = hex(value)
    return bytes.fromhex(rpc("eth_call", [tx, block])[2:])


def send(to, data, value=0):
    tx = {"from": USER, "to": to, "data": "0x" + data.hex(), "value": hex(value), "gas": hex(3_000_000)}
    h = rpc("eth_sendTransaction", [tx])
    for _ in range(200):
        rcpt = rpc("eth_getTransactionReceipt", [h])
        if rcpt is not None:
            return rcpt
        time.sleep(0.05)
    raise RuntimeError(f"transaction {h} not mined")


def erc20_balance(token, who):
    if token == NATIVE:
        return int(rpc("eth_getBalance", [who, "latest"]), 16)
    return int.from_bytes(call(token, sel("balanceOf(address)") + encode(["address"], [who])), "big")


def execute_calldata(commands: bytes, inputs: list[bytes], deadline: int) -> bytes:
    return sel("execute(bytes,bytes[],uint256)") + encode(["bytes", "bytes[]", "uint256"], [commands, inputs, deadline])


def v3_path(tokens, fees):
    out = bytes.fromhex(tokens[0][2:])
    for fee, token in zip(fees, tokens[1:]):
        out += fee.to_bytes(3, "big") + bytes.fromhex(token[2:])
    return out


def v4_swap_input(actions: bytes, params: list[bytes]) -> bytes:
    return encode(["bytes", "bytes[]"], [actions, params])


def single(zero_for_one, amount_in, min_out, min_hop_x36=0, hook=b"", six_fields=True):
    if six_fields:
        return encode(
            ["((address,address,uint24,int24,address),bool,uint128,uint128,uint256,bytes)"],
            [(POOL_KEY, zero_for_one, amount_in, min_out, min_hop_x36, hook)],
        )
    return encode(
        ["((address,address,uint24,int24,address),bool,uint128,uint128,bytes)"],
        [(POOL_KEY, zero_for_one, amount_in, min_out, hook)],
    )


def deadline():
    blk = rpc("eth_getBlockByNumber", ["latest", False])
    return int(blk["timestamp"], 16) + 600


def permit2_nonce(token):
    raw = call(PERMIT2, sel("allowance(address,address,address)") + encode(["address", "address", "address"], [USER, token, UR]))
    amount, expiration, nonce = int.from_bytes(raw[0:32], "big"), int.from_bytes(raw[32:64], "big"), int.from_bytes(raw[64:96], "big")
    return amount, expiration, nonce


def sign_permit_single(token, amount, expiration, nonce, sig_deadline) -> bytes:
    typed = {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "PermitSingle": [
                {"name": "details", "type": "PermitDetails"},
                {"name": "spender", "type": "address"},
                {"name": "sigDeadline", "type": "uint256"},
            ],
            "PermitDetails": [
                {"name": "token", "type": "address"},
                {"name": "amount", "type": "uint160"},
                {"name": "expiration", "type": "uint48"},
                {"name": "nonce", "type": "uint48"},
            ],
        },
        "primaryType": "PermitSingle",
        "domain": {"name": "Permit2", "chainId": 4663, "verifyingContract": PERMIT2},
        "message": {
            "details": {"token": token, "amount": str(amount), "expiration": str(expiration), "nonce": str(nonce)},
            "spender": UR,
            "sigDeadline": str(sig_deadline),
        },
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(typed, fh)
        path = fh.name
    out = subprocess.run(
        ["cast", "wallet", "sign", "--private-key", USER_KEY, "--data", "--from-file", path],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    return bytes.fromhex(out[2:])


def permit2_permit_input(token, amount, expiration, nonce, sig_deadline):
    sig = sign_permit_single(token, amount, expiration, nonce, sig_deadline)
    return encode(
        ["((address,uint160,uint48,uint48),address,uint256)", "bytes"],
        [((token, amount, expiration, nonce), UR, sig_deadline), sig],
    )


def approve(token, spender, amount):
    r = send(token, sel("approve(address,uint256)") + encode(["address", "uint256"], [spender, amount]))
    assert r["status"] == "0x1"


def snapshot(*pairs):
    return {(t, w): erc20_balance(t, w) for t, w in pairs}


def deltas(before, after):
    return {k: after[k] - before[k] for k in before}


def report(title, ok, detail=""):
    print(f"[{'OK ' if ok else 'FAIL'}] {title} {detail}")
    if not ok:
        sys.exit(1)


def revert_reason(fn):
    try:
        fn()
        return None
    except RuntimeError as exc:
        return str(exc)


def main():
    assert int(rpc("eth_chainId", []), 16) == 4663
    assert rpc("eth_getCode", [USER, "latest"]) in ("0x", "0x0"), "probe account must be a plain EOA"
    rpc("anvil_setBalance", [USER, hex(10_000 * 10**18)])
    rpc("anvil_impersonateAccount", [USER])
    print("fork block", int(rpc("eth_blockNumber", []), 16))

    # ---- T1: ETH -> USDG through V3 (WETH/USDG 1bp), fee on the input leg. ----
    amount_in = 10**18
    dl = deadline()

    def t1_data(min_hops=(), six_fields=True):
        cmds = bytes([WRAP_ETH, PAY_PORTION, V3_SWAP_EXACT_IN])
        swap = (
            encode(["address", "uint256", "uint256", "bytes", "bool", "uint256[]"], [MSG_SENDER, CONTRACT_BALANCE, 1, v3_path([WETH, USDG], [100]), False, list(min_hops)])
            if six_fields
            else encode(["address", "uint256", "uint256", "bytes", "bool"], [MSG_SENDER, CONTRACT_BALANCE, 1, v3_path([WETH, USDG], [100]), False])
        )
        return execute_calldata(cmds, [
            encode(["address", "uint256"], [ADDRESS_THIS, CONTRACT_BALANCE]),
            encode(["address", "address", "uint256"], [WETH, FEE_TO, FEE_BPS]),
            swap,
        ], dl)

    before = snapshot((WETH, FEE_TO), (USDG, USER), (WETH, UR), (USDG, UR), (NATIVE, UR))
    r = send(UR, t1_data(), value=amount_in)
    d = deltas(before, snapshot((WETH, FEE_TO), (USDG, USER), (WETH, UR), (USDG, UR), (NATIVE, UR)))
    report("T1 WRAP_ETH+PAY_PORTION+V3_SWAP_EXACT_IN(…,payerIsUser=false,uint256[] minHopPriceX36=[]) executes", r["status"] == "0x1", f"gas={int(r['gasUsed'],16)}")
    report("T1 fee recipient got exactly 75 bps of input in WETH", d[(WETH, FEE_TO)] == amount_in * FEE_BPS // 10_000, str(d[(WETH, FEE_TO)]))
    report("T1 user received USDG", d[(USDG, USER)] > 0, f"{d[(USDG, USER)]/1e6:.6f} USDG")
    report("T1 router holds nothing after", d[(WETH, UR)] == 0 and d[(USDG, UR)] == 0 and d[(NATIVE, UR)] == 0)
    swapped_in = amount_in - amount_in * FEE_BPS // 10_000
    v3_x36 = d[(USDG, USER)] * 10**36 // swapped_in
    ok_low = revert_reason(lambda: call(UR, t1_data(min_hops=(v3_x36 * 99 // 100,)), frm=USER, value=amount_in)) is None
    err_high = revert_reason(lambda: call(UR, t1_data(min_hops=(v3_x36 * 101 // 100,)), frm=USER, value=amount_in))
    ok_high = err_high is None
    print(f"      V3 minHopPriceX36 bracket (out/in*1e36={v3_x36}): 0.99x passes={ok_low}; 1.01x passes={ok_high} err={str(err_high)[:80]}")
    err_len = revert_reason(lambda: call(UR, t1_data(min_hops=(0, 0)), frm=USER, value=amount_in))
    print(f"      V3 minHopPriceX36 with 2 entries for 1 hop: {'accepted' if err_len is None else 'reverts ' + str(err_len)[:80]}")

    # ---- T2: USDG -> ETH through V3 with PERMIT2_PERMIT signature, fee on the output leg. ----
    usdg_in = 1_000 * 10**6
    approve(USDG, PERMIT2, usdg_in)  # exact-amount ERC-20 leg
    _, _, nonce = permit2_nonce(USDG)
    dl = deadline()
    cmds = bytes([PERMIT2_PERMIT, V3_SWAP_EXACT_IN, PAY_PORTION, UNWRAP_WETH])
    inputs = [
        permit2_permit_input(USDG, usdg_in, dl, nonce, dl),
        encode(["address", "uint256", "uint256", "bytes", "bool", "uint256[]"], [ADDRESS_THIS, usdg_in, 0, v3_path([USDG, WETH], [100]), True, []]),
        encode(["address", "address", "uint256"], [WETH, FEE_TO, FEE_BPS]),
        encode(["address", "uint256"], [MSG_SENDER, 1]),
    ]
    data = execute_calldata(cmds, inputs, dl)
    # Quote by eth_simulateV1 of the *same bytes*: the WETH Transfer/Withdrawal logs give gross output.
    sim = rpc("eth_simulateV1", [{"blockStateCalls": [{"calls": [{"from": USER, "to": UR, "data": "0x" + data.hex()}]}], "traceTransfers": True}, "latest"])
    res = sim[0]["calls"][0]
    report("T2 eth_simulateV1 of the exact calldata succeeds and returns logs", res["status"] == "0x1" and len(res["logs"]) > 0, f"logs={len(res['logs'])}")
    transfer_topic = "0x" + keccak(text="Transfer(address,address,uint256)").hex()
    gross = sum(int(l["data"], 16) for l in res["logs"] if l["address"].lower() == WETH and l["topics"][0] == transfer_topic and l["topics"][2][-40:] == UR[2:])
    before = snapshot((WETH, FEE_TO), (USDG, USER), (NATIVE, USER), (WETH, UR), (USDG, UR), (NATIVE, UR))
    r = send(UR, data)
    d = deltas(before, snapshot((WETH, FEE_TO), (USDG, USER), (NATIVE, USER), (WETH, UR), (USDG, UR), (NATIVE, UR)))
    report("T2 PERMIT2_PERMIT(sig)+V3(payerIsUser)+PAY_PORTION+UNWRAP_WETH executes", r["status"] == "0x1", f"gas={int(r['gasUsed'],16)}")
    report("T2 fee == floor(gross_out*75/10000) with gross_out from simulateV1 logs", d[(WETH, FEE_TO)] == gross * FEE_BPS // 10_000, f"gross={gross} fee={d[(WETH, FEE_TO)]}")
    report("T2 user spent exactly the permitted USDG", d[(USDG, USER)] == -usdg_in)
    report("T2 user received net ETH (gross-fee-gas)", d[(NATIVE, USER)] > 0 and d[(NATIVE, USER)] <= gross - gross * FEE_BPS // 10_000, str(d[(NATIVE, USER)]))
    amount, expiration, _ = permit2_nonce(USDG)
    report("T2 Permit2 allowance fully consumed (exact-amount, expiry=deadline)", amount == 0 and expiration == dl, f"remaining={amount} exp={expiration}")
    report("T2 router holds nothing after", d[(WETH, UR)] == 0 and d[(USDG, UR)] == 0 and d[(NATIVE, UR)] == 0)

    # ---- T3: native ETH -> Pons meme via V4_SWAP, 6-field struct, fee on input. ----
    eth_in = 10**16
    quote = call(V4_QUOTER, sel("quoteExactInputSingle(((address,address,uint24,int24,address),bool,uint128,bytes))") + encode(["((address,address,uint24,int24,address),bool,uint128,bytes)"], [(POOL_KEY, True, eth_in - eth_in * FEE_BPS // 10_000, b"")]))
    quoted_out = int.from_bytes(quote[:32], "big")
    dl = deadline()

    def t3_data(six_fields=True, min_hop=0, min_out=1):
        cmds = bytes([PAY_PORTION, V4_SWAP])
        actions = bytes([SETTLE, SWAP_EXACT_IN_SINGLE, TAKE])
        params = [
            encode(["address", "uint256", "bool"], [NATIVE, CONTRACT_BALANCE, False]),
            single(True, OPEN_DELTA, min_out, min_hop, six_fields=six_fields),
            encode(["address", "address", "uint256"], [MEME, MSG_SENDER, OPEN_DELTA]),
        ]
        return execute_calldata(cmds, [encode(["address", "address", "uint256"], [NATIVE, FEE_TO, FEE_BPS]), v4_swap_input(actions, params)], dl)

    err5 = revert_reason(lambda: call(UR, t3_data(six_fields=False), frm=USER, value=eth_in))
    print(f"      stock 5-field ExactInputSingleParams: {'reverts ' + err5[:80] if err5 else 'decodes by accident (hookData offset word read as minHopPriceX36); never rely on it'}")
    err_hop = revert_reason(lambda: call(UR, t3_data(min_hop=1 << 200), frm=USER, value=eth_in))
    report("T3 minHopPriceX36=2^200 reverts (field is enforced, not ignored)", err_hop is not None, (err_hop or "")[:160])
    before = snapshot((NATIVE, FEE_TO), (MEME, USER), (NATIVE, UR), (MEME, UR))
    r = send(UR, t3_data(), value=eth_in)
    d = deltas(before, snapshot((NATIVE, FEE_TO), (MEME, USER), (NATIVE, UR), (MEME, UR)))
    report("T3 PAY_PORTION(ETH)+V4_SWAP[SETTLE(CONTRACT_BALANCE),SWAP_EXACT_IN_SINGLE(OPEN_DELTA,minHop=0),TAKE(OPEN_DELTA)] executes", r["status"] == "0x1", f"gas={int(r['gasUsed'],16)}")
    report("T3 fee recipient got exactly 75 bps of ETH input", d[(NATIVE, FEE_TO)] == eth_in * FEE_BPS // 10_000, str(d[(NATIVE, FEE_TO)]))
    report("T3 user meme out == V4Quoter quote on the post-fee input (quoter includes Pons hook+creator fee)", d[(MEME, USER)] == quoted_out, f"actual={d[(MEME, USER)]} quoted={quoted_out}")
    report("T3 router holds nothing after", d[(NATIVE, UR)] == 0 and d[(MEME, UR)] == 0)
    # minHopPriceX36 semantics: bracket the realised price out/in scaled by 1e36.
    realised_x36 = d[(MEME, USER)] * 10**36 // (eth_in - eth_in * FEE_BPS // 10_000)
    ok_low = revert_reason(lambda: call(UR, t3_data(min_hop=realised_x36 * 99 // 100), frm=USER, value=eth_in)) is None
    ok_high = revert_reason(lambda: call(UR, t3_data(min_hop=realised_x36 * 101 // 100), frm=USER, value=eth_in)) is None
    print(f"      minHopPriceX36 bracket: realised out/in*1e36={realised_x36}; 0.99x passes={ok_low}; 1.01x passes={ok_high}")
    ok_inv_low = revert_reason(lambda: call(UR, t3_data(min_hop=(10**72 // realised_x36) * 99 // 100), frm=USER, value=eth_in)) is None
    ok_inv_high = revert_reason(lambda: call(UR, t3_data(min_hop=(10**72 // realised_x36) * 101 // 100), frm=USER, value=eth_in)) is None
    print(f"      inverse (in/out*1e36) bracket: 0.99x passes={ok_inv_low}; 1.01x passes={ok_inv_high}")

    # ---- T4: Pons meme -> native ETH via V4 with Permit2 signature, fee on output, SWEEP min-out. ----
    meme_in = d[(MEME, USER)] // 2
    approve(MEME, PERMIT2, meme_in)
    _, _, nonce = permit2_nonce(MEME)
    dl = deadline()
    quote = call(V4_QUOTER, sel("quoteExactInputSingle(((address,address,uint24,int24,address),bool,uint128,bytes))") + encode(["((address,address,uint24,int24,address),bool,uint128,bytes)"], [(POOL_KEY, False, meme_in, b"")]))
    quoted_out = int.from_bytes(quote[:32], "big")
    fee_expected = quoted_out * FEE_BPS // 10_000
    net_expected = quoted_out - fee_expected

    def t4_data(sweep_min):
        cmds = bytes([PERMIT2_PERMIT, V4_SWAP, PAY_PORTION, SWEEP])
        actions = bytes([SWAP_EXACT_IN_SINGLE, SETTLE_ALL, TAKE])
        params = [
            single(False, meme_in, 1, 0),
            encode(["address", "uint256"], [MEME, meme_in]),
            encode(["address", "address", "uint256"], [NATIVE, ADDRESS_THIS, OPEN_DELTA]),
        ]
        return execute_calldata(cmds, [
            permit2_permit_input(MEME, meme_in, dl, nonce, dl),
            v4_swap_input(actions, params),
            encode(["address", "address", "uint256"], [NATIVE, FEE_TO, FEE_BPS]),
            encode(["address", "address", "uint256"], [NATIVE, MSG_SENDER, sweep_min]),
        ], dl)

    err_min = revert_reason(lambda: call(UR, t4_data(net_expected + 1), frm=USER))
    report("T4 SWEEP amountMin one wei above net output reverts (min-out floor holds)", err_min is not None, (err_min or "")[:120])
    before = snapshot((NATIVE, FEE_TO), (MEME, USER), (NATIVE, USER), (NATIVE, UR), (MEME, UR))
    r = send(UR, t4_data(net_expected))
    d = deltas(before, snapshot((NATIVE, FEE_TO), (MEME, USER), (NATIVE, USER), (NATIVE, UR), (MEME, UR)))
    report("T4 PERMIT2_PERMIT+V4_SWAP[SWAP,SETTLE_ALL(payerIsUser),TAKE->router]+PAY_PORTION(ETH)+SWEEP(ETH,minOut) executes", r["status"] == "0x1", f"gas={int(r['gasUsed'],16)}")
    report("T4 fee == floor(quoted_out*75/10000)", d[(NATIVE, FEE_TO)] == fee_expected, f"fee={d[(NATIVE, FEE_TO)]} quoted_out={quoted_out}")
    report("T4 user spent exactly meme_in", d[(MEME, USER)] == -meme_in)
    report("T4 router holds nothing after", d[(NATIVE, UR)] == 0 and d[(MEME, UR)] == 0)

    # ---- T5: V2 leg. USDG -> WETH via Uniswap V2 pair through UR, chained with UNWRAP. ----
    usdg_in = 50 * 10**6
    approve(USDG, PERMIT2, usdg_in)
    _, _, nonce = permit2_nonce(USDG)
    dl = deadline()
    cmds = bytes([PERMIT2_PERMIT, V2_SWAP_EXACT_IN, PAY_PORTION, UNWRAP_WETH])
    inputs = [
        permit2_permit_input(USDG, usdg_in, dl, nonce, dl),
        encode(["address", "uint256", "uint256", "address[]", "bool", "uint256[]"], [ADDRESS_THIS, usdg_in, 1, [USDG, WETH], True, []]),
        encode(["address", "address", "uint256"], [WETH, FEE_TO, FEE_BPS]),
        encode(["address", "uint256"], [MSG_SENDER, 1]),
    ]
    before = snapshot((WETH, FEE_TO), (USDG, USER), (WETH, UR), (USDG, UR))
    r = send(UR, execute_calldata(cmds, inputs, dl))
    d = deltas(before, snapshot((WETH, FEE_TO), (USDG, USER), (WETH, UR), (USDG, UR)))
    report("T5 V2_SWAP_EXACT_IN(payerIsUser) on factory 0x8bce… pair executes", r["status"] == "0x1", f"gas={int(r['gasUsed'],16)} fee={d[(WETH, FEE_TO)]}")
    report("T5 router holds nothing after", d[(WETH, UR)] == 0 and d[(USDG, UR)] == 0)

    # ---- T6: multi-hop V4 SWAP_EXACT_IN with uint256[] minHopPriceX36 (ETH -> ITH, one PathKey). ----
    dl = deadline()
    path_key = (MEME, 0, 200, PONS_HOOK, b"")
    exact_in = encode(
        ["(address,(address,uint24,int24,address,bytes)[],uint256[],uint128,uint128)"],
        [(NATIVE, [path_key], [0], OPEN_DELTA, 1)],
    )
    actions = bytes([SETTLE, SWAP_EXACT_IN, TAKE])
    params = [encode(["address", "uint256", "bool"], [NATIVE, CONTRACT_BALANCE, False]), exact_in, encode(["address", "address", "uint256"], [MEME, MSG_SENDER, OPEN_DELTA])]
    data = execute_calldata(bytes([PAY_PORTION, V4_SWAP]), [encode(["address", "address", "uint256"], [NATIVE, FEE_TO, FEE_BPS]), v4_swap_input(actions, params)], dl)
    err = revert_reason(lambda: call(UR, data, frm=USER, value=eth_in))
    report("T6 SWAP_EXACT_IN (currencyIn, PathKey[], uint256[] minHopPriceX36, amountIn, minOut) eth_call succeeds", err is None, err or "")
    exact_in_stock = encode(["(address,(address,uint24,int24,address,bytes)[],uint128,uint128)"], [(NATIVE, [path_key], OPEN_DELTA, 1)])
    params[1] = exact_in_stock
    data = execute_calldata(bytes([PAY_PORTION, V4_SWAP]), [encode(["address", "address", "uint256"], [NATIVE, FEE_TO, FEE_BPS]), v4_swap_input(actions, params)], dl)
    err = revert_reason(lambda: call(UR, data, frm=USER, value=eth_in))
    report("T6 stock SWAP_EXACT_IN struct (no minHopPriceX36[]) reverts", err is not None, (err or "")[:120])

    # ---- T7: Pons launch policy words. ----
    raw = call(PONS_HOOK, bytes.fromhex("ad091230") + bytes.fromhex(POOL_ID[2:]))
    words = [int.from_bytes(raw[i:i + 32], "big") for i in range(0, len(raw), 32)]
    report("T7 launches(poolId) word0!=0 (registered), word7=creatorTaxBps, word10=hookFeeBps", words[0] != 0 and len(words) == 13, f"creator={words[7]} hook={words[10]}")
    print("all probes passed")


if __name__ == "__main__":
    main()
