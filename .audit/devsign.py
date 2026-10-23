import json
import subprocess
import sys
import tempfile

from eth_keys import keys

from rhpools.lp_gate_siwe import personal_sign_hash

method, params, private_key = sys.argv[1], json.loads(sys.argv[2]), sys.argv[3]
if method == "personal_sign":
    signature = keys.PrivateKey(bytes.fromhex(private_key[2:])).sign_msg_hash(personal_sign_hash(bytes.fromhex(params[0][2:]).decode()))
    print("0x" + signature.r.to_bytes(32, "big").hex() + signature.s.to_bytes(32, "big").hex() + bytes([signature.v + 27]).hex())
else:
    with tempfile.NamedTemporaryFile("w", suffix=".json") as handle:
        handle.write(params[1])
        handle.flush()
        print(subprocess.run(["/home/andnasnd/.foundry/bin/cast", "wallet", "sign", "--private-key", private_key, "--data", "--from-file", handle.name], check=True, capture_output=True, text=True).stdout.strip())
