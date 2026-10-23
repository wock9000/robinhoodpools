"""`rhpools-gate`: the owner's host CLI over the same Gate."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .lp_gate import Gate, GatePolicy, GateRefusal


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="rhpools-gate", description="rhpools holder gate: owner policy, audit, keys")
    ap.add_argument("--db", default=os.environ.get("RHP_GATE_DB", str(Path(os.environ.get("RHP_DATA_DIR", Path.home() / ".local/share/rhpools")) / "gate.sqlite")))
    ap.add_argument("--owner", default=os.environ.get("RHP_GATE_OWNER"))
    ap.add_argument("--rpc-url", default=os.environ.get("RHP_GATE_RPC_URL", "http://127.0.0.1:8547"))
    sub = ap.add_subparsers(dest="command", required=True)

    policy = sub.add_parser("policy").add_subparsers(dest="action", required=True)
    policy.add_parser("show")
    policy.add_parser("typed-data").add_argument("file", type=Path)
    apply = policy.add_parser("apply")
    apply.add_argument("file", type=Path)
    apply.add_argument("--signature", required=True)

    sub.add_parser("audit").add_argument("--limit", type=int, default=20)

    keys = sub.add_parser("keys").add_subparsers(dest="action", required=True)
    keys.add_parser("list").add_argument("--wallet", required=True)
    keys.add_parser("revoke").add_argument("--key-id", required=True)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    gate = Gate(args.db, owner=args.owner, rpc_url=args.rpc_url, hosts=frozenset())
    try:
        if args.command == "policy" and args.action == "show":
            print(json.dumps(gate.status(), indent=2))
        elif args.command == "policy" and args.action == "typed-data":
            print(json.dumps(GatePolicy.parse(json.loads(args.file.read_text())).typed_data(gate.chain_id), indent=2))
        elif args.command == "policy":
            applied = gate.apply_policy(GatePolicy.parse(json.loads(args.file.read_text())), args.signature, via="cli")
            print(json.dumps(applied.public(), indent=2))
        elif args.command == "audit":
            for row in gate.audit(args.limit):
                print(json.dumps(row))
        elif args.action == "list":
            for record in gate.keys(args.wallet):
                print(json.dumps(record.public()))
        else:
            print("revoked" if gate.revoke(args.key_id, via="cli") else "no live key with that id")
    except GateRefusal as refusal:
        print(json.dumps(refusal.payload), file=sys.stderr)
        return 2
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        gate.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
