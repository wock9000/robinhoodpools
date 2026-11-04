"""EIP-4361 message parsing and EIP-191 / ERC-1271 signature checks."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable
from urllib.parse import urlsplit

from eth_keys.datatypes import Signature
from eth_keys.exceptions import BadSignature
from eth_utils import keccak, to_checksum_address

ERC1271_MAGIC = "0x1626ba7e"
_ERC1271_SELECTOR = bytes.fromhex("1626ba7e")
_EIP7702_PREFIX = "0xef0100"
_ADDRESS_RE = re.compile(r"0x[0-9a-fA-F]{40}")
_HEX_RE = re.compile(r"0x[0-9a-fA-F]*")
_MESSAGE_RE = re.compile(
    r"\A(?P<domain>[^\n]+) wants you to sign in with your Ethereum account:\n"
    r"(?P<address>0x[0-9a-fA-F]{40})\n\n"
    r"(?:(?P<statement>[^\n]+)\n)?\n"
    r"URI: (?P<uri>[^\n]+)\n"
    r"Version: (?P<version>[^\n]+)\n"
    r"Chain ID: (?P<chain_id>[0-9]+)\n"
    r"Nonce: (?P<nonce>[A-Za-z0-9]{8,})\n"
    r"Issued At: (?P<issued_at>[^\n]+)"
    r"(?:\nExpiration Time: (?P<expiration_time>[^\n]+))?"
    r"(?:\nNot Before: (?P<not_before>[^\n]+))?"
    r"(?:\nRequest ID: (?P<request_id>[^\n]*))?"
    r"(?:\nResources:(?P<resources>(?:\n- [^\n]+)*))?\Z"
)


@dataclass(frozen=True)
class SiweMessage:
    domain: str
    address: str
    statement: str | None
    uri: str
    version: str
    chain_id: int
    nonce: str
    issued_at: float
    expiration_time: float | None
    not_before: float | None


def build_message(*, domain: str, address: str, statement: str, uri: str, chain_id: int,
                  nonce: str, issued_at: str, expiration_time: str) -> str:
    return (
        f"{domain} wants you to sign in with your Ethereum account:\n{address}\n\n"
        f"{statement}\n\nURI: {uri}\nVersion: 1\nChain ID: {chain_id}\nNonce: {nonce}\n"
        f"Issued At: {issued_at}\nExpiration Time: {expiration_time}"
    )


def _timestamp(text: str | None) -> float | None:
    if text is None:
        return None
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("SIWE timestamps need a timezone")
    return parsed.astimezone(timezone.utc).timestamp()


def parse_message(text: str) -> SiweMessage:
    if len(text) > 4096:
        raise ValueError("SIWE message too long")
    match = _MESSAGE_RE.match(text)
    if match is None:
        raise ValueError("Malformed SIWE message")
    fields = match.groupdict()
    if fields["version"] != "1":
        raise ValueError("Unsupported SIWE version")
    return SiweMessage(
        domain=fields["domain"], address=to_checksum_address(fields["address"]),
        statement=fields["statement"], uri=fields["uri"], version=fields["version"],
        chain_id=int(fields["chain_id"]), nonce=fields["nonce"],
        issued_at=_timestamp(fields["issued_at"]),
        expiration_time=_timestamp(fields["expiration_time"]),
        not_before=_timestamp(fields["not_before"]),
    )


def personal_sign_hash(message: str) -> bytes:
    body = message.encode()
    return keccak(b"\x19Ethereum Signed Message:\n" + str(len(body)).encode() + body)


def signature_bytes(signature: str) -> bytes:
    if not isinstance(signature, str) or not _HEX_RE.fullmatch(signature) or len(signature) != 132:
        raise ValueError("Signature must be 65 hex bytes")
    raw = bytes.fromhex(signature[2:])
    v = raw[64]
    if v >= 27:
        v -= 27
    if v not in (0, 1):
        raise ValueError("Signature recovery id out of range")
    return raw[:64] + bytes([v])


def recover_signer(msg_hash: bytes, signature: str) -> str:
    try:
        return Signature(signature_bytes=signature_bytes(signature)).recover_public_key_from_msg_hash(msg_hash).to_checksum_address()
    except BadSignature as exc:
        raise ValueError("Signature does not recover") from exc


def is_contract_code(code: str | None) -> bool:
    """True for deployed contract code; False for empty accounts and EIP-7702 designators."""
    code = (code or "0x").lower()
    return len(code) > 2 and not code.startswith(_EIP7702_PREFIX)


def erc1271_calldata(msg_hash: bytes, signature: str) -> str:
    raw = bytes.fromhex(signature[2:])
    return "0x" + (
        _ERC1271_SELECTOR + msg_hash + (64).to_bytes(32, "big") + len(raw).to_bytes(32, "big")
        + raw + b"\x00" * (-len(raw) % 32)
    ).hex()


def verify_signer(address: str, msg_hash: bytes, signature: str,
                  rpc: Callable[[str, list], object] | None) -> bool:
    """EOA (including EIP-7702 delegated) by ecrecover; real contracts by ERC-1271 through `rpc`."""
    if not _ADDRESS_RE.fullmatch(address):
        return False
    try:
        if recover_signer(msg_hash, signature).lower() == address.lower():
            return True
    except ValueError:
        return False
    if rpc is None:
        return False
    code = rpc("eth_getCode", [address, "latest"])
    if not is_contract_code(code if isinstance(code, str) else None):
        return False
    result = rpc("eth_call", [{"to": address, "data": erc1271_calldata(msg_hash, signature)}, "latest"])
    return isinstance(result, str) and result.lower().startswith(ERC1271_MAGIC)


def host_of(uri: str) -> str:
    return urlsplit(uri).netloc.lower()
