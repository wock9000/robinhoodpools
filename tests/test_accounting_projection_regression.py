"""Deferred replay never presents superseded inventory or hides prior owners."""
from __future__ import annotations

import time

import pytest

import rhpools.lp_market_accounting as accounting
from rhpools.lp_market_accounting import AccountBook
from rhpools.lp_market_protocols import (
    PANCAKE_V3_POSITION_MANAGER, UNISWAP_V3_POSITION_MANAGER, nft_position_key,
)
from rhpools.lp_market_store import MarketStore
from rhpools.lp_math import sqrt_ratio_at_tick
from rhpools.workbench_market import UNISWAP_V3_FACTORY, USDG

OWNER = "0x" + "11" * 20
OTHER = "0x" + "12" * 20
TOKEN = "0x" + "13" * 20
POOL = "0x" + "23" * 20
ZERO = "0x" + "00" * 20
TOKEN_ID = 1_178_253


def header(number: int) -> dict[str, str]:
    return {
        "number": hex(number), "hash": "0x" + f"{number:064x}",
        "parentHash": "0x" + f"{number - 1:064x}",
        "timestamp": hex(1_700_000_000 + number * 10),
    }


def pool() -> dict[str, object]:
    return {
        "id": POOL, "address": POOL, "protocol": "v3", "fee_ppm": 3_000,
        "token0": TOKEN, "token1": USDG, "symbol0": "ASSET", "symbol1": "USDG",
        "decimals0": 6, "decimals1": 6, "tick_spacing": 60, "hook": None,
        "factory": UNISWAP_V3_FACTORY, "created_block": 1, "source": "factory-live",
    }


def state(liquidity: int, owed: int = 0) -> dict[str, object]:
    return {
        "liquidity": str(liquidity), "tokens_owed0": str(owed),
        "tokens_owed1": str(owed), "claims_empty": owed == 0,
    }


def _base(block: dict[str, str], tx_index: int, log_index: int) -> dict[str, object]:
    number = int(block["number"], 16)
    return {
        "block_number": number, "block_hash": block["hash"],
        "tx_hash": "0x" + f"{number * 16 + tx_index:064x}",
        "tx_index": tx_index, "log_index": log_index,
        "timestamp": int(block["timestamp"], 16),
        "fee_ppm": 3_000, "sqrt_price_x96": str(1 << 96), "tick": 0,
        "liquidity": "1000000", "fee_amount0": None, "fee_amount1": None,
        "cashflow0": None, "cashflow1": None,
    }


def position_event(
    block: dict[str, str], kind: str, manager: str, *, tx_index: int = 0,
    log_index: int, delta: int, before: dict, after: dict, owner: str = OWNER,
) -> dict[str, object]:
    amounts = (abs(delta) * 1_000, abs(delta) * 1_000)
    cash = (-amounts[0], -amounts[1]) if kind == "add" else amounts if kind == "collect" else (0, 0)
    row = _base(block, tx_index, log_index)
    row.update(
        pool_id=POOL, protocol="v3", kind=kind, owner=owner, custody=manager,
        position_key=nft_position_key(manager, TOKEN_ID), token_id=str(TOKEN_ID),
        tick_lower=-60, tick_upper=60, liquidity_delta=str(delta),
        amount0=str(amounts[0]), amount1=str(amounts[1]),
        cashflow0=str(cash[0]), cashflow1=str(cash[1]),
        accounting_basis="pool_event", identity_basis="verified_nft_token_core_correlation",
        data={"position_before": before, "position_after": after},
    )
    return row


def transfer_event(
    block: dict[str, str], manager: str, source: str, target: str, *,
    tx_index: int = 0, log_index: int,
) -> dict[str, object]:
    row = _base(block, tx_index, log_index)
    row.update(
        pool_id=POOL, protocol="nft", kind="transfer",
        owner=None if target == ZERO else target, custody=manager,
        position_key=nft_position_key(manager, TOKEN_ID), token_id=str(TOKEN_ID),
        tick_lower=-60, tick_upper=60, liquidity_delta=None,
        amount0=None, amount1=None,
        accounting_basis="ownership_only_no_cashflow",
        identity_basis="recognized_manager_transfer",
        data={
            "manager_protocol": "v3", "prior_owner": source, "new_owner": target,
            "mint": source == ZERO, "burn": target == ZERO,
        },
    )
    return row


def opened(block: dict[str, str], manager: str, *, tx_index: int = 0) -> list[dict[str, object]]:
    return [
        position_event(
            block, "add", manager, tx_index=tx_index, log_index=0, delta=1_000,
            before=state(0), after=state(1_000),
        ),
        transfer_event(block, manager, ZERO, OWNER, tx_index=tx_index, log_index=1),
    ]


def burned(block: dict[str, str], manager: str) -> list[dict[str, object]]:
    return [
        position_event(
            block, "remove", manager, log_index=0, delta=-1_000,
            before=state(1_000), after=state(0, 1_000_000),
        ),
        position_event(
            block, "collect", manager, log_index=1, delta=0,
            before=state(0, 1_000_000), after=state(0),
        ),
        transfer_event(block, manager, OWNER, ZERO, log_index=2),
    ]


def drain(book: AccountBook) -> None:
    for _ in range(64):
        if not book.project_pending():
            break
    assert book.store.read().execute(
        "SELECT COUNT(*) FROM lp_accounting_pending",
    ).fetchone()[0] == 0


def _deferred_book(path) -> tuple[MarketStore, AccountBook]:
    store = MarketStore(path / "market.sqlite")
    book = AccountBook(store, deferred=True).install()
    store.upsert_pools([pool()])
    return store, book


def _owner_rows(book: AccountBook) -> dict[str, dict]:
    return {
        str(row["position_key"]): row
        for row in book.owner(OWNER, {"window": "all"})["positions"]
    }


UNISWAP_KEY = nft_position_key(UNISWAP_V3_POSITION_MANAGER, TOKEN_ID)
PANCAKE_KEY = nft_position_key(PANCAKE_V3_POSITION_MANAGER, TOKEN_ID)


def test_queued_burn_is_stale_not_open_and_only_for_its_manager(tmp_path):
    store, book = _deferred_book(tmp_path)
    try:
        first = header(100)
        store.ingest(
            [first],
            opened(first, UNISWAP_V3_POSITION_MANAGER)
            + opened(first, PANCAKE_V3_POSITION_MANAGER, tx_index=1),
        )
        drain(book)
        before = _owner_rows(book)
        assert before[UNISWAP_KEY]["status"] == "open"
        assert before[UNISWAP_KEY]["liquidity"] == "1000"
        assert before[UNISWAP_KEY]["projection"]["state"] == "current"

        second = header(101)
        store.ingest([second], burned(second, UNISWAP_V3_POSITION_MANAGER))

        # Burn events are mapped and queued but not replayed yet.
        queued = _owner_rows(book)
        stale = queued[UNISWAP_KEY]
        assert stale["status"] == "stale"
        assert stale["projection"] == {
            "state": "stale", "published_block": 100, "queued_block": 101,
        }
        assert stale["liquidity"] is None
        assert stale["principal_usd"] is None
        assert stale["principal0"] is None
        assert stale["valuation_basis"] == "unprojected_events_pending"
        assert stale["coverage"]["qualified"] is False
        assert "projection_stale" in stale["coverage"]["reasons"]
        open_episode = next(
            episode for episode in stale["episodes"] if episode["closed_at"] is None
        )
        assert "projection_stale" in open_episode["coverage"]["reasons"]
        detail = book.owner(OWNER, {"window": "all"})
        assert detail["summary"]["open_positions"] == 1
        assert detail["summary"]["stale_positions"] == 1
        # Same token id on another manager is a different position.
        live = queued[PANCAKE_KEY]
        assert live["status"] == "open"
        assert live["liquidity"] == "1000"
        assert live["projection"]["state"] == "current"

        drain(book)
        replayed = _owner_rows(book)
        assert replayed[UNISWAP_KEY]["status"] == "historical"
        assert replayed[UNISWAP_KEY]["coverage"]["reasons"] == ["no_longer_owned"]
        assert replayed[UNISWAP_KEY]["liquidity"] is None
        assert all(
            episode["closed_at"] is not None
            for episode in replayed[UNISWAP_KEY]["episodes"]
        )
        assert replayed[PANCAKE_KEY]["status"] == "open"
        detail = book.owner(OWNER, {"window": "all"})
        assert detail["summary"]["open_positions"] == 1
        assert detail["summary"]["stale_positions"] == 0
    finally:
        store.close()


def _consumer_views(book: AccountBook) -> dict[str, object]:
    stats = book.pool_stats([POOL])[POOL]
    owners = next(
        row for row in book.owners({"window": "all", "limit": 10})["rows"]
        if row.get("owner") == OWNER
    )
    open_at = accounting._OWNER_ACTIVITY_FIELDS.index("open_positions")
    rollup = book._owner_activity_snapshot(int(time.time()))
    rollup_open = sum(
        int(values[open_at]) for kind, identity, protocol, _tier, values in rollup["rows"]
        if kind == "owner" and identity == OWNER and protocol == ""
    )
    return {
        "principal": stats["observed_principal_usd"],
        "pool_open": stats["open_positions"],
        "pool_stale": stats["stale_positions"],
        "owners_open": owners["open_positions"],
        "owners_positions": owners["positions"],
        "rollup_open": rollup_open,
    }


def test_pool_owner_and_rollup_aggregates_exclude_superseded_positions(tmp_path):
    store, book = _deferred_book(tmp_path)
    try:
        with store.transaction() as conn:
            conn.execute(
                "CREATE TABLE lp_pool_state("
                "pool_id TEXT PRIMARY KEY,block_number INTEGER,tx_index INTEGER,"
                "log_index INTEGER,timestamp INTEGER,tick INTEGER,sqrt_price_x96 TEXT,"
                "price0_usd REAL,price1_usd REAL)"
            )
            conn.execute(
                "INSERT INTO lp_pool_state VALUES(?,?,?,?,?,?,?,?,?)",
                (POOL, 100, 0, 0, 1_700_001_000, 0, str(sqrt_ratio_at_tick(0)), 1.0, 1.0),
            )
        first = header(100)
        store.ingest(
            [first],
            opened(first, UNISWAP_V3_POSITION_MANAGER)
            + opened(first, PANCAKE_V3_POSITION_MANAGER, tx_index=1),
        )
        drain(book)
        both = _consumer_views(book)
        assert both["pool_open"] == 2 and both["pool_stale"] == 0
        assert both["owners_open"] == both["rollup_open"] == 2
        assert both["principal"] is not None and both["principal"] > 0

        second = header(101)
        store.ingest([second], burned(second, UNISWAP_V3_POSITION_MANAGER))
        queued = _consumer_views(book)
        assert queued["pool_open"] == 1 and queued["pool_stale"] == 1
        assert queued["principal"] == pytest.approx(both["principal"] / 2)
        assert queued["owners_positions"] == 2
        assert queued["owners_open"] == queued["rollup_open"] == 1

        drain(book)
        replayed = _consumer_views(book)
        assert replayed["pool_open"] == 1 and replayed["pool_stale"] == 0
        assert replayed["principal"] == pytest.approx(queued["principal"])
        assert replayed["owners_open"] == replayed["rollup_open"] == 1
    finally:
        store.close()


def test_burn_reorg_reopens_position_without_serving_superseded_state(tmp_path):
    store, book = _deferred_book(tmp_path)
    try:
        first = header(100)
        store.ingest([first], opened(first, UNISWAP_V3_POSITION_MANAGER))
        drain(book)
        second = header(101)
        store.ingest([second], burned(second, UNISWAP_V3_POSITION_MANAGER))
        assert _owner_rows(book)[UNISWAP_KEY]["status"] == "stale"

        store.rollback(100)
        # Rollback drops the published rows; nothing is presented until replay.
        assert UNISWAP_KEY not in _owner_rows(book)
        drain(book)
        reopened = _owner_rows(book)[UNISWAP_KEY]
        assert reopened["status"] == "open"
        assert reopened["owner"] == OWNER
        assert reopened["liquidity"] == "1000"
        assert reopened["projection"]["state"] == "current"
    finally:
        store.close()


def test_backfilled_history_marks_pending_without_discarding_tail_state(tmp_path):
    store, book = _deferred_book(tmp_path)
    try:
        later = header(101)
        store.ingest(
            [later],
            [position_event(
                later, "add", UNISWAP_V3_POSITION_MANAGER, log_index=0, delta=500,
                before=state(500), after=state(1_000),
            )],
        )
        drain(book)
        earlier = header(90)
        store.ingest([earlier], opened(earlier, UNISWAP_V3_POSITION_MANAGER), lane="history")
        row = _owner_rows(book)[UNISWAP_KEY]
        assert row["status"] == "open"
        assert row["liquidity"] == "1000"
        assert row["projection"] == {
            "state": "pending", "published_block": 101, "queued_block": 90,
        }
        assert row["coverage"]["qualified"] is False
        assert "projection_pending" in row["coverage"]["reasons"]
        drain(book)
        row = _owner_rows(book)[UNISWAP_KEY]
        assert row["projection"]["state"] == "current"
        assert row["coverage"]["history"] == "full"
    finally:
        store.close()


def _pending_identities(book: AccountBook, identities):
    with book.store.reader_snapshot() as conn:
        _order, _as_of, owners, custodies, complete = (
            book._scoped_owner_financial_state(
                conn, {"window": "all"}, None, identities,
            )
        )
    assert complete
    return owners, custodies


def test_owner_correction_keeps_prior_ledger_owner_pending_on_selected_page(tmp_path):
    store, book = _deferred_book(tmp_path)
    try:
        block = header(100)
        accounted = position_event(
            block, "add", UNISWAP_V3_POSITION_MANAGER, log_index=0, delta=1_000,
            before=state(0), after=state(1_000), owner=OWNER,
        )
        store.ingest([block], [accounted])
        drain(book)
        assert book.owner(OWNER, {"window": "all"})["summary"]["open_positions"] == 1

        corrected = dict(accounted)
        corrected["owner"] = OTHER
        store.enrich([corrected])

        selected = _pending_identities(
            book, [("owner", OWNER), ("owner", OTHER), ("custody", UNISWAP_V3_POSITION_MANAGER)],
        )
        unpaged = _pending_identities(book, None)
        assert selected == unpaged
        assert selected[0] == {OWNER, OTHER}
        assert selected[1] == {UNISWAP_V3_POSITION_MANAGER}

        drain(book)
        assert _pending_identities(book, [("owner", OWNER)]) == (set(), set())
        assert book.owner(OTHER, {"window": "all"})["summary"]["open_positions"] == 1
        assert book.owner(OWNER, {"window": "all"})["summary"]["open_positions"] == 0
    finally:
        store.close()
