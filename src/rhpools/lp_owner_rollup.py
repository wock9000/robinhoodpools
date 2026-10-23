"""Copy committed rollup inputs without retaining a live-database read snapshot."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import shutil
import sqlite3
import time
from typing import Any


_TABLES = (
    ('lp_accounting_episodes', (
        ('id', 'TEXT PRIMARY KEY'), ('last_timestamp', 'INTEGER'),
        ('owner', 'TEXT'), ('custody', 'TEXT'), ('protocol', 'TEXT'),
        ('pool_id', 'TEXT'), ('position_key', 'TEXT'), ('closed_at', 'INTEGER'),
        ('status', 'TEXT'), ('gross_pnl_usd', 'REAL'), ('gas_usd', 'REAL'),
        ('history_complete', 'INTEGER'), ('fees_complete', 'INTEGER'),
        ('pricing_complete', 'INTEGER'), ('fees_usd', 'REAL'),
        ('deposit_usd', 'REAL'), ('proceeds_usd', 'REAL'),
    )),
    ('lp_accounting_effects', (
        ('event_id', 'INTEGER PRIMARY KEY'), ('episode_id', 'TEXT'), ('tx_hash', 'TEXT'),
    )),
    ('lp_accounting_tx_costs', (
        ('tx_hash', 'TEXT PRIMARY KEY'), ('owner', 'TEXT'), ('gas_usd', 'REAL'),
    )),
)
_VALUE_COLUMNS = tuple(f"value_{index}" for index in range(max(len(fields) for _, fields in _TABLES)))
_PAGE_ROWS = 512
DEFAULT_RESERVE_BYTES = 4 * 1024**3


class ReplicaSpaceError(sqlite3.OperationalError):
    pass


def install_journal(connection: sqlite3.Connection) -> None:
    connection.execute('PRAGMA recursive_triggers=ON')
    connection.execute(
        'CREATE TABLE IF NOT EXISTS lp_owner_rollup_journal('
        'sequence INTEGER PRIMARY KEY AUTOINCREMENT,table_id INTEGER NOT NULL,'
        'row_key NOT NULL,deleted INTEGER NOT NULL,' + ','.join(_VALUE_COLUMNS) + ')'
    )
    for table_id, (table, fields) in enumerate(_TABLES):
        columns = tuple(name for name, _ in fields)
        key = columns[0]
        image = ','.join('NEW.' + name for name in columns)
        image_columns = ','.join(_VALUE_COLUMNS[:len(columns)])
        changed = ' OR '.join(f'OLD.{name} IS NOT NEW.{name}' for name in columns)
        connection.execute(
            f'CREATE TRIGGER IF NOT EXISTS {table}_rollup_insert AFTER INSERT ON {table} '
            f'BEGIN INSERT INTO lp_owner_rollup_journal(table_id,row_key,deleted,{image_columns}) '
            f'VALUES({table_id},NEW.{key},0,{image}); END'
        )
        connection.execute(
            f'CREATE TRIGGER IF NOT EXISTS {table}_rollup_update AFTER UPDATE OF '
            + ','.join(columns) + f' ON {table} WHEN {changed} BEGIN '
            'INSERT INTO lp_owner_rollup_journal(table_id,row_key,deleted) '
            f'SELECT {table_id},OLD.{key},1 WHERE OLD.{key} IS NOT NEW.{key}; '
            f'INSERT INTO lp_owner_rollup_journal(table_id,row_key,deleted,{image_columns}) '
            f'VALUES({table_id},NEW.{key},0,{image}); END'
        )
        connection.execute(
            f'CREATE TRIGGER IF NOT EXISTS {table}_rollup_delete AFTER DELETE ON {table} '
            'BEGIN INSERT INTO lp_owner_rollup_journal(table_id,row_key,deleted) '
            f'VALUES({table_id},OLD.{key},1); END'
        )


def prune_journal(connection: sqlite3.Connection, limit: int = 4096) -> int:
    acknowledged = connection.execute(
        "SELECT value FROM lp_accounting_meta WHERE key='owner_rollup_ack'"
    ).fetchone()
    if acknowledged is None:
        return 0
    return connection.execute(
        'DELETE FROM lp_owner_rollup_journal WHERE sequence IN('
        'SELECT sequence FROM lp_owner_rollup_journal WHERE sequence<=? '
        'ORDER BY sequence LIMIT ?)', (int(acknowledged[0]), limit),
    ).rowcount


def _sequence(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT seq FROM sqlite_sequence WHERE name='lp_owner_rollup_journal'"
    ).fetchone()
    return int(row[0]) if row is not None else 0


class RollupReplica:
    def __init__(
        self, source_directory: Path, stop: Any = None,
        *, reserve_bytes: int = DEFAULT_RESERVE_BYTES,
    ):
        self._directory = source_directory
        self._reserve_bytes = reserve_bytes
        self._stop = stop
        self._next_space_check = 0.0
        self._check_space()
        self.connection = sqlite3.connect('', isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute('PRAGMA journal_mode=MEMORY')
        self.connection.execute('PRAGMA synchronous=OFF')
        self.connection.execute('PRAGMA cache_size=-65536')
        for table, fields in _TABLES:
            self.connection.execute(
                f'CREATE TABLE {table}(' + ','.join(f'{name} {kind}' for name, kind in fields) + ')'
            )
        self._inserts = tuple(
            f'INSERT OR REPLACE INTO {table} VALUES(' + ','.join('?' for _ in fields) + ')'
            for table, fields in _TABLES
        )
        self._bounds: list[int] | None = None
        self._table = 0
        self._cursor = -(2**63)
        self.through = 0
        self.epoch = 0
        self.built_at = 0
        self._indexed = False
        self.financial: list[Any] = []

    def close(self) -> None:
        self.connection.close()

    def _check_space(self) -> None:
        now = time.monotonic()
        if now >= self._next_space_check:
            if shutil.disk_usage(self._directory).free < self._reserve_bytes:
                raise ReplicaSpaceError('rollup replica reached the free-space reserve')
            self._next_space_check = now + 0.25
        if self._stop is not None and self._stop.is_set():
            raise sqlite3.OperationalError('interrupted')

    @contextmanager
    def reader_snapshot(self, seconds: float):
        deadline = time.monotonic() + seconds
        space_error = None
        def progress():
            nonlocal space_error
            try:
                if time.monotonic() >= deadline:
                    return 1
                self._check_space()
            except ReplicaSpaceError as error:
                space_error = error
                return 1
            except sqlite3.OperationalError:
                return 1
            return 0
        self.connection.set_progress_handler(progress, 1000)
        try:
            yield self.connection
        finally:
            self.connection.set_progress_handler(None, 0)
            if space_error is not None:
                raise space_error

    def synchronize(self, book: Any) -> None:
        reader = book.store
        if self._bounds is None:
            with reader.reader_snapshot() as source:
                self.through = _sequence(source)
                self._bounds = [
                    int(source.execute(f'SELECT COALESCE(MAX(rowid),0) FROM {table}').fetchone()[0])
                    for table, _ in _TABLES
                ]
        while self._table < len(_TABLES):
            self._check_space()
            table, fields = _TABLES[self._table]
            with reader.reader_snapshot() as source:
                rows = source.execute(
                    'SELECT rowid,' + ','.join(name for name, _ in fields)
                    + f' FROM {table} WHERE rowid>=? AND rowid<=? ORDER BY rowid LIMIT ?',
                    (self._cursor, self._bounds[self._table], _PAGE_ROWS),
                ).fetchall()
            if not rows:
                self._table += 1
                self._cursor = -(2**63)
                continue
            self.connection.execute('BEGIN')
            try:
                self.connection.executemany(self._inserts[self._table], (tuple(row)[1:] for row in rows))
                self.connection.commit()
            except BaseException:
                self.connection.rollback()
                raise
            self._cursor = int(rows[-1][0]) + 1
            if self._cursor > self._bounds[self._table]:
                self._table += 1
                self._cursor = -(2**63)
        if not self._indexed:
            episode_fields = _TABLES[0][1]
            covering_fields = episode_fields[1:9] + episode_fields[:1] + episode_fields[9:]
            with self.reader_snapshot(1800) as connection:
                connection.execute(
                    'CREATE INDEX IF NOT EXISTS lp_accounting_episodes_owner_window_cover ON '
                    'lp_accounting_episodes(' + ','.join(name for name, _ in covering_fields) + ')'
                )
                connection.execute(
                    'CREATE INDEX IF NOT EXISTS lp_accounting_effects_episode '
                    'ON lp_accounting_effects(episode_id,tx_hash)'
                )
                connection.execute(
                    'CREATE INDEX IF NOT EXISTS lp_accounting_tx_costs_owner_gas_cover '
                    'ON lp_accounting_tx_costs(owner,tx_hash,gas_usd)'
                )
            self._indexed = True
        with reader.reader_snapshot() as source:
            target = _sequence(source)
            epoch = book._store_metadata_int(source, 'epoch')
            order, as_of, _, _, complete = book._scoped_owner_financial_state(source, {}, None, ())
            financial = [order, as_of, complete]
            built_at = int(time.time())
        if target < self.through:
            raise RuntimeError('rollup journal moved behind its replica')
        while self.through < target:
            self._check_space()
            with reader.reader_snapshot() as source:
                rows = source.execute(
                    'SELECT sequence,table_id,row_key,deleted,' + ','.join(_VALUE_COLUMNS)
                    + ' FROM lp_owner_rollup_journal '
                    'WHERE sequence>? AND sequence<=? ORDER BY sequence LIMIT ?',
                    (self.through, target, _PAGE_ROWS),
                ).fetchall()
            if not rows:
                raise RuntimeError('rollup journal was pruned before its replica consumed it')
            self.connection.execute('BEGIN')
            try:
                for row in rows:
                    _, table_id, key, deleted = row[:4]
                    table, fields = _TABLES[table_id]
                    if deleted:
                        self.connection.execute(f'DELETE FROM {table} WHERE {fields[0][0]}=?', (key,))
                    else:
                        self.connection.execute(self._inserts[table_id], row[4:4 + len(fields)])
                self.connection.commit()
            except BaseException:
                self.connection.rollback()
                raise
            self.through = int(rows[-1][0])
        self.epoch = epoch
        self.financial = financial
        self.built_at = built_at
