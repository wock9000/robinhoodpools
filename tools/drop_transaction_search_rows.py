"""Drop the per-transaction rows from the search catalog, offline.

Transactions are now answered from ``events`` (see MarketStore.search), so the
113M ``kind='transaction'`` search entities and their terms are dead weight.
Deleting them row by row would touch nearly every page of a ~280 GB index, so
this instead:

1. copies every non-transaction entity and term to a side database,
2. drops both search tables (their pages join the main file's freelist, which
   later inserts reuse before the file grows),
3. recreates the tables from their original DDL and reloads the kept rows in
   primary-key order, committing and checkpointing each chunk so the WAL never
   holds more than one chunk,
4. recreates the secondary indexes and checks the result.

Run only while robinhoodpools.service is stopped. Every step is resumable from
the side database. Set SQLITE_TMPDIR to a disk with room for the sorts.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time

CHILD, PARENT = "lp_search_terms", "lp_search_entities"
NON_TRANSACTION = "(kind<'transaction' OR kind>'transaction')"


def log(message: str) -> None:
    print(time.strftime("%H:%M:%S"), message, flush=True)


def meta(db: sqlite3.Connection, key: str) -> str | None:
    row = db.execute("SELECT value FROM side.meta WHERE key=?", (key,)).fetchone()
    return None if row is None else str(row[0])


def set_meta(db: sqlite3.Connection, key: str, value: object) -> None:
    db.execute(
        "INSERT INTO side.meta(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )


def main_tables(db: sqlite3.Connection) -> set[str]:
    return {
        str(row[0]) for row in db.execute(
            "SELECT name FROM main.sqlite_master WHERE type='table' AND name IN (?,?)",
            (CHILD, PARENT),
        )
    }


def checkpoint(db: sqlite3.Connection) -> None:
    busy, frames, done = db.execute("PRAGMA main.wal_checkpoint(TRUNCATE)").fetchone()
    if busy:
        raise RuntimeError("WAL checkpoint was blocked; is the service still running?")


def copy_kept_rows(db: sqlite3.Connection) -> None:
    db.execute("BEGIN")
    ddl = [
        str(row[0]) for row in db.execute(
            "SELECT sql FROM main.sqlite_master WHERE tbl_name IN (?,?) "
            "AND sql IS NOT NULL ORDER BY type='table' DESC, name",
            (CHILD, PARENT),
        )
    ]
    db.execute("DELETE FROM side.ddl")
    db.executemany("INSERT INTO side.ddl(position,sql) VALUES(?,?)", enumerate(ddl))
    db.execute("DELETE FROM side.entities")
    db.execute(
        f"INSERT INTO side.entities SELECT kind,id,label,subtitle,href,rank "
        f"FROM main.{PARENT} WHERE {NON_TRANSACTION} ORDER BY kind,id"
    )
    db.execute("DELETE FROM side.terms")
    db.execute(
        f"INSERT INTO side.terms SELECT term,kind,id,weight "
        f"FROM main.{CHILD} INDEXED BY lp_search_terms_entity_idx WHERE {NON_TRANSACTION}"
    )
    set_meta(db, "entities", db.execute("SELECT count(*) FROM side.entities").fetchone()[0])
    set_meta(db, "terms", db.execute("SELECT count(*) FROM side.terms").fetchone()[0])
    # Rows the live indexer writes after this snapshot are dropped with the
    # tables; rewinding its cursor here after the reload re-indexes them.
    cursor = db.execute(
        "SELECT value FROM main.metadata WHERE key='search_index_cursor'"
    ).fetchone()
    set_meta(db, "search_cursor", int(cursor[0]) if cursor else 0)
    set_meta(db, "copied", 1)
    db.execute("COMMIT")
    db.execute("CREATE INDEX IF NOT EXISTS side.terms_order ON terms(term,kind,id)")


def drop_main_tables(db: sqlite3.Connection) -> None:
    db.execute("BEGIN")
    db.execute(f"DROP TABLE IF EXISTS main.{CHILD}")
    db.execute(f"DROP TABLE IF EXISTS main.{PARENT}")
    set_meta(db, "dropped", 1)
    set_meta(db, "loaded_terms", 0)
    set_meta(db, "last_term_key", "")
    db.execute("COMMIT")
    checkpoint(db)


def reload(db: sqlite3.Connection, chunk: int) -> None:
    ddl = [str(row[0]) for row in db.execute("SELECT sql FROM side.ddl ORDER BY position")]
    tables = [sql for sql in ddl if sql.lstrip().upper().startswith("CREATE TABLE")]
    indexes = [sql for sql in ddl if sql not in tables]
    if meta(db, "tables_created") != "1":
        db.execute("BEGIN")
        for sql in tables:
            db.execute(sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1))
        db.execute(f"DELETE FROM main.{PARENT}")
        db.execute(
            f"INSERT INTO main.{PARENT}(kind,id,label,subtitle,href,rank) "
            "SELECT kind,id,label,subtitle,href,rank FROM side.entities ORDER BY kind,id"
        )
        set_meta(db, "tables_created", 1)
        db.execute("COMMIT")
        checkpoint(db)
    total = int(meta(db, "terms") or 0)
    loaded = int(meta(db, "loaded_terms") or 0)
    key = meta(db, "last_term_key") or ""
    while True:
        db.execute("BEGIN")
        if key:
            term, kind, entity = json.loads(key)
            rows = db.execute(
                "SELECT term,kind,id,weight FROM side.terms INDEXED BY terms_order "
                "WHERE (term,kind,id)>(?,?,?) ORDER BY term,kind,id LIMIT ?",
                (term, kind, entity, chunk),
            ).fetchall()
        else:
            rows = db.execute(
                "SELECT term,kind,id,weight FROM side.terms INDEXED BY terms_order "
                "ORDER BY term,kind,id LIMIT ?",
                (chunk,),
            ).fetchall()
        if not rows:
            db.execute("COMMIT")
            break
        db.executemany(f"INSERT OR IGNORE INTO main.{CHILD}(term,kind,id,weight) VALUES(?,?,?,?)", rows)
        loaded += len(rows)
        key = json.dumps(list(rows[-1][:3]))
        set_meta(db, "loaded_terms", loaded)
        set_meta(db, "last_term_key", key)
        db.execute("COMMIT")
        checkpoint(db)
        log(f"terms {loaded:,}/{total:,}")
    if meta(db, "indexes_created") != "1":
        db.execute("BEGIN")
        for sql in indexes:
            db.execute(sql.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS", 1))
        set_meta(db, "indexes_created", 1)
        db.execute("COMMIT")
        checkpoint(db)


def verify(db: sqlite3.Connection) -> None:
    entities = db.execute(f"SELECT count(*) FROM main.{PARENT}").fetchone()[0]
    terms = db.execute(f"SELECT count(*) FROM main.{CHILD}").fetchone()[0]
    transactions = db.execute(
        f"SELECT count(*) FROM main.{PARENT} WHERE kind='transaction'"
    ).fetchone()[0]
    expected = (int(meta(db, "entities") or -1), int(meta(db, "terms") or -1))
    if (entities, terms) != expected or transactions:
        raise RuntimeError(f"reload mismatch: {(entities, terms)} != {expected}, transactions={transactions}")
    for table in (PARENT, CHILD):
        result = [str(row[0]) for row in db.execute(f"PRAGMA main.integrity_check({table})")]
        if result != ["ok"]:
            raise RuntimeError(f"{table} integrity_check: {result[:5]}")
    free = db.execute("PRAGMA main.freelist_count").fetchone()[0]
    page = db.execute("PRAGMA main.page_size").fetchone()[0]
    log(f"verified {entities:,} entities, {terms:,} terms; freelist {free * page / 1e9:.1f} GB reusable")


def run() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("database")
    parser.add_argument("side", help="side SQLite file on another disk")
    parser.add_argument("--chunk", type=int, default=2_000_000)
    parser.add_argument(
        "--copy-only", action="store_true",
        help="only take the side copy; safe while the service is running",
    )
    args = parser.parse_args()
    db = sqlite3.connect(args.database, isolation_level=None, timeout=60)
    db.execute("PRAGMA foreign_keys=OFF")
    if not args.copy_only:
        db.execute("PRAGMA main.journal_mode=WAL")
        checkpoint(db)
    db.execute("ATTACH ? AS side", (args.side,))
    db.execute("PRAGMA side.journal_mode=OFF")
    db.execute("PRAGMA side.synchronous=OFF")
    db.executescript(
        "CREATE TABLE IF NOT EXISTS side.meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
        "CREATE TABLE IF NOT EXISTS side.ddl(position INTEGER PRIMARY KEY, sql TEXT NOT NULL);"
        "CREATE TABLE IF NOT EXISTS side.entities(kind TEXT, id TEXT, label TEXT, "
        "subtitle TEXT, href TEXT, rank INTEGER);"
        "CREATE TABLE IF NOT EXISTS side.terms(term TEXT COLLATE NOCASE, kind TEXT, "
        "id TEXT, weight INTEGER);"
    )
    if meta(db, "copied") != "1":
        if main_tables(db) != {CHILD, PARENT}:
            raise RuntimeError("search tables are missing and no complete side copy exists")
        log("copying non-transaction search rows to side database")
        copy_kept_rows(db)
    log(f"side copy: {meta(db, 'entities')} entities, {meta(db, 'terms')} terms")
    if args.copy_only:
        return
    if meta(db, "dropped") != "1":
        log("dropping search tables")
        drop_main_tables(db)
    log("reloading kept rows")
    reload(db, args.chunk)
    verify(db)
    db.execute(
        "UPDATE main.metadata SET value=? WHERE key='search_index_cursor' "
        "AND CAST(value AS INTEGER)>?",
        (meta(db, "search_cursor"), int(meta(db, "search_cursor") or 0)),
    )
    log(f"search cursor rewound to {meta(db, 'search_cursor')}")
    set_meta(db, "complete", 1)


if __name__ == "__main__":
    run()
