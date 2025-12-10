#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mempool RBF Tracker

- Polls the Bitcoin Mempool API for a full snapshot of txids and stores tx details in SQLite.
- Detects opt-in RBF txs (sequence < 0xfffffffe).
- Detects when such txs are replaced by new txs reusing inputs.
- Identifies change address by max diff shrink.
- Stores permanent mapping of change to input addresses in 'change_inputs'.
- Keeps 7d history for tx/replacements; change_inputs remain forever.
- Supports in-memory and file-based DB.
"""
import argparse
import logging
import queue
import sqlite3
import threading
import time
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter, Retry

# --- Global setup ------------------------------------------------------------

# HTTP session with retries
session = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=0.3,
    status_forcelist=[429, 500, 502, 503, 504],
)
session.mount("http://", HTTPAdapter(max_retries=retries))

# Counters for stats
counters = {
    "total_requests": 0,
    "success": 0,
    "notfound": 0,
    "http_errors": 0,
    "network_errors": 0,
    "processed": 0,
}

# --- Configuration ----------------------------------------------------------

API_BASE = "http://141.55.225.221:3000"
POLL_INTERVAL = 5  # seconds
HISTORY_DAYS = 7  # retention for tx/replacements
RBF_SEQ_THRESHOLD = 0xFFFFFFFE

# --- Logging ----------------------------------------------------------------

# Default to INFO; change to DEBUG to see per-request logs
logging.basicConfig(
    level=logging.INFO,  # statt INFO
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- Database ----------------------------------------------------------------


def init_db(db_path):
    conn = sqlite3.connect(db_path, check_same_thread=False)
    c = conn.cursor()
    # 1) Setze WAL-Journal statt Default-Journal (DELETE)
    c.execute("PRAGMA journal_mode = WAL;")
    # 2) Etwas weniger strenges fsync-Verhalten (meist ausreichend)
    c.execute("PRAGMA synchronous = NORMAL;")
    # Transactions
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS tx (
            txid TEXT PRIMARY KEY,
            fetched_at TEXT,
            fee INTEGER,
            vsize INTEGER
        )
    """
    )
    # Inputs
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS tx_inputs (
            txid TEXT,
            prev_txid TEXT,
            prev_vout INTEGER,
            address TEXT,
            value INTEGER,
            sequence INTEGER
        )
    """
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_inputs_prev ON tx_inputs(prev_txid, prev_vout)"
    )
    # Outputs
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS tx_outputs (
            txid TEXT,
            vout_index INTEGER,
            address TEXT,
            value INTEGER
        )
    """
    )
    # RBF candidates
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS rbf_txs (
            txid TEXT PRIMARY KEY,
            added_at TEXT
        )
    """
    )
    # Replacements
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS replacements (
            orig_txid TEXT,
            new_txid TEXT,
            change_address TEXT,
            change_vout_index INTEGER,
            old_value INTEGER,
            new_value INTEGER,
            diff INTEGER,
            detected_at TEXT,
            PRIMARY KEY(orig_txid, new_txid, change_address, change_vout_index)
        )
    """
    )
    # Change→Input mapping
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS change_inputs (
            orig_txid TEXT,
            new_txid TEXT,
            change_address TEXT,
            change_vout_index INTEGER,
            input_address TEXT,
            detected_at TEXT,
            PRIMARY KEY(orig_txid, new_txid, change_address, input_address, change_vout_index)
        )
    """
    )

    # add change_vout_index migration
    # def ensure_column(table, column, coldef):
    #     c.execute(f"PRAGMA table_info({table})")
    #     cols = {row[1] for row in c.fetchall()}
    #     if column not in cols:
    #         logging.info("Adding column %s.%s ...", table, column)
    #         c.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coldef}")
    #         logging.info("Added column %s.%s", table, column)
    #     else:
    #         logging.info("Column %s.%s already present", table, column)
    #
    # ensure_column("replacements", "change_vout_index", "INTEGER")
    # ensure_column("change_inputs", "change_vout_index", "INTEGER")

    conn.commit()
    logging.info("Datenbank initialisiert: %s", db_path)
    return conn


def purge_old(conn):
    cutoff = datetime.utcnow() - timedelta(days=HISTORY_DAYS)
    ts = cutoff.isoformat()
    c = conn.cursor()
    c.execute("DELETE FROM tx WHERE fetched_at < ?", (ts,))
    c.execute("DELETE FROM tx_inputs WHERE txid NOT IN (SELECT txid FROM tx)")
    c.execute("DELETE FROM tx_outputs WHERE txid NOT IN (SELECT txid FROM tx)")
    c.execute("DELETE FROM rbf_txs WHERE added_at < ?", (ts,))
    c.execute("DELETE FROM replacements WHERE detected_at < ?", (ts,))
    conn.commit()
    logging.info("Alte Datensätze älter als %d Tage gelöscht", HISTORY_DAYS)


# --- API Fetching -----------------------------------------------------------


def fetch_mempool_full():
    """
    Returns the list of all txids currently in the mempool.
    """
    resp = session.get(f"{API_BASE}/mempool/txids", timeout=5)
    resp.raise_for_status()
    return resp.json()


def fetch_tx(txid):
    """
    Fetch full transaction JSON for a given txid, with logging & counters.
    """
    start = time.time()
    counters["total_requests"] += 1
    try:
        resp = session.get(f"{API_BASE}/tx/{txid}", timeout=5)
        resp.raise_for_status()
    except requests.HTTPError:
        status = getattr(resp, "status_code", None)
        if status == 404:
            counters["notfound"] += 1
            # logging.debug("fetch_tx %s → 404 Not Found", txid)
            return None
        counters["http_errors"] += 1
        # logging.warning("fetch_tx %s → HTTP %s", txid, status, exc_info=True)
        return None
    except requests.RequestException as e:
        counters["network_errors"] += 1
        # logging.warning("fetch_tx %s → Network‐Fehler: %s", txid, e, exc_info=True)
        return None

    elapsed = time.time() - start
    counters["success"] += 1
    # logging.debug("fetch_tx %s → %d bytes in %.3fs", txid, len(resp.content), elapsed)
    return resp.json()


# --- RBF Detection ----------------------------------------------------------


def record_replacement(orig_txid, new_txid, conn):
    """
    Erfasst ein Replacement NUR dann, wenn:
      - beide TXs exakt dieselben vout_index-Keys besitzen,
      - an allen Indizes dieselbe Adresse steht,
      - sich genau EIN Output-Wert geändert hat,
      - und diese Änderung eine Schrumpfung (old > new) ist.
    Speichert zusätzlich den change_vout_index.
    """
    c = conn.cursor()

    # Outputs der Original-TX laden
    c.execute(
        "SELECT vout_index, address, value FROM tx_outputs WHERE txid=?", (orig_txid,)
    )
    orig_rows = c.fetchall()
    orig_by_idx = {idx: (addr, val) for (idx, addr, val) in orig_rows}

    # Outputs der neuen TX laden
    c.execute(
        "SELECT vout_index, address, value FROM tx_outputs WHERE txid=?", (new_txid,)
    )
    new_rows = c.fetchall()
    new_by_idx = {idx: (addr, val) for (idx, addr, val) in new_rows}

    # Gleiche Index-Menge erforderlich
    if set(orig_by_idx.keys()) != set(new_by_idx.keys()):
        return

    diffs = []
    # Adresse muss an jedem Index identisch sein; nur Werte vergleichen
    for idx in orig_by_idx.keys():
        orig_addr, orig_val = orig_by_idx[idx]
        new_addr, new_val = new_by_idx[idx]

        # Falls Adressen an einem Index abweichen -> raus
        if orig_addr != new_addr:
            return

        if orig_val != new_val:
            diffs.append((idx, orig_addr, orig_val, new_val))

    # Genau EINE Änderung zulassen, und diese muss schrumpfen
    if len(diffs) != 1:
        return

    change_idx, change_addr, old_val, new_val = diffs[0]
    if new_val >= old_val:
        # Keine Schrumpfung -> nicht als Change-Reduktion interpretieren
        return

    diff = old_val - new_val
    ts = datetime.utcnow().isoformat()

    # Replacement eintragen (inkl. change_vout_index)
    c.execute(
        "INSERT OR IGNORE INTO replacements "
        "(orig_txid, new_txid, change_address, change_vout_index, old_value, new_value, diff, detected_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (orig_txid, new_txid, change_addr, change_idx, old_val, new_val, diff, ts),
    )

    # Inputs der Original-TX holen und Mapping change<-inputs speichern (inkl. Index)
    c.execute("SELECT DISTINCT address FROM tx_inputs WHERE txid=?", (orig_txid,))
    inputs = [row[0] for row in c.fetchall()]

    for inp in inputs:
        c.execute(
            "INSERT OR IGNORE INTO change_inputs "
            "(orig_txid, new_txid, change_address, change_vout_index, input_address, detected_at) "
            "VALUES (?,?,?,?,?,?)",
            (orig_txid, new_txid, change_addr, change_idx, inp, ts),
        )

    logging.info(
        "Replacement (strict one-output shrink): %s → %s; change %s[#%d]; %d → %d (−%d); inputs=%d",
        orig_txid,
        new_txid,
        change_addr,
        change_idx,
        old_val,
        new_val,
        diff,
        len(inputs),
    )


def process_added_nocommit(txid, conn):
    """
    Like process_added but without committing at the end.
    Inserts tx, inputs, outputs and handles RBF detection, but requires an external commit.
    """
    tx = fetch_tx(txid)
    if tx is None:
        return

    now = datetime.utcnow().isoformat()
    fee = tx.get("fee", 0)
    vsize = tx.get("vsize", 0)
    c = conn.cursor()

    # Insert transaction record
    c.execute(
        "INSERT OR IGNORE INTO tx (txid, fetched_at, fee, vsize) VALUES (?,?,?,?)",
        (txid, now, fee, vsize),
    )

    # Insert inputs
    is_rbf = False
    for vin in tx.get("vin", []):
        seq = vin.get("sequence", 0)
        prev = vin.get("prevout", {})
        c.execute(
            "INSERT INTO tx_inputs (txid, prev_txid, prev_vout, address, value, sequence) "
            "VALUES (?,?,?,?,?,?)",
            (
                txid,
                vin["txid"],
                vin["vout"],
                prev.get("scriptpubkey_address"),
                prev.get("value"),
                seq,
            ),
        )
        if seq < RBF_SEQ_THRESHOLD:
            is_rbf = True

    # Insert outputs
    for idx, vout in enumerate(tx.get("vout", [])):
        c.execute(
            "INSERT INTO tx_outputs (txid, vout_index, address, value) "
            "VALUES (?,?,?,?)",
            (txid, idx, vout.get("scriptpubkey_address"), vout.get("value")),
        )

    # If this tx is opt-in RBF, record it and look for replacements
    if is_rbf:
        c.execute(
            "INSERT OR IGNORE INTO rbf_txs (txid, added_at) VALUES (?,?)", (txid, now)
        )
        for vin in tx.get("vin", []):
            prev_txid = vin["txid"]
            prev_vout = vin["vout"]
            c2 = conn.cursor()
            c2.execute(
                "SELECT ti.txid "
                "FROM tx_inputs ti "
                "JOIN rbf_txs r ON ti.txid = r.txid "
                "WHERE ti.prev_txid = ? AND ti.prev_vout = ?",
                (prev_txid, prev_vout),
            )
            for (orig_txid,) in c2.fetchall():
                if orig_txid != txid:
                    record_replacement(orig_txid, txid, conn)

    # Note: no conn.commit() here — commit must be done externally


# --- Threading --------------------------------------------------------------


def purge_scheduler(event_q):
    while True:
        time.sleep(3600)
        event_q.put(("purge", None))


class Poller(threading.Thread):
    def __init__(self, event_q):
        super().__init__(daemon=True)
        self.event_q = event_q
        self.last = set()

    def run(self):
        # # 1) Initial-Snapshot, ohne Events zu senden / Initial-Snapshot überspringen – erzeugt keine Events
        # initial = fetch_mempool_full()
        # self.last = set(initial)
        # logging.info("Initial snapshot: %d txids geladen", len(self.last))

        # 1) Initial-Snapshot: erstelle Events für alle IDs, damit historische RBF-Paare erkannt werden
        initial = fetch_mempool_full()
        for txid in initial:
            self.event_q.put(("added", txid))
        # Jetzt ist self.last gesetzt
        self.last = set(initial)

        # 2) Ab hier echte Poll-Schleife / Normales Polling
        while True:
            try:
                txids = fetch_mempool_full()  # ordered list
                current = set(txids)
                new_txids = [tx for tx in txids if tx not in self.last]
                # logging.info("Poll: %d total, %d new", len(txids), len(new_txids))
                for txid in new_txids:
                    self.event_q.put(("added", txid))
                self.last = current
            except Exception as e:
                logging.error("Polling-Fehler: %s", e, exc_info=True)
            time.sleep(POLL_INTERVAL)


class BatchWorker(threading.Thread):
    def __init__(self, event_q, conn, batch_size=100):
        super().__init__(daemon=True)
        self.event_q = event_q
        self.conn = conn
        self.batch_size = batch_size
        self.buffer = []

    def run(self):
        try:
            while True:
                evt, txid = self.event_q.get()
                if evt == "added":
                    self.buffer.append(txid)
                    if len(self.buffer) >= self.batch_size:
                        self._flush_batch()
                    self.event_q.task_done()

                elif evt == "purge":
                    purge_old(self.conn)
                    self.event_q.task_done()

                elif evt == "stop":
                    # Markiere das Stop-Event als erledigt und verlasse die Schleife
                    self.event_q.task_done()
                    break

                else:
                    # Unbekanntes Event, trotzdem task_done aufrufen
                    self.event_q.task_done()
        finally:
            # 1) Rest-Puffer flushen
            if self.buffer:
                logging.info("Flush %d verbleibende txids vor Exit", len(self.buffer))
                self._flush_batch()
            # 2) Commit & Close der DB
            self.conn.commit()
            self.conn.close()

    def _flush_batch(self):
        # BEGIN; löschen – sqlite3 startet automatisch eine Transaktion
        for txid in self.buffer:
            counters["processed"] += 1
            process_added_nocommit(txid, self.conn)
        # Ein Commit reicht, um alle Änderungen zu schreiben
        self.conn.commit()
        logging.debug("Batch-Commit: %d txids", len(self.buffer))
        self.buffer.clear()


# class Worker(threading.Thread):
#     def __init__(self, event_q, conn):
#         super().__init__(daemon=True)
#         self.event_q = event_q
#         self.conn = conn
#
#     def run(self):
#         while True:
#             evt, txid = self.event_q.get()
#             # logging.debug("Worker: Event %r für %s", evt, txid)
#             try:
#                 if evt == 'added':
#                     counters['processed'] += 1
#                     process_added(txid, self.conn)
#                 else:
#                     purge_old(self.conn)
#             except Exception as e:
#                 logging.error("Verarbeitungs-Fehler %s: %s", txid, e, exc_info=True)
#             finally:
#                 self.event_q.task_done()

# --- Main -------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Mempool RBF Tracker")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--in-memory", action="store_true", help="Use in-memory SQLite (default)"
    )
    group.add_argument("--db-file", type=str, help="Path to SQLite DB file")
    args = parser.parse_args()

    db_path = args.db_file or ":memory:"
    conn = init_db(db_path)

    event_q = queue.Queue()
    Poller(event_q).start()
    # Worker(event_q, conn).start() # <- statt BatchWorker, falls kein Batch-Betrieb
    bw = BatchWorker(event_q, conn, batch_size=200)
    bw.start()
    threading.Thread(target=purge_scheduler, args=(event_q,), daemon=True).start()
    logging.info("Starte Polling mit Intervall %.1fs", POLL_INTERVAL)

    def stats_scheduler():
        while True:
            time.sleep(60)
            logging.info(
                "API-Statistiken: total=%d, success=%d, 404=%d, http_err=%d, net_err=%d, processed=%d",
                counters["total_requests"],
                counters["success"],
                counters["notfound"],
                counters["http_errors"],
                counters["network_errors"],
                counters["processed"],
            )

    threading.Thread(target=stats_scheduler, daemon=True).start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("Shutting down.")
        # Stop-Event schicken
        event_q.put(("stop", None))
        # Warten, bis der BatchWorker fertig geflushed und geschlossen hat
        bw.join()
        logging.info("Closed DB, bye!")


if __name__ == "__main__":
    main()
