#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_txs.py

Liest alle new_txid und change_address aus change_inputs,
holt für jede new_txid per HTTP die Transaktions-JSON,
und speichert das Ergebnis zusammen mit der change_address
in einer JSON- oder CSV-Datei.

Verwendung:
  python fetch_txs.py --db MEMPOOL.DB --out-file out.json [--format json|csv]

Optionen:
  --db         Pfad zur SQLite-DB (z.B. mempool.db)
  --out-file   Ausgabedatei (z.B. transactions.json oder .csv)
  --format     Ausgabeformat; default ist json

Features:
  • Fehlerbehandlung für HTTP 404 (TX nicht gefunden)
  • 300ms Pause zwischen Anfragen
  • Erfolgsmeldung je erfolgreicher API-Abfrage
  • Progressive Ausgabe alle 100 erfolgreichen Abfragen
"""
import argparse
import sqlite3
import sys
import requests
import json
import csv
import time

API_URL = "http://141.55.225.221:3000/tx/{}"
PAUSE_SEC = 0.2  # 300 ms Pause zwischen Anfragen
PROGRESS_INTERVAL = 100  # Anzahl erfolgreicher Abfragen bis Zwischenspeicherung


def load_inputs(n = 10_000):
    """Liefert Liste von (tx_hash, change_output_idx)."""
    import pandas as pd

    print("loading csv...")
    df = pd.read_csv("change-ground-truth.csv")
    print("csv loaded.")

    sample = df.sample(n=n, replace=False, random_state=42, ignore_index=True)
    tuples = list(
        sample[["txhash", "change_output_idx"]].itertuples(index=False, name=None)
    )

    return tuples

def fetch_tx_json(txid):
    """Holt das JSON von der API; gibt None bei 404 zurück, wirft bei anderen Fehlern."""
    try:
        resp = requests.get(API_URL.format(txid), timeout=10)
    except requests.RequestException as e:
        raise RuntimeError(f"Netzwerkfehler für TXID {txid}: {e}")

    if resp.status_code == 404:
        return None
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"HTTP-Fehler für TXID {txid}: {e}")

    return resp.json()


def save_as_json(records, out_file):
    """Speichert die Liste von dicts als JSON."""
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def save_as_csv(records, out_file):
    """
    Speichert records als CSV.
    Achtung: nur flache Felder werden direkt exportiert.
    Komplexe Felder (vin, vout) werden als JSON-String in der Zelle abgelegt.
    Überschreibt die Datei komplett.
    """
    if not records:
        print("Keine Datensätze zum Speichern.")
        return
    fieldnames = list(records[0].keys())
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            flat = {}
            for k, v in rec.items():
                if isinstance(v, (list, dict)):
                    flat[k] = json.dumps(v, ensure_ascii=False)
                else:
                    flat[k] = v
            writer.writerow(flat)


def main():
    parser = argparse.ArgumentParser(
        description="Fetch transactions + change_address und speichere als JSON/CSV"
    )
    parser.add_argument(
        "--out-file",
        required=True,
        help="Ausgabedatei (z.B. transactions.json oder .csv)",
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Ausgabeformat; default ist json",
    )
    args = parser.parse_args()

    pairs = load_inputs()

    results = []
    success_count = 0

    for tx_hash, change_vout_index in pairs:
        try:
            tx = fetch_tx_json(tx_hash)

        except Exception as e:
            print(e, file=sys.stderr)
            time.sleep(PAUSE_SEC)
            continue
        if tx is None:
            print(f"TXID {tx_hash} nicht gefunden (404).", file=sys.stderr)
        else:
            success_count += 1
            print(
                f"[{success_count}] TXID {tx_hash} erfolgreich abgefragt (Change-Index: {change_vout_index})."
            )
            tx["change_address"] = change_vout_index
            tx["change_vout_index"] = change_vout_index
            results.append(tx)

            # Zwischenspeicherung nach jedem PROGRESS_INTERVAL erfolgreichen Abfragen
            if success_count % PROGRESS_INTERVAL == 0:
                print(
                    f"Zwischenspeicherung: {success_count} Transaktionen in {args.out_file} geschrieben."
                )
                if args.format == "json":
                    save_as_json(results, args.out_file)
                else:
                    save_as_csv(results, args.out_file)

        time.sleep(PAUSE_SEC)

    # Endgültige Ausgabe
    if args.format == "json":
        save_as_json(results, args.out_file)
    else:
        save_as_csv(results, args.out_file)

    print(f"Fertig: {success_count} Transaktionen in {args.out_file} geschrieben.")


if __name__ == "__main__":
    main()
