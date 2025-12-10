#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CLI-Tool für Abfrage der RBF-Replacements und zugehörigen Adressen.

Verwendung:
  # Zeilenweise Ausgabe der letzten Replacement-Events (Standard 10):
  python query_replacements_cli.py --list [LIMIT] [--latest]

  # Für eine gegebene Change-Adresse die zugehörigen Input-Adressen ausgeben:
  python query_replacements_cli.py --change-address <ADDRESS> [--latest]

  # Für eine gegebene Input-Adresse die zugehörigen Change-Adressen ausgeben:
  python query_replacements_cli.py --input-address <ADDRESS> [--latest]

  # Statistik über Change-Adressen und Input-Zuordnungen:
  python query_replacements_cli.py --stats

Optional:
  --db FILE         Pfad zur SQLite-DB (Standard: mempool.db)
  --latest          Nur die zeitlich letzte Ersetzung anzeigen
"""
import argparse
import sqlite3
import sys

def list_replacements(conn, limit):
    c = conn.cursor()
    c.execute(
        'SELECT detected_at, orig_txid, new_txid, change_address, diff '
        'FROM replacements '
        'ORDER BY detected_at DESC LIMIT ?', (limit,)
    )
    rows = c.fetchall()
    if not rows:
        print("Keine Replacement-Einträge gefunden.")
        return
    for ts, orig, new, addr, diff in rows:
        print(f"{ts}: {orig} → {new}, Change-Adresse {addr} (Diff {diff})")

def show_inputs_for_change(conn, change_addr, latest=False):
    c = conn.cursor()
    if latest:
        # Nur letzte Ersetzung als Event holen
        c.execute(
            'SELECT orig_txid, new_txid FROM replacements '
            'WHERE change_address = ? ORDER BY detected_at DESC LIMIT 1',
            (change_addr,)
        )
        row = c.fetchone()
        if not row:
            print(f"Keine Einträge für Change-Adresse {change_addr} gefunden.")
            return
        orig, new = row
        # Eingangsadressen dieser letzten Ersetzung
        c.execute(
            'SELECT DISTINCT input_address FROM change_inputs '
            'WHERE orig_txid = ? AND new_txid = ? AND change_address = ?',
            (orig, new, change_addr)
        )
    else:
        # Alle zugeordneten Eingangsadressen, ohne Duplikate
        c.execute(
            'SELECT DISTINCT input_address FROM change_inputs '
            'WHERE change_address = ?',
            (change_addr,)
        )
    rows = c.fetchall()
    if not rows:
        print(f"Keine Eingangsadressen für Change-Adresse {change_addr} gefunden.")
        return
    print(f"Input-Adressen für Change-Adresse {change_addr}:")
    for (inp,) in rows:
        print(f"  {inp}")

def show_changes_for_input(conn, input_addr, latest=False):
    c = conn.cursor()
    if latest:
        # Nur letzte Ersetzung als Event holen
        c.execute(
            'SELECT change_address, orig_txid, new_txid FROM change_inputs '
            'WHERE input_address = ? ORDER BY detected_at DESC LIMIT 1',
            (input_addr,)
        )
        row = c.fetchone()
        if not row:
            print(f"Keine Einträge für Input-Adresse {input_addr} gefunden.")
            return
        change_addr, orig, new = row
        # Diese Change-Adresse ausgeben
        rows = [(change_addr,)]
    else:
        # Alle Change-Adressen zu dieser Input-Adresse, ohne Duplikate
        c.execute(
            'SELECT DISTINCT change_address FROM change_inputs '
            'WHERE input_address = ?',
            (input_addr,)
        )
        rows = c.fetchall()
    if not rows:
        print(f"Keine Change-Adressen für Input-Adresse {input_addr} gefunden.")
        return
    print(f"Change-Adressen für Input-Adresse {input_addr}:")
    for (addr,) in rows:
        print(f"  {addr}")

def show_stats(conn):
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM replacements')
    total = c.fetchone()[0]
    c.execute('SELECT COUNT(DISTINCT change_address) FROM replacements')
    unique = c.fetchone()[0]
    c.execute(
        'SELECT COUNT(*) FROM (SELECT change_address FROM replacements '
        'GROUP BY change_address HAVING COUNT(*) > 1)'
    )
    multi = c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM change_inputs')
    ci_total = c.fetchone()[0]
    c.execute('SELECT COUNT(DISTINCT input_address) FROM change_inputs')
    ci_unique = c.fetchone()[0]

    print("Statistik:")
    print(f"  Gesamt-Replacement-Einträge: {total}")
    print(f"  Eindeutige Change-Adressen: {unique}")
    print(f"  Change-Adressen mit mehreren Events: {multi}")
    print(f"  Gesamt-Zuordnungen Change→Input: {ci_total}")
    print(f"  Eindeutige Input-Adressen in Zuordnungen: {ci_unique}")

def main():
    parser = argparse.ArgumentParser(description="RBF Replacement CLI Query Tool")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--list', nargs='?', metavar='LIMIT', const=10, type=int,
        help='Liste der Replacement-Events anzeigen, optional mit LIMIT'
    )
    group.add_argument(
        '--change-address', metavar='ADDRESS',
        help='Input-Adressen für Change-Address abrufen'
    )
    group.add_argument(
        '--input-address', metavar='ADDRESS',
        help='Change-Adressen für Input-Adresse abrufen'
    )
    group.add_argument(
        '--stats', action='store_true',
        help='Statistik über Replacements und Zuordnungen anzeigen'
    )
    parser.add_argument(
        '--db', default='mempool2.db', help='Pfad zur SQLite-DB-Datei'
    )
    parser.add_argument(
        '--latest', action='store_true', help='Nur die zeitlich letzte Ersetzung anzeigen'
    )
    args = parser.parse_args()

    try:
        conn = sqlite3.connect(args.db)
    except sqlite3.Error as e:
        print(f"Fehler beim Öffnen der DB {args.db}: {e}")
        sys.exit(1)

    if args.list is not None:
        limit = 1 if args.latest else args.list
        list_replacements(conn, limit)
    elif args.change_address:
        show_inputs_for_change(conn, args.change_address, latest=args.latest)
    elif args.input_address:
        show_changes_for_input(conn, args.input_address, latest=args.latest)
    elif args.stats:
        show_stats(conn)

    conn.close()

if __name__ == '__main__':
    main()
