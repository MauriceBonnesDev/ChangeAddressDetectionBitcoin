#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extract_features.py

Liest die Datei transactions_with_change.json (Liste von Transaktions-JSONs mit Feld change_address)
Extrahiert definierte Merkmale, filtert und speichert als CSV.

Verwendung:
  python extract_features.py --in-file transactions_with_change.json --out-file features.csv [--one-per-change]

Optionen:
  --in-file          Eingabe-JSON-Datei mit Transaktionen
  --out-file         Ausgabe-CSV-Datei für Features
  --one-per-change   Nur einen Datensatz pro Wechselgeld-Adresse ausgeben
"""
import json
import argparse
import numpy as np
import pandas as pd


def extract_features(tx):
    """Extrahiert Merkmale aus einer einzelnen Transaktion."""
    vins = tx.get("vin", [])                 # Inputs
    vouts = tx.get("vout", [])               # Outputs

    status = tx.get("status", {})
    confirmed_block = status.get("block_height", 0)
    confirmed_time = status.get("block_time", 0)

    # 0. Locktime-Merkmale
    locktime = tx.get("locktime", 0)                                          # Raw Locktime-Wert
    is_timestamp_lock = int(locktime >= 500_000_000)                           # Type-Flag: Block- vs. Timestamp-Locktime
    blocks_until_unlock = locktime - confirmed_block if not is_timestamp_lock else 0   # Differenz in Blöcken
    seconds_until_unlock = locktime - confirmed_time if is_timestamp_lock else 0       # Differenz in Sekunden
    if is_timestamp_lock:
        already_unlocked = int(confirmed_time >= locktime)                      # Schon entsperrt (Timestamp)
    else:
        already_unlocked = int(confirmed_block >= locktime)                    # Schon entsperrt (Block)

    # 1. Input-Merkmale
    vals_in = [vin["prevout"]["value"] for vin in vins if vin.get("prevout")]
    n_inputs = len(vals_in)                        # Anzahl Inputs
    sum_inputs = sum(vals_in)                      # Summe Input-Werte (Satoshis)
    mean_inputs = sum_inputs / n_inputs if n_inputs else 0  # Durchschnittlicher Input-Wert
    var_inputs = np.var(vals_in) if vals_in else 0        # Varianz der Inputs

    # 2. Sequence & RBF
    seq_values = [vin.get("sequence", 0) for vin in vins]
    min_sequence = min(seq_values) if seq_values else 0    # Kleinste Sequence aller Inputs
    rbf_flag = int(any(s < 0xFFFFFFFF - 1 for s in seq_values))  # Replace-By-Fee-Flag

    # 3. Output-Merkmale
    vals_out = [v.get("value", 0) for v in vouts]
    n_outputs = len(vals_out)                               # Anzahl Outputs
    sum_outputs = sum(vals_out)                             # Summe Output-Werte
    sorted_out = sorted(vals_out, reverse=True)
    max_output = sorted_out[0] if sorted_out else 0         # Größter Output
    second_max_output = sorted_out[1] if len(sorted_out) > 1 else 0  # Zweitgrößter
    std_outputs = np.std(vals_out) if vals_out else 0       # Standardabweichung Outputs
    q25_outputs = np.quantile(vals_out, 0.25) if vals_out else 0  # 25%-Quantil
    q75_outputs = np.quantile(vals_out, 0.75) if vals_out else 0  # 75%-Quantil
    dust_output_count = sum(1 for v in vals_out if v < 1000)        # Dust-Count (<1000 sats)

    # 4. Gebühren & Größe
    fee = tx.get("fee", 0)            # Gebühr in Satoshis
    size = tx.get("size", 1)          # Größe in Bytes
    weight = tx.get("weight", 1)      # Gewichtseinheit
    fee_rate_size = fee / size          # Fee-Rate (Satoshis/Byte)
    fee_rate_weight = fee / weight      # Fee-Rate (Satoshis/Gewicht)

    # 5. Skript/Adressentyp
    p2wpkh_count = sum(1 for v in vouts if v.get("scriptpubkey_type") == "v0_p2wpkh")
    pct_outputs_p2wpkh = p2wpkh_count / n_outputs if n_outputs else 0  # Anteil P2WPKH

    # 6. SigOps & abgeleitete Merkmale
    sigops = tx.get("sigops", 0)      # Anzahl Signatur-Operationen
    sigops_per_input = sigops / n_inputs if n_inputs else 0        # SigOps pro Input
    sigops_per_output = sigops / n_outputs if n_outputs else 0     # SigOps pro Output
    sigops_density = sigops / weight if weight else 0             # SigOps-Dichte (pro Gewichtseinheit)
    relative_sig_complexity = sigops_per_input / pct_outputs_p2wpkh if pct_outputs_p2wpkh else 0  # Rel. Komplexität

    # Rückgabe aller Merkmale als Dictionary
    return {
        # Locktime
        "locktime": locktime,
        "is_timestamp_lock": is_timestamp_lock,
        "blocks_until_unlock": blocks_until_unlock,
        "seconds_until_unlock": seconds_until_unlock,
        "already_unlocked": already_unlocked,
        # Input
        "n_inputs": n_inputs,
        "sum_inputs": sum_inputs,
        "mean_inputs": mean_inputs,
        "var_inputs": var_inputs,
        # Sequence & RBF
        "min_sequence": min_sequence,
        "rbf_flag": rbf_flag,
        # Output
        "n_outputs": n_outputs,
        "sum_outputs": sum_outputs,
        "max_output": max_output,
        "second_max_output": second_max_output,
        "std_outputs": std_outputs,
        "q25_outputs": q25_outputs,
        "q75_outputs": q75_outputs,
        "dust_output_count": dust_output_count,
        # Gebühren & Raten
        "fee": fee,
        "fee_rate_size": fee_rate_size,
        "fee_rate_weight": fee_rate_weight,
        # Skript/Addr-Typ
        "pct_outputs_p2wpkh": pct_outputs_p2wpkh,
        # SigOps
        "sigops": sigops,
        "sigops_per_input": sigops_per_input,
        "sigops_per_output": sigops_per_output,
        "sigops_density": sigops_density,
        "relative_sig_complexity": relative_sig_complexity,
    }


def compute_change_position(tx):
    """Berechnet die Position der change_address im vout-Array (1, 2 oder 0)."""
    change_addr = tx.get("change_address")
    vouts = tx.get("vout", [])
    addresses = [v.get("scriptpubkey_address") for v in vouts]
    if addresses and addresses[0] == change_addr:
        return 1
    if addresses and addresses[-1] == change_addr:
        return 2
    if len(addresses) > 2 and change_addr in addresses:
        return 0
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-file", required=True)
    parser.add_argument("--out-file", required=True)
    parser.add_argument("--one-per-change", action="store_true")
    args = parser.parse_args()

    with open(args.in_file, "r", encoding="utf-8") as f:
        tx_list = json.load(f)

    rows = []
    for tx in tx_list:
        vouts = tx.get("vout", [])
        if len(vouts) <= 1:
            continue
        if any(v.get("scriptpubkey_type") == "op_return" for v in vouts):
            continue
        pos = compute_change_position(tx)
        addresses = [v.get("scriptpubkey_address") for v in vouts]
        if pos is None or addresses.count(tx.get("change_address")) != 1:
            continue

        feats = extract_features(tx)
        # txid und change_address hinzufügen
        feats["txid"] = tx.get("txid")
        feats["change_address"] = tx.get("change_address")
        feats["change_position"] = pos
        # Bestimme Typ der Change-Adresse
        change_type = None
        for v in vouts:
            if v.get("scriptpubkey_address") == tx.get("change_address"):
                change_type = v.get("scriptpubkey_type")
                break
        if change_type is None:
            change_type = "unknown"
        # Flag: Legacy-Adresse (P2PKH oder P2SH)
        is_legacy = int(change_type in ["p2pkh", "p2sh"])
        feats["change_addr_is_legacy"] = is_legacy
        # Konkreter Adresstyp als lesbarer String
        type_map = {
            "p2pkh": "P2PKH",
            "p2sh": "P2SH",
            "v0_p2wpkh": "P2WPKH",
            "v0_p2wsh": "P2WSH",
            "v1_p2tr": "P2TR",
        }
        feats["change_addr_type"] = type_map.get(change_type, change_type.upper())

        rows.append(feats)

    df = pd.DataFrame(rows)
    if args.one_per_change:
        df = df.drop_duplicates(subset=["change_address"], keep="first")
    df.insert(0, "row_number", range(1, len(df) + 1))

    df.to_csv(args.out_file, index=False)
    print(f"{len(df)} Datensätze in '{args.out_file}' geschrieben.")

if __name__ == "__main__":
    main()

