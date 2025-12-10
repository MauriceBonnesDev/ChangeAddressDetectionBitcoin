# Change Address Detection on the Bitcoin Blockchain

This project focuses on **detecting change addresses** in Bitcoin transactions.
A **RBF dataset** serves as the ground truth. The project includes:

* Extracting transaction data from a data source
* Querying an Electrum server
* Preprocessing and expanding the data using n-hop neighborhoods
* Training a Graph Neural Network (HeteroGNN) on this data

---

## 📦 Requirements & Dataset

The project requires a dataset containing Bitcoin transaction IDs.
This dataset can, for example, be stored in a **database table**.

---

## 📥 Step 1: Fetch Transactions (`fetch_txs.py`)

The script `fetch_txs.py` reads transaction IDs from the database and sends queries to an Electrum server.
For each transaction ID, it downloads the complete transaction as **JSON**.

### Usage

```bash
python3 ./fetch_txs.py --db <PATH/TO/DB> --out-file <PATH/TO/OUTPUT.(json|csv)>
```

**Parameters:**

* `--db` : Path to the database containing the transaction IDs
* `--out-file` : Destination path for the generated JSON or csv file

The resulting file contains raw data of all fetched transactions.

---

## 🧹 Step 2: Preprocessing (`0) Preprocessing.ipynb`)

The JSON file created in the previous step is processed in this notebook.

Goals of preprocessing:

* Convert the data into a unified structure
* Build the **n-hop neighborhood** for each transaction
* Export the result as an **NDJSON** file

Example output format (ndjson):

```
{"nodes": [...], "edges": [...]}
{"nodes": [...], "edges": [...]}
...
```

### TODO

The steps from `fetch_txs.py` and the preprocessing notebook could be merged to avoid duplicate Electrum queries.

---

## 🧠 Step 3: GNN Training (`1) GNN.ipynb`)

The generated `.ndjson` file is loaded here and converted into the required **HeteroData** format.

Pipeline inside the notebook:

1. Load the data
2. Convert to PyTorch Geometric HeteroData
3. Train a **HeteroGNN model**
4. Evaluate and analyze results

The model learns to predict change addresses based on topological and transaction-related features.

---

## 📂 Project Structure (Example)

```
.
├── fetch_txs.py
├── db/
│   ├── mempool.db
├── data/
│   ├── raw.json
│   ├── processed.ndjson
├── notebooks/
│   ├── 0) Preprocessing.ipynb
│   ├── 1) GNN.ipynb
└── models/
    └── GNN1
```

---

## ✔️ Summary

* You start with a database of transaction IDs
* `fetch_txs.py` retrieves raw data from the Electrum server
* Preprocessing generates extended graph data (n-hop context)
* The GNN notebook handles training and evaluation

