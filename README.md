# Gaard Reporting System

Generates quarterly portfolio PDF reports from Interactive Brokers Flex Query CSVs, using Yahoo Finance for benchmark and security reference data.

## Prerequisites

- Python 3.x
- Install dependencies: `pip install -r requirements.txt` (from the project root)
- **GnuPG** (`gpg`) on your PATH — PGP decryption in `ib_connector` shells out to GnuPG
- Run the program **from the `src` directory** so local imports resolve

## Using the switchboard

All runtime options you normally change live in **`src/main.py`**, in the block labeled **`SWITCHBOARD`** (at the top of the file, after the imports). Set these values, save the file, then run `main` (see [Run](#run)).

### 1. Page visibility

Each `show_page_*` variable is a boolean: `True` includes that section in the PDF, `False` skips it.

| Variable | Purpose |
|----------|---------|
| `show_page_cover` | Cover page |
| `show_page_table_of_contents` | Table of contents |
| `show_page_goals_and_objectives` | Goals and objectives |
| `show_page_target_allocations` | Target allocations |
| `show_page_breakdown_of_accounts` | Breakdown of accounts (**consolidated reports only**) |
| `show_page_change_in_portfolio_value` | Change in portfolio value |
| `show_page_portfolio_overview` | Portfolio overview |
| `show_page_portfolio_performance` | Portfolio performance |
| `show_page_expanded_performance` | Expanded performance |
| `show_page_risk_analysis` | Risk analysis |
| `show_page_financial_statistics` | Financial statistics |
| `show_page_macro_views` | Macro views |
| `show_page_market_review` | Market review |
| `show_page_disclosures` | Disclosures |
| `show_page_end_cover` | End cover |

### 2. Per-client benchmark allocation

- **`CLIENT_BENCHMARK_OVERRIDES`** — Dictionary mapping a **substring** of the account name (case-insensitive) to a tuple `(SPY_weight, AGG_weight)`. The two weights **must sum to 1.0**.
- **`DEFAULT_BENCHMARK_RATIO`** — `(SPY, AGG)` tuple used when no override key matches the account name.

### 3. Report type filter

**`REPORT_TYPE`** controls which accounts get a report:

| Value | Effect |
|-------|--------|
| `'both'` | Individual (IBKR `U…` accounts) and consolidated (`CONSOL_…`) |
| `'individual'` | Only individual accounts |
| `'consolidated'` | Only consolidated accounts |

### 4. Data source

**`USE_TEST_DATA`**

- `False` (default): Connect to IBKR SFTP, download encrypted files, decrypt with PGP, and read decrypted CSVs from `data/raw_downloads/`.
- `True`: Skip SFTP. Scan **`data/test_data/`** for subfolders whose names indicate **inception** vs **quarterly** data (see `scan_test_data_folder` in `main.py`), and use the CSVs inside those folders.

### 5. SFTP and PGP connection

| Setting | Purpose |
|---------|---------|
| `IB_SFTP_HOST` | IBKR SFTP hostname |
| `IB_USERNAME` | SFTP username |
| `REMOTE_STATEMENT_DIR` | Remote directory to download from (e.g. `outgoing`) |
| `SSH_PRIVATE_KEY_PATH` | Path to SSH private key file |
| `SSH_PUBLIC_KEY_PATH` | Path to SSH public key file |
| `PGP_PRIVATE_KEY_PATH` | Path to PGP private key file |
| `PGP_PUBLIC_KEY_PATH` | Path to PGP public key file |
| `PGP_PASSPHRASE` | Passphrase for the PGP private key, or `None` if none |

Update the key paths to match **your machine** (the repo may ship with example absolute paths). **These files are sensitive**; keep them out of version control in shared environments.

### 6. Benchmark definitions

- **`BENCHMARK_CONFIG`** — For each asset class key, a list of ETF tickers used as benchmarks in allocation-style tables (e.g. `'U.S. Equities': ['SPY']`).
- **`BENCHMARK_NAMES`** — Display names for tickers (used in the PDF).

### 7. Ticker filters

- **`IGNORE_EXACT`** — Ticker strings to drop from holdings (case-insensitive **exact** match), e.g. aggregate cash lines.
- **`IGNORE_STARTSWITH`** — Any holding whose ticker **starts with** one of these strings is excluded (e.g. specific Treasury CUSIP prefixes).

## Run

From the project root:

```bash
cd src
python main.py
```

On some systems use `python3` instead of `python`.

PDFs are written to **`output/`** with names like `{AccountTitle}_Quarter_Portfolio_Report_{YYYYMMDD_HHMM}.pdf`.

**Other inputs (not on the switchboard):** `data/info_for_pdf.xlsx` supplies key/value text for the PDF; logos live under `data/pdf_resources/logos/` (`gaard_logo.png`, `gaard_text_logo.png`).

---

## System map

High-level flow from data fetch through PDF output:

```mermaid
flowchart LR
  subgraph inputs [Inputs]
    SFTP[IB_SFTP]
    Keys[SSH_PGP_keys]
    TestData[data_test_data]
    InfoXlsx[info_for_pdf_xlsx]
    Logos[pdf_resources_logos]
  end
  subgraph core [Core]
    Main[main_py]
    IB[ib_connector]
    Stmt[statement_ingestion]
    YF[yf_loader]
    Ret[return_metrics]
    Risk[risk_metrics]
    PDF[pdf_writer]
  end
  SFTP --> IB
  Keys --> IB
  IB --> Decrypt[decrypt_PGP]
  Decrypt --> RawDL[data_raw_downloads]
  TestData --> Main
  RawDL --> Main
  Main --> Stmt
  Main --> YF
  Main --> Ret
  Main --> Risk
  Stmt --> Main
  YF --> Main
  Ret --> Main
  Risk --> Main
  InfoXlsx --> Main
  Logos --> Main
  Main --> PDF
  PDF --> Out[output_PDFs]
```

### Root

| Artifact | Role | What you can do |
|----------|------|-----------------|
| [requirements.txt](requirements.txt) | Python package dependencies (`pandas`, `yfinance`, `fpdf2`, `altair`, `paramiko`, `python-gnupg`, `scipy`, `openpyxl`, etc.) | Add or pin versions when upgrading tooling |

### `src/` — Python modules

| File | Role | What you can do |
|------|------|-----------------|
| [src/main.py](src/main.py) | Entry point: switchboard, account discovery and date-aware pairing, orchestrates ingest → market data → metrics → PDF per account | Edit **SWITCHBOARD**; extend `auto_classify_asset` for classification rules; adjust pairing helpers if IBKR CSV layout changes |
| [src/ib_connector.py](src/ib_connector.py) | SFTP download from IBKR; PGP decrypt; unzip when needed; writes under `data/raw_encrypted_downloads` and `data/raw_downloads` | Adjust remote paths or diagnostics if SFTP layout changes |
| [src/statement_ingestion.py](src/statement_ingestion.py) | Parses Flex Query CSVs: holdings, consolidated breakdown, key statistics, legal notes; `parse_since_inception_csv` for daily performance | Update parsers if IBKR section names or structure change |
| [src/yf_loader.py](src/yf_loader.py) | Yahoo Finance: benchmark returns and security names for tickers | Change download logic or swap data source |
| [src/return_metrics.py](src/return_metrics.py) | Return math: cumulative indices, period windows, composite benchmark blend, chart preparation | Adjust windows or benchmark blending |
| [src/risk_metrics.py](src/risk_metrics.py) | Risk metrics vs benchmark and factor betas (`scipy`, Yahoo risk-free proxy) | Extend metrics or defaults |
| [src/pdf_writer.py](src/pdf_writer.py) | Multi-page PDF (`fpdf2`, `altair` charts); honors `page_visibility` | Layout, copy, colors, page order |
| [src/excel_writer.py](src/excel_writer.py) | Optional XLSX report (`write_portfolio_report_xlsx`) | **Not called** by the current pipeline (only imported in `main.py`); wire into `generate_report_for_account` if you want Excel output |
| [src/wrds_loader.py](src/wrds_loader.py) | Optional WRDS/CRSP benchmark returns | **Not used** by current `main`; integrate if replacing Yahoo for benchmarks |
| [src/statement_ingestion_old.py](src/statement_ingestion_old.py) | Legacy ingestion | Reference or remove after confirming nothing else imports it |
| [src/test_ib_connector.py](src/test_ib_connector.py) | Tests / manual checks for SFTP and PGP | Run when validating connectivity |

### `data/` — directories and static files

| Location | Role | What you can do |
|----------|------|-----------------|
| `data/Gaard_Keys/` | SSH and PGP key material (paths set on the switchboard) | Store keys securely; point `*_PATH` variables at these files |
| `data/raw_encrypted_downloads/` | Encrypted downloads from SFTP before decryption | Inspect failed deliveries; clean up old files if needed |
| `data/raw_downloads/` | Decrypted CSVs (and extracted archives) used for account pairing | Refresh by re-running SFTP mode; or copy IBKR exports here for debugging |
| `data/test_data/` | Folder-per-run style layout for `USE_TEST_DATA = True` | Add subfolders with `inception` / quarterly naming per `scan_test_data_folder` |
| `data/info_for_pdf.xlsx` | PDF boilerplate (`key` / `value` columns, header on row 3) | Edit text values without changing code |
| `data/pdf_resources/logos/` | `gaard_logo.png`, `gaard_text_logo.png` | Replace assets to rebrand |

### `output/`

| Location | Role | What you can do |
|----------|------|-----------------|
| `output/` | Generated `*_Quarter_Portfolio_Report_*.pdf` files | Archive or distribute reports; delete old runs if disk space matters |
