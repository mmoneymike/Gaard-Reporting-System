import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


SECTION_HEADER = "Header"
SECTION_DATA = "Data"


@dataclass(frozen=True)
class StatementSections:
    raw_sections: dict[str, pd.DataFrame]
    statement_metadata: "StatementMetadata"
    positions: pd.DataFrame                 # Model (Asset Class/Bucket)
    open_positions: pd.DataFrame            # Cost Basis & Value
    dividends: pd.DataFrame                 
    trades: pd.DataFrame
    nav_summary: pd.DataFrame               # Account Total


@dataclass(frozen=True)
class CumulativeReturnResults:
    positions: pd.DataFrame
    buckets: pd.DataFrame
    orphaned_divs: float                    # Dividends from assets not held anymore


@dataclass(frozen=True)
class StatementMetadata:
    title: str | None
    period: str | None
    when_generated: str | None

@dataclass(frozen=True)
class PortfolioData:
    """Strict definition of this portfolio data contract."""
    holdings: pd.DataFrame
    account_title: str
    report_date: str
    total_nav: float
    orphaned_divs: float

def read_statement_csv(path: str | Path) -> dict[str, pd.DataFrame]:
    """Parses IBKR CSV into a dictionary of DataFrames."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Statement CSV not found at {path}")

    headers: dict[str, list[str]] = {}
    rows: dict[str, list[dict[str, str]]] = defaultdict(list)

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for raw_row in reader:
            if not raw_row: continue
            section = raw_row[0].strip()
            record_type = raw_row[1].strip() if len(raw_row) > 1 else ""

            if record_type == SECTION_HEADER:
                headers[section] = [cell.strip() for cell in raw_row[2:]]
            elif record_type == SECTION_DATA:
                header = headers.get(section)
                values = [cell.strip() for cell in raw_row[2:]]
                if header:
                    padded = values + [""] * (len(header) - len(values))
                    rows[section].append(dict(zip(header, padded[: len(header)])))

    return {k: pd.DataFrame(v) for k, v in rows.items()}


def extract_statement_metadata(statement: pd.DataFrame) -> StatementMetadata:
    if statement.empty:
        return StatementMetadata(title=None, period=None, when_generated=None)

    def lookup(field_name: str) -> str | None:
        matches = statement[statement["Field Name"].str.strip().str.lower() == field_name.lower()]
        if matches.empty:
            return None
        value = matches["Field Value"].iloc[-1]
        if isinstance(value, str):
            value = value.strip()
        return value if value else None

    return StatementMetadata(
        title=lookup("Title"),
        period=lookup("Period"),
        when_generated=lookup("WhenGenerated"),
    )
    

def extract_generated_date(sections: dict) -> str:
    """
    Looks into the 'Statement' section for 'WhenGenerated'.
    Format in CSV: "2026-01-13, 10:55:58 EST"
    Returns: "2026-01-13"
    """
    df = sections.get("Statement", pd.DataFrame())
    if df.empty: return "2024-01-01" # Fallback

    # Look for the row where 'Field Name' is 'WhenGenerated'
    # Note: Column names might vary, so we check columns 0 and 1 roughly
    try:
        # Usually Column 0 is 'Field Name', Column 1 is 'Field Value'
        # But based on your CSV snippet: Field Name, Field Value
        row = df[df['Field Name'] == 'WhenGenerated']
        if not row.empty:
            raw_date = row['Field Value'].iloc[0]
            # Split "2026-01-13, 10:55..." -> "2026-01-13"
            return raw_date.split(',')[0].strip()
    except Exception:
        pass
        
    return "2024-01-01" # Fallback


def extract_symbol_from_description(description: str) -> str | None:
    if not description: return None
    # --- FIXED REGEX ---
    # Looks for characters like 'ICSH' followed immediately by a literal '('
    try:
        match = re.match(r"^([A-Z0-9.]+)\(", description.strip())
        return match.group(1) if match else None
    except Exception:
        return None


def _coerce_float(value) -> float:
    """Robust string-to-float converter (handles '$1,000.00' and '(500)')."""
    if value is None or value == "": return 0.0
    if isinstance(value, (int, float)): return float(value)
    
    cleaned = str(value).replace(",", "").replace("$", "").strip()
    if not cleaned: return 0.0
    
    # Handle Accounting Negative: (100) -> -100
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = "-" + cleaned[1:-1]
        
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def get_total_nav_from_file(sections: StatementSections) -> float:
    """
    Scans the 'Net Asset Value' section for the 'Account Total' row 
    and 'Ending Net Asset Value' column.
    """
    df = sections.nav_summary
    if df.empty: return 0.0
    
    # We look for rows where the header column (usually col 0 or 'Asset Class') says 'Total'
    # Based on standard IBKR CSV structure for NAV section:
    # Asset Class | ... | Ending Net Asset Value
    
    # Try to find the Total row
    # Iterate loosely to find "Account Total" in the dataframe
    
    try:
        # Standard IBKR CSV often has 'Asset Class' as the key column in NAV section
        if 'Asset Class' in df.columns and 'Ending' in df.columns: # 'Ending' matches 'Ending Net Asset Value' partially?
             # Let's look for exact column match
             target_col = [c for c in df.columns if "Ending" in c and "Net Asset Value" in c]
             if not target_col: return 0.0
             target_col = target_col[0]
             
             # Find row where Asset Class is 'Account Total' or 'Total'
             row = df[df['Asset Class'].str.contains('Total', case=False, na=False)]
             if not row.empty:
                 return _coerce_float(row[target_col].iloc[0])
                 
        # Fallback: Search all string columns for "Account Total"
        # This covers if the column name changed
        for col in df.select_dtypes(include=['object']):
            row = df[df[col].astype(str).str.contains('Account Total', case=False, na=False)]
            if not row.empty:
                # Find the column with the big number (Ending NAV)
                # Usually the last column or one named "Ending..."
                target_col = [c for c in df.columns if "Ending" in c and "Net Asset Value" in c]
                if target_col:
                    return _coerce_float(row[target_col[0]].iloc[0])
    except Exception:
        pass

    return 0.0


def build_statement_sections(path: str | Path) -> StatementSections:
    sections = read_statement_csv(path)
    
    meta = extract_statement_metadata(sections.get("Statement", pd.DataFrame()))
    pos = sections.get("Positions", pd.DataFrame())
    open_pos = sections.get("Open Positions", pd.DataFrame())
    divs = sections.get("Dividends", pd.DataFrame())
    trades = sections.get("Trades", pd.DataFrame())
    nav = sections.get("Net Asset Value", pd.DataFrame())

    return StatementSections(sections, meta, pos, open_pos, divs, trades, nav)


def calculate_cumulative_returns_with_dividends(sections: StatementSections) -> CumulativeReturnResults:
    if sections.open_positions.empty:
        return CumulativeReturnResults(pd.DataFrame(), pd.DataFrame(), 0.0)
        
    df = sections.open_positions.copy()
    df = df[df['Symbol'].str.strip() != '']
    df['Symbol'] = df['Symbol'].str.strip().str.upper()
    
    df['cost_basis'] = df['Cost Basis'].apply(_coerce_float)
    df['market_value'] = df['Value'].apply(_coerce_float)
    df['quantity'] = df['Quantity'].apply(_coerce_float)

    # --- DIVIDENDS & ORPHANS ---
    orphaned_total = 0.0
    
    if not sections.dividends.empty:
        divs = sections.dividends.copy()
        if "Symbol" not in divs.columns and "Description" in divs.columns:
            divs['Symbol'] = divs['Description'].apply(extract_symbol_from_description)
            
        divs['Symbol'] = divs['Symbol'].str.strip().str.upper()
        divs['Amount'] = divs['Amount'].apply(_coerce_float)
        
        # 1. Total Dividends per Ticker
        div_summary = divs.groupby('Symbol')['Amount'].sum().rename('total_dividends')
        
        # 2. Identify Orphans (Dividends for tickers NOT in Open Positions)
        held_tickers = df['Symbol'].unique()
        all_div_tickers = div_summary.index.unique()
        orphan_tickers = [t for t in all_div_tickers if t not in held_tickers]
        
        if orphan_tickers:
            orphaned_total = div_summary.loc[orphan_tickers].sum()
            
        # 3. Merge Held Dividends
        df = df.merge(div_summary, on='Symbol', how='left')
    else:
        df['total_dividends'] = 0.0
        
    df['total_dividends'] = df['total_dividends'].fillna(0.0)
    
    # Calculate Returns for Held Positions
    df['total_ending_value'] = df['market_value'] + df['total_dividends']
    
    def calc_ret(row):
        if row['cost_basis'] and row['cost_basis'] != 0:
            return (row['total_ending_value'] / row['cost_basis']) - 1.0
        return 0.0

    df['cumulative_return'] = df.apply(calc_ret, axis=1)
    
    return CumulativeReturnResults(positions=df, buckets=pd.DataFrame(), orphaned_divs=orphaned_total)


def get_portfolio_holdings(file_path, benchmark_default_date: str):
    """
    Returns: (DataFrame, report_date, total_nav_from_file, orphaned_dividends)
    """
    sections = build_statement_sections(file_path)
    results = calculate_cumulative_returns_with_dividends(sections)
    
    # Get True NAV
    total_nav = get_total_nav_from_file(sections)

    # Extract Date
    meta = sections.statement_metadata
    account_title = meta.title if meta.title else "Total Portfolio"
    raw_date = meta.when_generated
    report_date = raw_date.split(',')[0].strip() if raw_date else benchmark_default_date

    if results.positions.empty:
        return pd.DataFrame(), report_date, total_nav, 0.0

    # Rename to Pipeline Standard
    df = results.positions.rename(columns={
        'Symbol': 'ticker',
        'market_value': 'raw_value',
        'cost_basis': 'avg_cost'
    })
    
    # NOTE: We do NOT rely on 'Model' anymore for Asset Class.
    # We will do full Auto-Classification in main.py
    
    cols = ['ticker', 'avg_cost', 'raw_value', 'total_dividends', 'cumulative_return']
    
    return PortfolioData(
        holdings=df[cols], 
        account_title=account_title,
        report_date=report_date,
        total_nav=total_nav,
        oprhaned_divs=results.orphaned_divs,
    )

# ========================================
#  ASSET CLASSIFICATION MECHANICISMS below
# ========================================

# # NO LONGER USING
# def normalize_model_bucket(model: str) -> str:
#     """
#     Maps IBKR Model names to your 5 Standard Buckets.
#     Uses keyboard matching to match assets to asset class/bucket.
#     """
#     if not model: return "Unclassified"
#     cleaned = model.strip().lower()
#     # 1. U.S. Equities
#     if "domestic" in cleaned or "u.s" in cleaned or "us " in cleaned:
#         return "U.S. Equities"
#     # 2. International Equities
#     if "international" in cleaned:
#         return "International Equities"
#     # 3. Fixed Income
#     if "fixed" in cleaned or "income" in cleaned:
#         return "Fixed Income"
#     # 4. Alternative Assets (Real Assets)
#     if "alternative" in cleaned:
#         return "Alternative Assets" 
#     # 5. Cash
#     if "cash" in cleaned:
#         return "Cash"   
#     # Handle "Independent" or unknown
#     return "Unclassified"


# WORK IN PROGRESS - ASSET-CLASS SORTING AUTOMATION
def auto_classify_asset(ticker: str, security_name: str) -> str:
    """
    Determines Asset Class based on Ticker and Official Name.
    Uses 'Specific to General' logic to catch Cash Equivalents (VGSH)
    before they get caught by Fixed Income (AGG).
    """
    t = str(ticker).upper().strip()
    n = str(security_name).upper().strip()
    
    # --- 1. HARDCODED ASSET LIST ---
    cash_tickers = [
        'ICSH'                     
    ]
    if t in cash_tickers: return 'Cash'

    intl_tickers = [
        'VEA', 'VWO', 'IMTM'
    ]
    if t in intl_tickers: return 'International Equities'
    
    fi_tickers = [
        'BND', 'VGSH', 'VGIT'
    ]
    if t in fi_tickers: return 'Fixed Income'
    
    alt_tickers = [
        'VNQ', 'BCI'
    ]
    if t in alt_tickers: return 'Alternative Assets'
    
    # --- 2. SMART KEYWORD LOGIC ---
    # INTERNATIONAL EQUITIES
    intl_keywords = ['INTL', 'INTERNATIONAL', 'EMERGING', 'EUROPE', 'PACIFIC', 'ASIA', 'CHINA', 'JAPAN', 'EX-US', 'DEVELOPED MKT', 'VXUS', 'VEA', 'VWO']
    if any(k in n for k in intl_keywords):
        return 'International Equities'
        
    # FIXED INCOME
    fi_keywords = ['BOND', 'TREASURY', 'AGGREGATE', 'FIXED INC', 'MUNICIPAL', 'AGNCY', 'CORP BD', 'AGG', 'LQD']
    if any(k in n for k in fi_keywords):
        return 'Fixed Income'
        
    # ALTERNATIVE ASSETS (Real Assets)
    alt_keywords = ['REIT', 'REAL ESTATE', 'GOLD', 'SILVER', 'COMMODITY', 'CRYPTO', 'BITCOIN', 'OIL', 'GLD', 'IAU', 'SLV', 'VNQ']
    if any(k in n for k in alt_keywords):
        return 'Alternative Assets'

    # --- 3. DEFAULT ---
    # If it's in the database but matches none of the above, it's likely a standard US Stock
    return 'U.S. Equities'