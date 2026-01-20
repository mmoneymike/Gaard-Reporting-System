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
    positions: pd.DataFrame                 # Has Model (Asset Class/Bucket)
    open_positions: pd.DataFrame            # Has Cost Basis & Value
    dividends: pd.DataFrame                 
    trades: pd.DataFrame
    

@dataclass(frozen=True)
class CumulativeReturnResults:
    positions: pd.DataFrame
    buckets: pd.DataFrame


@dataclass(frozen=True)
class StatementMetadata:
    title: str | None
    period: str | None
    when_generated: str | None



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
    
    
def normalize_model_bucket(model: str) -> str:
    """
    Maps IBKR Model names to your 5 Standard Buckets.
    Uses keyboard matching to match assets to asset class/bucket.
    """
    if not model: return "Unclassified"
    cleaned = model.strip().lower()
    # 1. U.S. Equities
    if "domestic" in cleaned or "u.s" in cleaned or "us " in cleaned:
        return "U.S. Equities"
    # 2. International Equities
    if "international" in cleaned:
        return "International Equities"
    # 3. Fixed Income
    if "fixed" in cleaned or "income" in cleaned:
        return "Fixed Income"
    # 4. Alternative Assets (Real Assets)
    if "alternative" in cleaned:
        return "Alternative Assets" 
    # 5. Cash
    if "cash" in cleaned:
        return "Cash"   
    # Handle "Independent" or unknown
    return "Unclassified"


def extract_symbol_from_description(description: str) -> str | None:
    if not description: return None
    # Regex: Matches "ICSH(US...)" and captures "ICSH"
    match = re.match(r"^([A-Z0-9\\.]+)\\(", description.strip())
    return match.group(1) if match else None


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


def build_statement_sections(path: str | Path) -> StatementSections:
    sections = read_statement_csv(path)
    
    statement_metadata = extract_statement_metadata(sections.get("Statement", pd.DataFrame()))
    positions = sections.get("Positions", pd.DataFrame())
    open_positions = sections.get("Open Positions", pd.DataFrame())
    dividends = sections.get("Dividends", pd.DataFrame())
    trades = sections.get("Trades", pd.DataFrame())

    return StatementSections(sections, statement_metadata, positions, open_positions, dividends, trades)


def calculate_cumulative_returns_with_dividends(sections: StatementSections) -> CumulativeReturnResults:
    """
    1. Base: Open Positions (Cost Basis + Market Value)
    2. Join: Positions (Model Name)
    3. Join: Dividends (Total Amounts)
    4. Calculate: Return = (Market Value + Divs) / Cost Basis - 1
    """
    # --- 1. BASE: Open Positions (The Financial Truth) ---
    if sections.open_positions.empty:
        return CumulativeReturnResults(pd.DataFrame(), pd.DataFrame())
        
    df = sections.open_positions.copy()
    
    # Clean Symbols and filter out Subtotals
    df = df[df['Symbol'].str.strip() != '']
    df['Symbol'] = df['Symbol'].str.strip().str.upper()
    
    # Clean Numbers
    df['cost_basis'] = df['Cost Basis'].apply(_coerce_float)
    df['market_value'] = df['Value'].apply(_coerce_float)
    df['quantity'] = df['Quantity'].apply(_coerce_float)

    # --- 2. ENRICH: Get Model from 'Positions' ---
    if not sections.positions.empty:
        model_ref = sections.positions.copy()
        model_ref = model_ref[model_ref['Symbol'].str.strip() != '']
        model_ref['Symbol'] = model_ref['Symbol'].str.strip().str.upper()
        
        # LOGIC: "Use the first Model that has the name"
        # We drop duplicates to get unique Ticker -> Model mapping
        model_map = model_ref.drop_duplicates(subset=['Symbol'])[['Symbol', 'Model']]
        
        # Merge Model into Main DF
        df = df.merge(model_map, on='Symbol', how='left')
    else:
        df['Model'] = "Independent"

    # --- 3. BUCKET: Map Model to 5 Asset Classes ---
    df['Bucket'] = df['Model'].apply(normalize_model_bucket)

    # --- 4. DIVIDENDS: Sum by Ticker ---
    if not sections.dividends.empty:
        divs = sections.dividends.copy()
        # Parse Symbol from Description (e.g., "ICSH(...)")
        if "Symbol" not in divs.columns and "Description" in divs.columns:
            divs['Symbol'] = divs['Description'].apply(extract_symbol_from_description)
            
        divs['Symbol'] = divs['Symbol'].str.strip().str.upper()
        divs['Amount'] = divs['Amount'].apply(_coerce_float)
        
        # Group and Merge
        div_summary = divs.groupby('Symbol')['Amount'].sum().rename('total_dividends')
        df = df.merge(div_summary, on='Symbol', how='left')
    else:
        df['total_dividends'] = 0.0
        
    df['total_dividends'] = df['total_dividends'].fillna(0.0)

    # --- 5. CALCULATE RETURNS ---
    # Total Ending Value = Current Market Value + Cash Dividends Received
    df['total_ending_value'] = df['market_value'] + df['total_dividends']
    
    def calc_ret(row):
        # Prevent divide by zero
        if row['cost_basis'] and row['cost_basis'] != 0:
            return (row['total_ending_value'] / row['cost_basis']) - 1.0
        return 0.0

    df['cumulative_return'] = df.apply(calc_ret, axis=1)

    # --- 6. AGGREGATE BY BUCKET ---
    # We sum the raw dollars, THEN calculate the bucket return
    # This creates a true "Weighted" return
    bucket_cols = ['cost_basis', 'market_value', 'total_dividends', 'total_ending_value']
    bucket_summary = df.groupby('Bucket')[bucket_cols].sum()
    
    bucket_summary['cumulative_return'] = (
        bucket_summary['total_ending_value'] / bucket_summary['cost_basis']
    ) - 1.0
    
    # Sort for display
    bucket_summary = bucket_summary.sort_values('market_value', ascending=False)
    
    return CumulativeReturnResults(positions=df, buckets=bucket_summary.reset_index())


# ==========================================
# BRIDGE FUNCTION (Required for main.py)
# ==========================================
def get_portfolio_holdings(file_path):
    """
    Orchestrates the ingestion process and returns a standardized DataFrame
    AND the report date that main.py can consume.
    
    Returns: (DataFrame, str)
    """
    # 1. Parse and Calculate
    sections = build_statement_sections(file_path)
    results = calculate_cumulative_returns_with_dividends(sections)
    
    # 2. Extract Date from Metadata
    # Access the new metadata object you created
    meta = sections.statement_metadata
    raw_date = meta.when_generated # e.g., "2026-01-13, 10:55:58 EST"
    
    # Clean the date string
    report_date = "2024-01-01" # Default fallback
    if raw_date:
        # Split on comma to remove time
        report_date = raw_date.split(',')[0].strip()

    if results.positions.empty:
        return pd.DataFrame(), report_date

    # 3. Rename columns to Pipeline Standard
    df = results.positions.rename(columns={
        'Symbol': 'ticker',
        'Bucket': 'asset_class',
        'market_value': 'raw_value',
        'cost_basis': 'avg_cost'
    })
    
    # 4. Calculate Weight
    total_mv = df['raw_value'].sum()
    df['weight'] = df['raw_value'] / total_mv if total_mv else 0
    
    # 5. Return Clean Data AND Date
    cols = ['ticker', 'asset_class', 'weight', 'avg_cost', 'raw_value', 'total_dividends', 'cumulative_return']
    return df[cols], report_date

