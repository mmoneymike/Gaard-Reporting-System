import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


SECTION_HEADER = "Header"
SECTION_DATA = "Data"


@dataclass(frozen=True)
class PortfolioData:
    """Strict definition of this portfolio data contract."""
    holdings: pd.DataFrame
    account_title: str
    report_date: str
    total_nav: float


@dataclass(frozen=True)
class StatementMetadata:
    title: str | None
    period: str | None
    when_generated: str | None
    
    
@dataclass(frozen=True)
class StatementSections:
    raw_sections: dict[str, pd.DataFrame]
    statement_metadata: "StatementMetadata"
    accounts: pd.DataFrame
    positions: pd.DataFrame                 # Model (Asset Class/Bucket)
    open_positions: pd.DataFrame            # Cost Basis & Value
    dividends: pd.DataFrame                 
    perf_summary: pd.DataFrame
    nav_summary: pd.DataFrame               # Account Total


@dataclass(frozen=True)
class CumulativeReturnResults:
    positions: pd.DataFrame


# --- CSV PARSING ---
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


def build_statement_sections(path: str | Path) -> StatementSections:
    raw_sections = read_statement_csv(path)
    
    meta = extract_statement_metadata(raw_sections.get("Statement", pd.DataFrame()))
    account = raw_sections.get("Accounts", pd.DataFrame())
    pos = raw_sections.get("Positions", pd.DataFrame())
    open_pos = raw_sections.get("Open Positions", pd.DataFrame())
    divs = raw_sections.get("Dividends", pd.DataFrame())
    performance = raw_sections.get("Realized & Unrealized Performance Summary", pd.DataFrame())
    nav = raw_sections.get("Net Asset Value", pd.DataFrame())

    return StatementSections(
        raw_sections=raw_sections, 
        statement_metadata=meta, 
        accounts=account,
        positions=pos, 
        open_positions=open_pos, 
        dividends=divs, 
        perf_summary=performance, 
        nav_summary=nav)
    
   
# --- HELPER FUNCTIONS --- 
def extract_statement_metadata(statement: pd.DataFrame) -> StatementMetadata:
    """Extracts metadata from the 'Statement' section in IKBR CSV."""
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
    
    
def extract_account_name(accounts_df: pd.DataFrame) -> str:
    """
    Extracts the Account Name from the 'Accounts' section of the raw dataframe.
    Expected format: Accounts, Data, [Name], [ID], ...
    """
    if accounts_df.empty:
        return "Total Portfolio"
   
    if "Name" not in accounts_df.columns:
        return "Total Portfolio"

    try:
        # Grab the first row's value in the "Name" column
        name_val = accounts_df["Name"].iloc[0]
        return str(name_val).strip()
    except Exception:
        return "Total Portfolio"


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
        
    return "2025-07-30" # Fallback


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


# --- MAIN CALCULATION LOGIC ---
# --- MAIN CALCULATION LOGIC ---
def calculate_cumulative_returns_with_dividends(sections: StatementSections) -> CumulativeReturnResults:
    
    # A. PREPARE OPEN POSITIONS
    if sections.positions.empty:
        df = pd.DataFrame(columns=['Symbol', 'cost_basis', 'market_value'])
    else:
        df = sections.positions.copy()
        
    if not df.empty and 'Symbol' in df.columns:
        # 1. Clean Symbol
        df = df[df['Symbol'].str.strip() != '']
        df['Symbol'] = df['Symbol'].str.strip().str.upper()
        
        # 3. Coerce Values
        if 'Cost Basis' in df.columns:
            df['cost_basis'] = df['Cost Basis'].apply(_coerce_float)
        else:
            df['cost_basis'] = 0.0

        if 'Value' in df.columns:
            df['market_value'] = df['Value'].apply(_coerce_float)
        else:
            df['market_value'] = 0.0

    # B. PROCESS REALIZED P/L
    realized_pl_map = {}
    if not sections.perf_summary.empty:
        p_df = sections.perf_summary.copy()
        
        if 'Symbol' in p_df.columns and 'Realized Total' in p_df.columns:
            p_df = p_df[p_df['Symbol'].str.strip() != '']
            p_df['Symbol'] = p_df['Symbol'].str.strip().str.upper()
            
            p_df['Realized Total'] = p_df['Realized Total'].apply(_coerce_float)
            realized_pl_map = p_df.groupby('Symbol')['Realized Total'].sum().to_dict()

    # C. PROCESS DIVIDENDS
    div_map = {}
    if not sections.dividends.empty:
        d_df = sections.dividends.copy()
        if "Symbol" not in d_df.columns and "Description" in d_df.columns:
            d_df['Symbol'] = d_df['Description'].apply(extract_symbol_from_description)
            
        if "Symbol" in d_df.columns:
            d_df['Symbol'] = d_df['Symbol'].str.strip().str.upper()
            d_df['Amount'] = d_df['Amount'].apply(_coerce_float)
            div_map = d_df.groupby('Symbol')['Amount'].sum().to_dict()

    # D. MERGE ALL DATA
    all_tickers = set(df['Symbol'].unique()) | set(realized_pl_map.keys()) | set(div_map.keys())
    all_tickers.discard(None)
    all_tickers.discard('')

    final_rows = []
    
    for ticker in all_tickers:
        held_rows = df[df['Symbol'] == ticker]
        
        if not held_rows.empty:
            cost = held_rows['cost_basis'].sum()
            mv = held_rows['market_value'].sum()
            
            # --- THE SAFETY VALVE ---
            # If Market Value exists but Cost Basis is 0 (common for Cash/Transfers),
            if cost == 0.0 and mv != 0.0:
                cost = mv
                
        else:
            cost = 0.0
            mv = 0.0
            
        r_pl = realized_pl_map.get(ticker, 0.0)
        divs = div_map.get(ticker, 0.0)
        
        final_rows.append({
            'ticker': ticker,
            'avg_cost': cost,
            'raw_value': mv,
            'realized_pl': r_pl,
            'total_dividends': divs
        })
        
    final_df = pd.DataFrame(final_rows)

    # E. FINAL METRICS
    if not final_df.empty:
        final_df['total_generated_value'] = final_df['raw_value'] + final_df['total_dividends'] + final_df['realized_pl']
        
        def calc_ret(row):
            if row['avg_cost'] != 0:
                return (row['total_generated_value'] / row['avg_cost']) - 1.0
            return 0.0

        final_df['cumulative_return'] = final_df.apply(calc_ret, axis=1)
    
    return CumulativeReturnResults(positions=final_df)


# --- MAIN ENTRY POINT ---
def get_portfolio_holdings(file_path, benchmark_default_date: str):
    """
    Returns: (DataFrame, account title, report_date, total_nav_from_file)
    """
    sections = build_statement_sections(file_path)
    account_title = extract_account_name(sections.accounts)
    results = calculate_cumulative_returns_with_dividends(sections)
    
    # Extract Date
    meta = sections.statement_metadata
    raw_date = meta.when_generated
    report_date = raw_date.split(',')[0].strip() if raw_date else benchmark_default_date
    
    # Get True NAV
    total_nav = get_total_nav_from_file(sections)

    if results.positions.empty:
        return pd.DataFrame(), report_date, total_nav, 0.0

    # Rename to Pipeline Standard
    df = results.positions.rename(columns={
        'Symbol': 'ticker',
        'market_value': 'raw_value',
        'cost_basis': 'avg_cost'
    })
    cols = ['ticker', 'avg_cost', 'raw_value', 'total_dividends', 'cumulative_return']
    
    return PortfolioData(
        holdings=df[cols], 
        account_title=account_title,
        report_date=report_date,
        total_nav=total_nav,
    )

# ========================================
#  ASSET CLASSIFICATION MECHANICISMS (WIP)
# ========================================
def auto_classify_asset(ticker: str, security_name: str) -> str:
    """
    Determines Asset Class based on Ticker and Official Name.
    Currently uses hard-coded asset classification and then keywords.
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