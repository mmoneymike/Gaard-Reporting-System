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
    settled_cash: float


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
    cash_report: pd.DataFrame


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
    cash = raw_sections.get("Cash Report", pd.DataFrame())

    return StatementSections(
        raw_sections=raw_sections, 
        statement_metadata=meta, 
        accounts=account,
        positions=pos, 
        open_positions=open_pos, 
        dividends=divs, 
        perf_summary=performance, 
        nav_summary=nav,
        cash_report=cash)
    
   
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
    if accounts_df.empty: return "Total Portfolio"
    if "Name" not in accounts_df.columns: return "Total Portfolio"
    try:
        name_val = accounts_df["Name"].iloc[0]
        return str(name_val).strip()
    except Exception:
        return "Total Portfolio"


def extract_symbol_from_description(description: str) -> str | None:
    if not description: return None
    try:
        match = re.match(r"^([A-Z0-9.]+)\(", description.strip())
        return match.group(1) if match else None
    except Exception:
        return None


def extract_total_nav(sections: StatementSections) -> float:
    """
    STRICT NAV EXTRACTION:
    1. Section: 'Net Asset Value'
    2. Row Identifier: 'Account' column == 'Account Total'
    3. Target Value: 'Ending Net Asset Value' column
    """
    df = sections.nav_summary
    
    # 1. Validation: Check if section and required columns exist
    if df is None or df.empty:
        return 0.0
        
    required_cols = ['Account', 'Ending Net Asset Value']
    if not all(col in df.columns for col in required_cols):
        return 0.0

    try:
        # 2. Hardcode Row Selection
        # Filter strictly for 'Account Total' in the 'Account' column
        # .strip() handles potential whitespace like "Account Total "
        row = df[df['Account'].astype(str).str.strip() == 'Account Total']
        
        if row.empty:
            return 0.0

        # 3. Extract Value
        raw_val = row['Ending Net Asset Value'].iloc[0]
        
        # 4. Clean & Convert
        return _coerce_float(raw_val)

    except Exception:
        return 0.0


def extract_settled_cash(sections: pd.DataFrame):
    try:
        # 1. Hardcode Section
        if not hasattr(sections, 'cash_report'):
             return 0.0
             
        df = sections.cash_report
        
        if df is None or df.empty:
            return 0.0

        # 2. Hardcode Row Selection
        if 'Account' not in df.columns:
            return 0.0
            
        row = df[df['Account'].astype(str).str.strip() == 'Account Total']
        
        if row.empty:
            return 0.0

        # 3. Hardcode Column Selection
        if 'Ending Cash' not in df.columns:
            return 0.0
            
        raw_val = row['Ending Cash'].iloc[0]
        
        # 4. Clean & Convert
        if isinstance(raw_val, str):
            clean = raw_val.replace('$', '').replace(',', '').replace(' ', '')
            if '(' in clean and ')' in clean:
                clean = '-' + clean.replace('(', '').replace(')', '')
            return float(clean)
            
        return float(raw_val)

    except Exception as e:
        print(f"Error extracting hardcoded cash: {e}")
        return 0.0
    

def _coerce_float(value) -> float:
    """Robust string-to-float converter."""
    if value is None or value == "": return 0.0
    if isinstance(value, (int, float)): return float(value)
    
    cleaned = str(value).replace(",", "").replace("$", "").strip()
    if not cleaned: return 0.0
    
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = "-" + cleaned[1:-1]
        
    try:
        return float(cleaned)
    except ValueError:
        return 0.0
        
def is_valid_ticker(ticker: str) -> bool:
    t = str(ticker).upper().strip()
    if not t: return False
    invalid = ['TOTAL', 'SUBTOTAL', 'STOCKS', 'EQUITY', 'BONDS', 'CASH', 'FUNDS']
    if any(x in t for x in invalid): return False
    return True


# --- MAIN CALCULATION LOGIC ---
def calculate_cumulative_returns_with_dividends(sections: StatementSections) -> CumulativeReturnResults:
    
    # A. PREPARE OPEN POSITIONS
    if sections.open_positions.empty:
        df = pd.DataFrame(columns=['Symbol', 'cost_basis', 'market_value'])
    else:
        df = sections.open_positions.copy()
        
    if not df.empty and 'Symbol' in df.columns:
        df = df[df['Symbol'].str.strip() != '']
        df['Symbol'] = df['Symbol'].str.strip().str.upper()
        df = df[df['Symbol'].apply(is_valid_ticker)]
        
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
            p_df = p_df[p_df['Symbol'].apply(is_valid_ticker)]
            
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
            d_df = d_df[d_df['Symbol'].apply(is_valid_ticker)]
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
            
            # Safety Valve: Fix infinite return on Cash-like positions
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
    
    # --- Robust NAV Extraction ---
    # We sum the column directly instead of looking for specific "Total" rows
    total_nav = extract_total_nav(sections)

    # --- Strict Settled Cash Extraction ---
    settled_cash = extract_settled_cash(sections)
                
    # --- Ensure realized_pl is passed to main.py ---
    # We rename columns first
    df = results.positions.rename(columns={
        'Symbol': 'ticker',
        'market_value': 'raw_value',
        'cost_basis': 'avg_cost'
    })
    
    # MUST include 'realized_pl' so main.py summary table works
    cols = ['ticker', 'avg_cost', 'raw_value', 'total_dividends', 'realized_pl', 'cumulative_return']
    
    # Safety check if df is empty or missing columns
    if df.empty:
        final_df = pd.DataFrame(columns=cols)
    else:
        final_df = df[cols].copy()
    
    return PortfolioData(
        holdings=final_df, 
        account_title=account_title,
        report_date=report_date,
        total_nav=total_nav,
        settled_cash=settled_cash
    )