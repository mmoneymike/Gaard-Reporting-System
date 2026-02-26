import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

SECTION_HEADER = "Header"
SECTION_DATA = "Data"
SECTION_META = "MetaInfo"

@dataclass(frozen=True)
class PortfolioData:
    """Strict definition of this portfolio data contract."""
    holdings: pd.DataFrame          # Open Positions
    account_title: str              
    report_date: str                # Last Date of Analysis Period
    quarter_start_date: str         # Start Date of Analysis Period
    total_nav: float                
    key_statistics: dict            # Aggregated summary stats (Pulls Analysis Period and Change in NAV)
    settled_cash: float             
    legal_notes: pd.DataFrame       
    daily_history: pd.DataFrame     # From Inception CSV section: Cumulative Performance Statistics

@dataclass(frozen=True)
class QuarterStatementMetadata:
    name: str | None
    account: str | None
    period: str | None
    
@dataclass(frozen=True)
class QuarterStatementSections:
    raw_sections: dict[str, pd.DataFrame]
    metainfo: dict[str, dict[str, str]]
    introduction: pd.DataFrame
    key_statistics: pd.DataFrame
    perf_by_symbol: pd.DataFrame
    open_positions: pd.DataFrame    
    dividends: pd.DataFrame         
    legal_notes: pd.DataFrame       
    cash_report: pd.DataFrame       

@dataclass(frozen=True)
class SinceInceptionData:
    daily_returns: pd.DataFrame
    risk_measures: pd.DataFrame


#  ==========================================
#    QUARTERLY STATEMENT INGESTION
#  ==========================================

# === QUARTER STATEMENT CSV PARSING ===
def read_quarter_statement_csv(path: str | Path) -> tuple[dict[str, pd.DataFrame], dict[str, dict[str, str]]]:
    """Parses Quarterly Statement into DataFrames and MetaInfo dicts."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Statement CSV not found at {path}")

    headers: dict[str, list[str]] = {}
    rows: dict[str, list[dict[str, str]]] = defaultdict(list)
    metainfo: dict[str, dict[str, str]] = defaultdict(dict)

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
            elif record_type == SECTION_META:
                # MetaInfo format: Section, MetaInfo, Key, Value
                if len(raw_row) >= 4:
                    key = raw_row[2].strip()
                    val = raw_row[3].strip()
                    metainfo[section][key] = val

    dfs = {k: pd.DataFrame(v) for k, v in rows.items()}
    return dfs, metainfo


# === PULL & BUILD SECTIONS FROM QUARTERLY STATEMENT ===
def build_statement_sections(path: str | Path) -> QuarterStatementSections:
    """Builds individual sections from Quarter Statement."""
    raw_sections, metainfo = read_quarter_statement_csv(path)
    
    return QuarterStatementSections(
        raw_sections=raw_sections, 
        metainfo=metainfo,
        introduction=raw_sections.get("Introduction", pd.DataFrame()),
        key_statistics=raw_sections.get("Key Statistics", pd.DataFrame()),
        perf_by_symbol=raw_sections.get("Performance by Symbol", pd.DataFrame()),
        open_positions=raw_sections.get("Open Position Summary", pd.DataFrame()),
        dividends=raw_sections.get("Dividends", pd.DataFrame()),
        legal_notes=raw_sections.get("Notes and Disclosure", pd.DataFrame()),
        cash_report=raw_sections.get("Cash Report", pd.DataFrame()) # Kept purely as fallback
    )
    
   
# === HELPER FUNCTIONS === 
def extract_metadata(sections: QuarterStatementSections) -> QuarterStatementMetadata:
    """Extract metadata: Name, Account, Period from Quarter Statement."""
    name = None
    account = None
    period = None

    # 1. Name & Account from Introduction
    intro_df = sections.introduction
    if not intro_df.empty:
        if "Name" in intro_df.columns:
            name = intro_df["Name"].iloc[0].strip()
        if "Account" in intro_df.columns:
            account = intro_df["Account"].iloc[0].strip()

    # 2. Period from Key Statistics MetaInfo
    key_stats_meta = sections.metainfo.get("Key Statistics", {})
    period = key_stats_meta.get("Analysis Period", period)


    return QuarterStatementMetadata(
        name=name,
        account=account,
        period=period
    )


def extract_key_statistics(sections: QuarterStatementSections) -> dict:
    """Extracts BeginningNAV, EndingNAV, Change in Interest Accruals (Other), etc."""
    df = sections.key_statistics
    stats = {}
    
    if df.empty:
        return stats
        
    row = df.iloc[0]
    
    fields = [
        'BeginningNAV', 'EndingNAV', 'CumulativeReturn', 'MTM', 
        'Deposits & Withdrawals', 'Dividends', 'Interest', 
        'Fees & Commissions', 'Other', 'ChangeInNAV'
    ]
    
    for field in fields:
        if field in df.columns:
            val = _coerce_float(row[field])
            # If it's the return field, divide by 100 to get the decimal
            if field == 'CumulativeReturn':
                stats[field] = val / 100
            else:
                stats[field] = val
            
    # MAP 'Other' to 'ChangeInInterestAccruals' for PDF reporting
    stats['ChangeInInterestAccruals'] = stats.get('Other', 0.0)
            
    return stats


def extract_settled_cash(sections: QuarterStatementSections):
    """Pulls cash from the Open Position Summary where Symbol is USD."""
    try:
        df = sections.open_positions
        if df is None or df.empty or 'Symbol' not in df.columns:
            return 0.0
            
        # Filter for the Cash row (Symbol is USD)
        cash_row = df[df['Symbol'].astype(str).str.strip().str.upper() == 'USD']
        
        if cash_row.empty: 
            return 0.0
            
        # Return the 'Value' of the cash position
        return _coerce_float(cash_row['Value'].iloc[0])
    except Exception as e:
        print(f"Warning: Could not extract settled cash: {e}")
        return 0.0


def _coerce_float(value) -> float:
    """String-to-float converter."""
    if pd.isna(value) or value is None or value == "": return 0.0
    if isinstance(value, (int, float)): return float(value)
    
    cleaned = str(value).replace(",", "").replace("$", "").replace("%", "").strip()
    if not cleaned: return 0.0
    
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = "-" + cleaned[1:-1]
        
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def is_valid_ticker(ticker: str) -> bool:
    """Valid ticker check."""
    t = str(ticker).upper().strip()
    if not t: return False
    invalid = ['TOTAL', 'SUBTOTAL', 'STOCKS', 'EQUITY', 'BONDS', 'CASH', 'FUNDS']
    if any(x in t for x in invalid): return False
    return True


# --- INDIVIDUAL ASSET RETURN CALCULATION LOGIC ---
def process_holdings_from_data(sections: QuarterStatementSections) -> pd.DataFrame:
    """
    Manually calculates returns and weights using:
    (Current Value - Cost Basis + Dividends + Realized P&L) / Cost Basis
    """
    df_open = sections.open_positions
    df_perf = sections.perf_by_symbol
    df_div = sections.dividends

    # 1. Clean Open Positions
    if not df_open.empty and 'Symbol' in df_open.columns:
        df_open = df_open[df_open['Symbol'].apply(is_valid_ticker)].copy()
        df_open['Value'] = df_open.get('Value', pd.Series(dtype=float)).apply(_coerce_float)
        df_open['Cost Basis'] = df_open.get('Cost Basis', pd.Series(dtype=float)).apply(_coerce_float)
    else:
        df_open = pd.DataFrame(columns=['Symbol', 'Value', 'Cost Basis', 'Description', 'Sector'])

    # 2. Clean Dividends
    div_map = {}
    if not df_div.empty and 'Symbol' in df_div.columns:
        df_div_clean = df_div[df_div['Symbol'].apply(is_valid_ticker)].copy()
        df_div_clean['Amount'] = df_div_clean.get('Amount', pd.Series(dtype=float)).apply(_coerce_float)
        div_map = df_div_clean.groupby('Symbol')['Amount'].sum().to_dict()

    # 3. Clean Performance by Symbol (Realized P&L, Fallback Meta, & Fallback Returns)
    real_map = {}
    desc_map = {}
    sector_map = {}
    ibkr_return_map = {} 
    
    if not df_perf.empty and 'Symbol' in df_perf.columns:
        df_perf_clean = df_perf[df_perf['Symbol'].apply(is_valid_ticker)].copy()
        df_perf_clean['Realized_P&L'] = df_perf_clean.get('Realized_P&L', pd.Series(dtype=float)).apply(_coerce_float)
        df_perf_clean['Return'] = df_perf_clean.get('Return', pd.Series(dtype=float)).apply(_coerce_float) 
        
        real_map = df_perf_clean.groupby('Symbol')['Realized_P&L'].sum().to_dict()
        desc_map = df_perf_clean.set_index('Symbol')['Description'].to_dict()
        sector_map = df_perf_clean.set_index('Symbol')['Sector'].to_dict()
        
        # We divide by 100 because IBKR usually reports 3.07 for 3.07%
        ibkr_return_map = (df_perf_clean.set_index('Symbol')['Return'] / 100).to_dict()

    # Aggregate all unique symbols
    all_symbols = set(df_open['Symbol']) | set(div_map.keys()) | set(real_map.keys())
    all_symbols.discard('')
    
    rows = []
    for symbol in all_symbols:
        o = df_open[df_open['Symbol'] == symbol]
        
        cv = o['Value'].sum() if not o.empty else 0.0
        cb = o['Cost Basis'].sum() if not o.empty else 0.0
        
        # Meta fallback
        if not o.empty:
            desc = o['Description'].iloc[0]
            sector = o['Sector'].iloc[0]
        else:
            desc = desc_map.get(symbol, '')
            sector = sector_map.get(symbol, '')
            
        div = div_map.get(symbol, 0.0)
        real = real_map.get(symbol, 0.0)
        
        # --- RETURN LOGIC ---
        # Use the official IBKR return for the symbol if available (TWR)
        # Fallback to simple ROI only if IBKR data is missing
        official_twr = ibkr_return_map.get(symbol)
        if official_twr is not None:
            ret = official_twr
        elif cb > 0:
            total_gen = (cv - cb) + div + real
            ret = total_gen / cb
        else:
            ret = 0.0
    
        rows.append({
            'ticker': symbol,
            'description': desc,
            'asset_class': sector,
            'raw_value': cv,
            'avg_cost': cb,
            'total_dividends': div,
            'realized_pl': real,
            'cumulative_return': ret
        })
        
    df = pd.DataFrame(rows)
    
    # Calculate updated weights strictly based on current values
    if not df.empty:
        total_val = df['raw_value'].sum()
        df['avg_weight'] = df['raw_value'] / total_val if total_val else 0.0
    else:
        df = pd.DataFrame(columns=['ticker', 'description', 'asset_class', 'raw_value', 'avg_cost', 'total_dividends', 'realized_pl', 'cumulative_return', 'avg_weight'])
        
    return df


# --- MAIN ENTRY POINT FOR QUARTERLY STATEMENT ---
def get_portfolio_holdings(quarterly_stmt_csv: str, benchmark_default_date: str) -> PortfolioData:
    """
    Ingests the quarterly statement (main statement).
    """
    sections = build_statement_sections(quarterly_stmt_csv)
    meta = extract_metadata(sections)
    
    account_title = meta.name if meta.name else "Total Portfolio"
    
    # 1. Report End Date (from 'Analysis Period'). Temporarily assigned PORTFOLIO_FALLBACK_DATA in main.py
    report_date = benchmark_default_date
    period_start_date = benchmark_default_date
    
    # Assign Report Start/End Date from statement metadata
    if meta.period:
        try:
            parts = meta.period.split('-')
            if len(parts) == 2:
                period_start_date = pd.to_datetime(parts[0].strip()).strftime('%Y-%m-%d')
                report_date = pd.to_datetime(parts[1].strip()).strftime('%Y-%m-%d')
        except Exception as e:
            print(f"Warning: Could not parse Period Dates ({e}).")

    # 2. Extract Key Statistics & NAV
    key_stats = extract_key_statistics(sections)
    total_nav = key_stats.get("EndingNAV", 0.0)
    
    # 3. Extract Holdings (Manually calculated to ensure accuracy)
    holdings_df = process_holdings_from_data(sections)
    
    # 4. Extract Cash
    settled_cash = extract_settled_cash(sections)
    
    return PortfolioData(
        holdings=holdings_df, 
        account_title=account_title,
        report_date=report_date,
        quarter_start_date=period_start_date,
        total_nav=total_nav,
        key_statistics=key_stats,
        settled_cash=settled_cash,
        legal_notes=sections.legal_notes, 
        daily_history=pd.DataFrame() 
    )
    
    
#  ==========================================
#    SINCE INCEPTION (PERFORMANCE) INGESTION
#  ==========================================
def parse_since_inception_csv(since_inception_stmt_csv: str) -> SinceInceptionData:
    """Parses the Since Inception CSV for Cumulative Performance and Risk Measures."""
    if not since_inception_stmt_csv or not Path(since_inception_stmt_csv).exists():
        return SinceInceptionData(pd.DataFrame(), pd.DataFrame())

    raw_sections, _ = read_quarter_statement_csv(since_inception_stmt_csv)
    
    # --- 1. Performance Data ---
    perf_df = raw_sections.get("Cumulative Performance Statistics", pd.DataFrame())
        
    if not perf_df.empty and 'Date' in perf_df.columns and 'Return' in perf_df.columns:
        perf_df['date'] = pd.to_datetime(perf_df['Date'], format='%m/%d/%y', errors='coerce')
        
        # Divide the raw 'Return' column by 100
        perf_df['return'] = perf_df['Return'].apply(_coerce_float) / 100
        
        daily_returns = perf_df.dropna(subset=['date', 'return']).sort_values('date').copy()
    else:
        daily_returns = pd.DataFrame(columns=['date', 'return'])

    # --- 2. Risk Measures Extraction & Cleaning ---
    risk_df = raw_sections.get("Risk Measures", pd.DataFrame())
    processed_risk = {}

    if not risk_df.empty and 'Risk Measure' in risk_df.columns and 'Account Value' in risk_df.columns:
        # Create a dictionary mapping the metric name (without the colon) to its value
        # Example: "Max Drawdown:" -> "Max Drawdown"
        val_map = {
            str(row['Risk Measure']).replace(':', '').strip(): row['Account Value'] 
            for _, row in risk_df.iterrows()
        }
        
        # Extract the metrics based on their exact names in the CSV
        processed_risk['Ending VAMI'] = _coerce_float(val_map.get('Ending VAMI', 0))
        processed_risk['Max Drawdown'] = _coerce_float(val_map.get('Max Drawdown', 0))
        processed_risk['Sharpe Ratio'] = _coerce_float(val_map.get('Sharpe Ratio', 0))
        processed_risk['Sortino Ratio'] = _coerce_float(val_map.get('Sortino Ratio', 0))
        processed_risk['Standard Deviation'] = _coerce_float(val_map.get('Standard Deviation', 0))
        processed_risk['Downside Deviation'] = _coerce_float(val_map.get('Downside Deviation', 0))
        processed_risk['Mean Return'] = _coerce_float(val_map.get('Mean Return', 0))
        # Keep text-based fields as strings 
        processed_risk['Peak-To-Valley'] = str(val_map.get('Peak-To-Valley', ''))
        processed_risk['Recovery'] = str(val_map.get('Recovery', ''))
        processed_risk['Positive Periods'] = str(val_map.get('Positive Periods', ''))
        processed_risk['Negative Periods'] = str(val_map.get('Negative Periods', ''))   
        
    return SinceInceptionData(daily_returns=daily_returns, risk_measures=processed_risk)