import pandas as pd
import datetime

def get_cumulative_index(returns_df: pd.DataFrame, start_value: float = 100.0) -> pd.DataFrame:
    """
    Converts a Series/DataFrame of Percentage Returns into a Price Index.
    
    Formula: Start_Value * Cumulative Product of (1 + Return)
    Example: [0.01, 0.02] -> [101.0, 103.02]
    """
    if returns_df.empty:
        return pd.DataFrame()
        
    # 1. Fill NaNs with 0 (assume flat performance for missing days)
    clean_returns = returns_df.fillna(0.0)
    
    # 2. Calculate Growth Path
    # (1+r1) * (1+r2) * ...
    growth_factors = (1 + clean_returns).cumprod()
    
    # 3. Scale to Start Value
    price_index = start_value * growth_factors
    return price_index


def get_cumulative_return(series: pd.Series, window: str) -> float:
    """
    Calculates the total return over a specific window (e.g., '1Y', 'YTD', 'INCEPTION').
    Input: A Price Index Series (e.g. 100, 105, 110)
    """
    if series is None or series.empty: 
        return 0.0
    
    # Ensure Datetime Index
    series = series.dropna().sort_index()
    if not isinstance(series.index, pd.DatetimeIndex):
        return 0.0
        
    end_price = float(series.iloc[-1])
    end_date = series.index[-1]
    
    start_date = None
    window = window.upper()
    
    # --- WINDOW PARSING LOGIC ---
    if window == "INCEPTION":
        start_price = float(series.iloc[0])
        return (end_price / start_price) - 1.0

    elif window == "YTD":
        # Jan 1st of current year
        start_date = pd.Timestamp(year=end_date.year, month=1, day=1)

    elif window.endswith('Y') and window[:-1].isdigit():
        # "1Y" -> Subtract Calendar Year
        years = int(window[:-1])
        start_date = end_date - pd.DateOffset(years=years)
        
    elif window.endswith('M') and window[:-1].isdigit():
        # "1M" -> Subtract Calendar Month
        months = int(window[:-1])
        start_date = end_date - pd.DateOffset(months=months)
        
    else:
        # Default fallback
        start_price = float(series.iloc[0])
        return (end_price / start_price) - 1.0

    # --- PRICE LOOKUP ---
    # Handle "Start Date Before Inception"
    if start_date < series.index[0]:
        start_date = series.index[0]

    # Find price on that date (or closest previous date)
    start_price = series.asof(start_date)
    
    if pd.isna(start_price) or start_price == 0:
        start_price = float(series.iloc[0])
        
    return (end_price / start_price) - 1.0


def calculate_nav_performance(change_in_nav_df: pd.DataFrame) -> dict:
    """
    Calculates the Official NAV Return based on the 'Change in NAV' section.
    Formula: (Ending - (Start + Flows)) / (Start + Flows)
    """
    default_res = {'NAV': 0.0, 'Return': 0.0, 'Breakdown': {}}
    
    if change_in_nav_df is None or change_in_nav_df.empty:
        return default_res

    # Helper for local float coercion
    def parse_val(field_name):
        try:
            row = change_in_nav_df[change_in_nav_df['Field Name'].astype(str).str.strip() == field_name]
            if row.empty: return 0.0
            val_str = row['Field Value'].iloc[0]
            # Simple clean
            clean = str(val_str).replace(',', '').replace('$', '').strip()
            if clean.startswith('(') and clean.endswith(')'):
                clean = '-' + clean[1:-1]
            return float(clean)
        except Exception:
            return 0.0

    # 1. Calculate Return Metrics
    start_val = parse_val("Starting Value")
    end_val   = parse_val("Ending Value")
    flows     = parse_val("Deposits & Withdrawals")
    comms     = parse_val("Commissions")                # Negative Value
    
    basis = start_val + flows
    profit = end_val - basis
    
    # Cumulative Return %
    if basis != 0:
        ret_pct = profit / basis
    else:
        ret_pct = 0.0

    # 2. Extract Detailed Breakdown
    target_fields = [
        "Starting Value", 
        "Mark-to-Market", 
        "Deposits & Withdrawals", 
        "Dividends", 
        "Interest", 
        "Change in Interest Accruals", 
        "Commissions", 
        "Ending Value"
    ]
    
    breakdown = {}
    for field in target_fields:
        breakdown[field] = parse_val(field)
        
    return {
        'NAV': end_val,
        'Return': ret_pct,
        'Breakdown': breakdown
    }
    

def calculate_period_returns(daily_nav_df, report_date_str):
    """
    Calculates returns and returns a tuple: (results_dict, period_label_str)
    """
    results = {'1M': None, '3M': None, '6M': None, 'YTD': None, 'Period': None}
    period_label = "Period" # Default fallback
    
    if daily_nav_df.empty: 
        return results, period_label
    
    df = daily_nav_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    current_date = pd.to_datetime(report_date_str)
    
    # Filter
    df = df[df['date'] <= current_date]
    if df.empty: return results, period_label

    end_val = df.iloc[-1]['nav']
    start_file_val = df.iloc[0]['nav']
    
    # --- CREATE DYNAMIC LABEL ---
    start_d = df.iloc[0]['date']
    end_d = df.iloc[-1]['date']
    # Format: "07/30/2025 - 01/12/2026"
    period_label = f"{start_d.strftime('%m/%d/%Y')} - {end_d.strftime('%m/%d/%Y')}"

    # Helper
    def get_nav_at(target_date):
        subset = df[df['date'] <= target_date]
        if subset.empty: return None
        return subset.iloc[-1]['nav']

    d_1m = current_date - pd.DateOffset(months=1)
    d_3m = current_date - pd.DateOffset(months=3)
    d_6m = current_date - pd.DateOffset(months=6)
    d_ytd = datetime.datetime(current_date.year, 1, 1)

    def calc(start, end):
        if start and start != 0: return (end / start) - 1.0
        return None

    results['1M'] = calc(get_nav_at(d_1m), end_val)
    results['3M'] = calc(get_nav_at(d_3m), end_val)
    results['6M'] = calc(get_nav_at(d_6m), end_val)
    results['YTD'] = calc(get_nav_at(d_ytd), end_val)
    results['Period'] = calc(start_file_val, end_val)
    
    return results, period_label


def prepare_chart_data(daily_nav_df, benchmark_ticker='SPY'):
    """Aligns Portfolio NAV with Benchmark for plotting."""
    if daily_nav_df.empty: return pd.DataFrame()
    
    df = daily_nav_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    df['pct'] = df['nav'].pct_change().fillna(0)
    df['Portfolio'] = (1 + df['pct']).cumprod() - 1
    
    from yf_loader import fetch_benchmark_returns_yf
    start_d = df['date'].iloc[0].strftime('%Y-%m-%d')
    end_d = df['date'].iloc[-1].strftime('%Y-%m-%d')
    
    bench = fetch_benchmark_returns_yf([benchmark_ticker], start_date=start_d, end_date=end_d)
    
    if not bench.empty:
        merged = pd.merge(df, bench[benchmark_ticker], left_on='date', right_index=True, how='left')
        merged['b_pct'] = merged[benchmark_ticker].fillna(0)
        merged['S&P 500'] = (1 + merged['b_pct']).cumprod() - 1
        return merged[['date', 'Portfolio', 'S&P 500']]
        
    return df[['date', 'Portfolio']]