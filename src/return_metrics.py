import pandas as pd
import datetime

def get_cumulative_index(returns_df: pd.DataFrame, start_value: float = 100.0) -> pd.DataFrame:
    """Converts a Series/DataFrame of Percentage Returns into a Price Index."""
    if returns_df.empty: return pd.DataFrame()
    clean_returns = returns_df.fillna(0.0)
    growth_factors = (1 + clean_returns).cumprod()
    return start_value * growth_factors

def get_cumulative_return(series: pd.Series, window: str) -> float:
    """Calculates the total return over a specific window."""
    if series is None or series.empty: return 0.0
    series = series.dropna().sort_index()
    if not isinstance(series.index, pd.DatetimeIndex): return 0.0
        
    end_price = float(series.iloc[-1])
    end_date = series.index[-1]
    
    start_date = None
    window = window.upper()
    
    if window == "INCEPTION":
        start_price = float(series.iloc[0])
        return (end_price / start_price) - 1.0
    elif window == "YTD":
        start_date = pd.Timestamp(year=end_date.year, month=1, day=1)
    elif window.endswith('Y') and window[:-1].isdigit():
        years = int(window[:-1])
        start_date = end_date - pd.DateOffset(years=years)
    elif window.endswith('M') and window[:-1].isdigit():
        months = int(window[:-1])
        start_date = end_date - pd.DateOffset(months=months)
    else:
        start_price = float(series.iloc[0])
        return (end_price / start_price) - 1.0

    if start_date < series.index[0]: start_date = series.index[0]
    start_price = series.asof(start_date)
    
    if pd.isna(start_price) or start_price == 0: start_price = float(series.iloc[0])
    return (end_price / start_price) - 1.0

def calculate_nav_performance(change_in_nav_df: pd.DataFrame) -> dict:
    """Calculates the Official NAV Return based on the 'Change in NAV' section."""
    default_res = {'NAV': 0.0, 'Return': 0.0, 'Breakdown': {}}
    if change_in_nav_df is None or change_in_nav_df.empty: return default_res

    def parse_val(field_name):
        try:
            row = change_in_nav_df[change_in_nav_df['Field Name'].astype(str).str.strip() == field_name]
            if row.empty: return 0.0
            val_str = row['Field Value'].iloc[0]
            clean = str(val_str).replace(',', '').replace('$', '').strip()
            if clean.startswith('(') and clean.endswith(')'): clean = '-' + clean[1:-1]
            return float(clean)
        except: return 0.0

    start_val = parse_val("Starting Value")
    end_val   = parse_val("Ending Value")
    flows     = parse_val("Deposits & Withdrawals")
    
    basis = start_val + flows
    profit = end_val - basis
    ret_pct = profit / basis if basis != 0 else 0.0

    target_fields = ["Starting Value", "Mark-to-Market", "Deposits & Withdrawals", "Dividends", "Interest", "Change in Interest Accruals", "Commissions", "Ending Value"]
    breakdown = {field: parse_val(field) for field in target_fields}
        
    return {'NAV': end_val, 'Return': ret_pct, 'Breakdown': breakdown}
    
def calculate_period_returns(daily_nav_df, report_date_str):
    """
    Calculates returns for: Period (File Range), 1M, 3M, 6M, YTD, 1Y, 3Y, Inception.
    Returns: (results_dict, period_label_str)
    """
    results = {
        'Period': None, '1M': None, '3M': None, '6M': None, 
        'YTD': None, '1Y': None, '3Y': None, 'Inception': None
    }
    period_label = "Period"
    
    if daily_nav_df.empty: return results, period_label
    
    df = daily_nav_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    current_date = pd.to_datetime(report_date_str)
    
    # Filter to report date
    df = df[df['date'] <= current_date]
    if df.empty: return results, period_label

    end_val = df.iloc[-1]['nav']
    start_file_val = df.iloc[0]['nav']
    
    # --- 1. RESTORED: Dynamic "Q{num} {Year}" Label ---
    # We calculate which quarter the report_date falls into
    q_num = ((current_date.month - 1) // 3) + 1
    period_label = f"Q{q_num} {current_date.year}"

    # --- Helper Calculation ---
    def get_nav_at(target_date):
        if target_date < df.iloc[0]['date']: return df.iloc[0]['nav']
        subset = df[df['date'] <= target_date]
        if subset.empty: return None
        return subset.iloc[-1]['nav']

    def calc(start, end):
        if start and start != 0: return (end / start) - 1.0
        return None

    # Define Dates
    d_1m = current_date - pd.DateOffset(months=1)
    d_3m = current_date - pd.DateOffset(months=3)
    d_6m = current_date - pd.DateOffset(months=6)
    d_ytd = datetime.datetime(current_date.year, 1, 1)
    d_1y  = current_date - pd.DateOffset(years=1)
    d_3y  = current_date - pd.DateOffset(years=3)

    # --- Calculate Returns ---
    # Period = Full range of the provided CSV (Matches "Change in NAV" timeline)
    # But displayed as "Q{x} {Year}"
    results['Period']    = calc(start_file_val, end_val)
    
    results['1M']        = calc(get_nav_at(d_1m), end_val)
    results['3M']        = calc(get_nav_at(d_3m), end_val)
    results['6M']        = calc(get_nav_at(d_6m), end_val)
    results['YTD']       = calc(get_nav_at(d_ytd), end_val)
    results['1Y']        = calc(get_nav_at(d_1y), end_val)
    results['3Y']        = calc(get_nav_at(d_3y), end_val)
    results['Inception'] = calc(df.iloc[0]['nav'], end_val)
    
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
        
        if not merged.empty:
            merged.iloc[0, merged.columns.get_loc(benchmark_ticker)] = 0.0  # Ensures both lines start at the exact same point on the Y-axis

        merged['b_pct'] = merged[benchmark_ticker].fillna(0)
        merged['S&P 500'] = (1 + merged['b_pct']).cumprod() - 1
        
        return merged[['date', 'Portfolio', 'S&P 500']]
        
    return df[['date', 'Portfolio']]