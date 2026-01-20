import pandas as pd

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