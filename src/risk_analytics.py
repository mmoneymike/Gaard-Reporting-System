import pandas as pd
import numpy as np
import yfinance as yf
from scipy import stats


def get_live_risk_free_rate(default_rate=0.04):
    """
    Fetches the current yield of the 13-Week Treasury Bill (^IRX) from Yahoo.
    Returns a float (e.g., 4.25% -> 0.0425).
    """
    try:
        # ^IRX is the Yahoo ticker for 13-Week Treasury Bill Yield
        ticker = yf.Ticker("^IRX")
        
        # Get the most recent closing price
        # Note: Yahoo provides this as an index (e.g., 4.25 means 4.25%)
        hist = ticker.history(period="5d")
        
        if not hist.empty:
            latest_yield = hist['Close'].iloc[-1]
            rf_decimal = latest_yield / 100.0
            print(f"   > Fetched Live Risk-Free Rate: {rf_decimal:.2%}")
            return rf_decimal
            
    except Exception as e:
        print(f"   > Warning: Could not fetch live Risk-Free rate ({e}). Using default.")
    
    # Fallback
    return default_rate


def fetch_history_for_risk(tickers, start_date=None):
    """Fetches historical adjusted close prices for risk calc."""
    if not tickers: return pd.DataFrame()
    
    unique_tickers = list(set([t.upper() for t in tickers]))
    
    # Fetch Data (Auto Adjust = True handles splits/divs)
    try:
        data = yf.download(unique_tickers, start=start_date, progress=False, auto_adjust=True)
    except Exception as e:
        print(f"   > Error fetching Yahoo data: {e}")
        return pd.DataFrame()
    
    # Handle MultiIndex vs Single Index
    if isinstance(data.columns, pd.MultiIndex):
        if 'Close' in data.columns.levels[0]:
            prices = data['Close']
        else:
            prices = data
    else:
        if 'Close' in data.columns:
            prices = data[['Close']]
            prices.columns = unique_tickers
        else:
            prices = data

    # Calculate Daily Returns
    returns = prices.pct_change(fill_method=None).dropna(how='all')
    return returns

def calculate_portfolio_risk(holdings_df, benchmark_ticker='SPY', lookback_years=1, rf_rate=None):
    """
    Calculates risk metrics based on CURRENT holdings weights.
    Defaults to 1 Year lookback to match standard 'Trailing 1Y' reports.
    """
    
    # Live Risk Free Rate
    if rf_rate is None:
        rf_rate = get_live_risk_free_rate(default_rate=0.04)
        
    # 1. Filter Valid Holdings (Exclude Cash placeholders if they don't have tickers)
    # Note: If you hold 'VGSH' or 'BIL', those are valid tickers. 
    # 'CASH_BAL' usually has no ticker, so it gets skipped (treated as 0 vol).
    valid_holdings = holdings_df[
        (holdings_df['asset_class'] != 'Other') &
        (holdings_df['weight'] > 0)
    ].copy()
    
    # Get list of tickers to fetch
    tickers = [t for t in valid_holdings['ticker'].unique() if isinstance(t, str) and len(t) < 10]
    
    if benchmark_ticker not in tickers:
        tickers.append(benchmark_ticker)
        
    # Set Lookback Window (Default 1 Year)
    start_date = (pd.Timestamp.now() - pd.DateOffset(years=lookback_years)).strftime('%Y-%m-%d')
    
    # 2. Fetch History
    print(f"   > Fetching 1Y history for Risk Metrics ({len(tickers)} tickers)...")
    returns_df = fetch_history_for_risk(tickers, start_date=start_date)
    
    if returns_df.empty or benchmark_ticker not in returns_df.columns:
        print("   > Warning: Insufficient history for risk calculations.")
        return {}

    # 3. Build Synthetic Portfolio Return Series
    # Sum(Weight * Daily_Return) for each day
    portfolio_daily_ret = pd.Series(0.0, index=returns_df.index)
    
    for _, row in valid_holdings.iterrows():
        t = row['ticker']
        w = row['weight']
        if t in returns_df.columns:
            portfolio_daily_ret = portfolio_daily_ret.add(returns_df[t] * w, fill_value=0)
            
    # Align with Benchmark
    combined = pd.concat([portfolio_daily_ret, returns_df[benchmark_ticker]], axis=1).dropna()
    combined.columns = ['Portfolio', 'Benchmark']
    
    if combined.empty: return {}

    # 4. Calculate Metrics
    
    # A. Volatility (Standard Deviation)
    daily_std = combined['Portfolio'].std()
    ann_vol = daily_std * np.sqrt(252)  # Annualized
    
    # B. Beta & R-Squared
    slope, intercept, r_value, p_value, std_err = stats.linregress(
        combined['Benchmark'], 
        combined['Portfolio']
    )
    beta = slope
    r_squared = r_value ** 2
    
    # C. Sharpe Ratio (Annualized)
    # Sharpe = (Ann_Ret - Rf) / Ann_Vol
    avg_daily_ret = combined['Portfolio'].mean()
    ann_ret = avg_daily_ret * 252
    
    if ann_vol > 0:
        sharpe = (ann_ret - rf_rate) / ann_vol
    else:
        sharpe = 0.0

    return {
        'Beta': beta,
        'R2': r_squared,
        'Daily Standard Deviation': daily_std,  # This should match your 0.11%
        'Annual Volatility': ann_vol,
        'Sharpe Ratio': sharpe,
        'RiskFreeRateUsed': rf_rate
    }