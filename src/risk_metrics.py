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
        # ^IRX: Yahoo ticker for 13-Week Treasury Bill Yield
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


def calculate_portfolio_risk(daily_nav_df, benchmark_series, rf_rate=None):
    """
    Calculates SPECIFIC risk metrics: Idiosyncratic Risk and Factor Coefficients.
    Uses the full date range of the provided data (since inception).
    """
    metrics = {
        'Idiosyncratic Risk': 0.0,
        'R-Squared (vs Bench)': 0.0,
        'Beta: Size (IWM)': 0.0, 
        'Beta: Value (IWD)': 0.0, 
        'Beta: Quality (QUAL)': 0.0, 
        'Beta: Momentum (MTUM)': 0.0
    }
    
    if daily_nav_df.empty or benchmark_series.empty:
        return metrics
    
     # Live Risk Free Rate
    if rf_rate is None:
        rf_rate = get_live_risk_free_rate(default_rate=0.04)
            
    # 1. Prepare Portfolio Returns
    df = daily_nav_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date')
    
    port_returns = df['nav'].pct_change().dropna()
    
    # 2. Align with Main Benchmark (for Idiosyncratic Risk)
    combined = pd.concat([port_returns, benchmark_series], axis=1, join='inner').dropna()
    
    if not combined.empty:
        combined.columns = ['Portfolio', 'Benchmark']
        y = combined['Portfolio']
        x = combined['Benchmark']
        
        # --- A. IDIOSYNCRATIC RISK ---
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
        predicted = intercept + slope * x
        residuals = y - predicted
        
        # Annualize Std Dev of Residuals
        metrics['Idiosyncratic Risk'] = residuals.std() * np.sqrt(252)
        metrics['R-Squared (vs Bench)'] = r_value**2

    # --- B. FACTOR COEFFICIENTS (BETAS) ---
    factors = {
        'Size (IWM)': 'IWM',
        'Value (IWD)': 'IWD',
        'Quality (QUAL)': 'QUAL',
        'Momentum (MTUM)': 'MTUM'
    }
    
    try:
        # Fetch Factor Data for the exact portfolio period (Filtered)
        if not port_returns.empty:
            start_d = port_returns.index.min().strftime('%Y-%m-%d')
            end_d = (port_returns.index.max() + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            
            tickers = list(factors.values())
            f_data = yf.download(tickers, start=start_d, end=end_d, progress=False)['Close']
            
            if not f_data.empty:
                f_rets = f_data.pct_change().dropna()
                
                # Align Portfolio with Factors
                aligned_factors = pd.concat([port_returns, f_rets], axis=1, join='inner').dropna()
                aligned_factors = aligned_factors.dropna()
                
                if not aligned_factors.empty:
                    y_port = aligned_factors.iloc[:, 0] # Portfolio is first column
                    
                    for label, ticker in factors.items():
                        if ticker in aligned_factors.columns:
                            x_factor = aligned_factors[ticker]
                            if len(x_factor) > 1:
                                # Univariate Regression
                                beta, _, _, _, _ = stats.linregress(x_factor, y_port)
                                metrics[f'Beta: {label}'] = beta
                            
    except Exception as e:
        print(f"   > Warning: Factor calculation failed: {e}")

    return metrics


def calculate_descriptive_risk_stats(daily_nav_df, rf_rate=None):
    """Recalculates IBKR-style descriptive risk metrics from a raw daily NAV series.
    
    Used for aggregate reports where IBKR per-account values cannot be combined directly.
    Expects daily_nav_df with columns 'date' and 'nav'.
    """
    result = {}
    
    if daily_nav_df.empty or 'nav' not in daily_nav_df.columns:
        return result
    
    if rf_rate is None:
        rf_rate = get_live_risk_free_rate(default_rate=0.04)
    
    df = daily_nav_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    
    nav = df['nav'].values
    daily_returns = pd.Series(nav).pct_change().dropna()
    
    if len(daily_returns) < 2:
        return result
    
    result['Ending VAMI'] = (nav[-1] / nav[0]) * 1000
    
    mean_daily = daily_returns.mean()
    result['Mean Return'] = mean_daily * 100
    
    std_daily = daily_returns.std()
    result['Standard Deviation'] = std_daily * np.sqrt(252) * 100
    
    negative_returns = daily_returns[daily_returns < 0]
    if len(negative_returns) > 0:
        downside_dev = negative_returns.std() * np.sqrt(252) * 100
    else:
        downside_dev = 0.0
    result['Downside Deviation'] = downside_dev
    
    # Max Drawdown and Peak-To-Valley
    cumulative = (1 + daily_returns).cumprod()
    running_max = cumulative.cummax()
    drawdown = (cumulative / running_max) - 1
    max_dd = drawdown.min()
    result['Max Drawdown'] = max_dd * 100
    
    if max_dd < 0:
        trough_idx = drawdown.idxmin()
        peak_idx = cumulative.iloc[:trough_idx + 1].idxmax()
        peak_date = df['date'].iloc[peak_idx + 1] if (peak_idx + 1) < len(df) else df['date'].iloc[0]
        trough_date = df['date'].iloc[trough_idx + 1] if (trough_idx + 1) < len(df) else df['date'].iloc[-1]
        result['Peak-To-Valley'] = (trough_date - peak_date).days
    else:
        result['Peak-To-Valley'] = None
    
    if max_dd < 0:
        trough_idx = drawdown.idxmin()
        post_trough = drawdown.iloc[trough_idx:]
        recovered = post_trough[post_trough >= 0]
        result['Recovery'] = 'Yes' if len(recovered) > 0 else 'No'
    else:
        result['Recovery'] = 'N/A'
    
    # Sharpe Ratio (annualized)
    rf_daily = rf_rate / 252
    if std_daily > 0:
        result['Sharpe Ratio'] = ((mean_daily - rf_daily) / std_daily) * np.sqrt(252)
    else:
        result['Sharpe Ratio'] = 0.0
    
    # Sortino Ratio (annualized)
    downside_std_daily = negative_returns.std() if len(negative_returns) > 0 else 0.0
    if downside_std_daily > 0:
        result['Sortino Ratio'] = ((mean_daily - rf_daily) / downside_std_daily) * np.sqrt(252)
    else:
        result['Sortino Ratio'] = 0.0
    
    result['Positive Periods'] = str(int((daily_returns > 0).sum()))
    result['Negative Periods'] = str(int((daily_returns < 0).sum()))
    
    return result