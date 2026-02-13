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


def calculate_portfolio_risk(daily_nav_df, benchmark_series, lookback_years=None, rf_rate=None):
    """
    Calculates SPECIFIC risk metrics: Idiosyncratic Risk and Factor Coefficients.
    If lookback_years is provided (e.g., 3), it filters data to the most recent N years.
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
    
    # --- FILTER BY LOOKBACK HORIZON ---
    if lookback_years is not None and lookback_years > 0:
        end_date = df.index.max()
        start_date = end_date - pd.DateOffset(years=lookback_years)
        df = df[df.index >= start_date]
        
        # Also filter benchmark series to match start date (for efficiency)
        benchmark_series = benchmark_series[benchmark_series.index >= start_date]
    
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