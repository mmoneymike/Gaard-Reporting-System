import yfinance as yf
import pandas as pd

def fetch_benchmark_returns_yf(tickers, start_date=None, end_date=None):
    """
    Fetches Daily Total Returns (Adjusted Close % Change) from Yahoo Finance.
    Replicates the output format of the WRDS loader.
    """
    if not tickers:
        return pd.DataFrame()
    
    # Deduplicate and clean tickers
    unique_tickers = list(set([t.upper() for t in tickers]))
    print(f"Fetching data for {len(unique_tickers)} symbols via Yahoo Finance...")
    
    try:
        # download() with auto_adjust=True gives us Total Return (Divs + Splits included)
        data = yf.download(
            unique_tickers, 
            start=start_date, 
            progress=False, 
            auto_adjust=True
        )
        
        # Handle cases where yfinance returns MultiIndex columns
        if isinstance(data.columns, pd.MultiIndex):
            # If we requested multiple tickers, 'Close' is level 0
            if 'Close' in data.columns.levels[0]:
                prices = data['Close']
            else:
                # Fallback
                prices = data
        else:
            # Single ticker request returns a simple DataFrame
            # We need to ensure it's a DataFrame, not a Series
            if 'Close' in data.columns:
                prices = data[['Close']]
                prices.columns = unique_tickers # Rename col to ticker
            else:
                return pd.DataFrame()

        # Convert Prices -> Returns
        returns = prices.pct_change(fill_method=None)
        
        # Cut off data strictly at the report date
        if end_date:
            returns = returns.loc[:end_date]
            
        # Yahoo often returns data slightly before start_date if using 'max', 
        # but here we are good. We just drop the first NaN row.
        return returns.dropna(how='all')

    except Exception as e:
        print(f"Yahoo Finance Error: {e}")
        return pd.DataFrame()


def fetch_security_names_yf(tickers):
    """
    Fetches the official long names for a list of tickers.
    Used for Auto-Classification.
    Note: Yahoo .info is slower than batch price download, so this loops.
    """
    name_map = {}
    print(f"Auto-Classifying {len(tickers)} tickers via Yahoo (fetching metadata)...")
    
    for t in tickers:
        try:
            # yfinance Ticker object
            stock = yf.Ticker(t)
            # .info triggers an API call
            info = stock.info 
            
            # Try to find the best name available
            official_name = info.get('longName') or info.get('shortName')
            
            if official_name:
                name_map[t] = official_name.upper()
            else:
                name_map[t] = t # Fallback to ticker if no name found
                
        except Exception:
            # If Yahoo fails for one ticker, just skip it or use ticker as name
            name_map[t] = t
            
    return name_map