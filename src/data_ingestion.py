import pandas as pd
import yfinance as yf
from src.security_master import SecurityMaster

def fetch_market_data_yf(master_path='data/security_master.csv', start_date="2015-01-01"):
    """
    Main ingestion function.
    1. Reads the Security Master to find out WHAT to fetch.
    2. Downloads price history (currently via Yahoo Finance).
    3. Returns a clean DataFrame of Adjusted Close prices.
    """
    if not pd.io.common.file_exists(master_path):
        print(f"Error: Master file not found at {master_path}")
        return pd.DataFrame()
    
    print("\n--- Starting Data Ingestion ---")
    
    # 1. Initialize Security Master
    # We need this to get the list of Assets + Benchmarks (SPY, AGG, etc.)
    sec_master = SecurityMaster(master_path)
    
    # 2. Get the master list of tickers
    # This uses the function we wrote to grab 'ticker', 'benchmark_primary', etc.
    tickers_to_download = sec_master.get_all_downloaded_tickers()
    
    if not tickers_to_download:
        print("Error: No tickers found in Security Master.")
        return pd.DataFrame()
    
    print(f"Fetching data for {len(tickers_to_download)} symbols...")
    print(f"Tickers: {sorted(tickers_to_download)}")
    
    # 3. Download Data
    # NOTE: When WRDS is active, you would replace this block with your 
    # 'wrds_connection.raw_sql(...)' code.
    try:
        # threads=True speeds up the download significantly
        yf_data = yf.download(
            tickers_to_download, 
            start=start_date, 
            progress=False, 
            threads=True
        )['Close']
        
        if yf_data.empty:
            print("Warning: Download returned empty DataFrame.")
            return pd.DataFrame()
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

    # Drop columns that failed to download entirely (all NaNs)
    yf_data.dropna(axis=1, how='all', inplace=True)
    
    # Forward fill missing data (e.g., if a stock didn't trade on a holiday where others did)
    # This is crucial for 'Total Portfolio' calculations so we don't drop rows unnecessarily
    yf_data.ffill(inplace=True)
    
    print("Download Complete.")
    print(f"Data Shape: {yf_data.shape} (Dates, Tickers)")
    
    return yf_data

if __name__ == "__main__":
    # Quick test to see if it works
    df = fetch_market_data_yf()
    print(df.tail())
    
