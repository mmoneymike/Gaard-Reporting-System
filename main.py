import pandas as pd
from src.security_master import *
from src.data_ingestion import *
from src.portfolio_analytics import *

# from src.analytics import ... (We will add this later)

def run_pipeline():
    # 1. Initialize the Brain (to help us label our holdings)
    sec_master = SecurityMaster('data/security_master.csv')
    # Hardcode the earliest data to be available
    START_DATE = "2016-01-01"
    print("--- 1. Pipeline Initialized ---")
    
    # 2. Load Holdings
    try:
        holdings = pd.read_csv('data/holdings.csv')
        print("\n--- 2. Holdings Loaded ---")
    except FileNotFoundError:
        print("Error: data/holdings.csv not found. Did you run setup_data.py?")
        return

    # 3. Enrich Holdings with Metadata (The Sorting Mechanism)
    # This creates the "Buckets" we need for the report (e.g., grouping by 'Equities')
    print("Enriching holdings with metadata...")
    holdings['asset_class'] = holdings['ticker'].apply(sec_master.get_asset_class)
    
    # Note: This returns a LIST of benchmarks (e.g., ['SPY', 'ACWI'])
    holdings['benchmarks'] = holdings['ticker'].apply(sec_master.get_benchmarks)
    
    print("\n--- 3. Portfolio Structure (Snapshot) ---")
    print(holdings.head())
    
    # 4. Fetch Market Data
    # This function automatically looks at security_master.csv, finds every 
    # asset and benchmark we need, and downloads them.
    print(f"\n--- 4. Fetching Market Data (Start: {START_DATE}) ---")
    prices_df = fetch_market_data_yf(master_path='data/security_master.csv',start_date=START_DATE)
    
    if prices_df.empty:
        print("Critical Error: No price data fetched. Check your internet or tickers.")
        return

    print(f"Successfully loaded price history for {len(prices_df.columns)} symbols.")
    print(f"Date Range: {prices_df.index[0].date()} to {prices_df.index[-1].date()}")
    
    # 5. Pass to Analytics
    print("\n--- 5. Calculating Portfolio History ---")
    aggregator = PortfolioAggregator(holdings, prices_df)
    bucket_indices = aggregator.calculate_bucket_indices()
    benchmark_indices = aggregator.get_aligned_benchmarks(sec_master.get_unique_benchmarks())
    
    all_prices = pd.concat([bucket_indices, benchmark_indices], axis=1)
    
    print("n/--- 6. Performance Report ---")
    report_order = ['Portfolio']
    
    # Loop through every Asset Class you actually own
    unique_classes = holdings['asset_class'].unique()
    
    for asset_class in unique_classes:
        if asset_class == 'Unclassified': continue
        
        # Add the Asset Class Bucket itself (e.g., "Equities")
        report_order.append(asset_class)
        
        # Find the specific benchmarks associated with this class
        class_holdings = holdings[holdings['asset_class'] == asset_class]
        
        associated_benchmarks = []
        for ticker in class_holdings['ticker']:
            # Ask Security Master: "What benchmarks does this ticker use?"
            # This returns a list like ['SPY'] or ['AGG', 'JNK']
            b_list = sec_master.get_benchmarks(ticker)
            associated_benchmarks.extend(b_list)
        
        # Deduplicate (If 5 stocks use SPY, we only want SPY once)
        unique_benchmarks = list(dict.fromkeys(associated_benchmarks))
        
        # Add those benchmarks to the list immediately after the Asset Class
        report_order.extend(unique_benchmarks)

    # Filter & Clean: Ensure we only ask for columns that actually exist in our data 
    final_cols = []
    seen = set()
    
    for col in report_order:
        if col in all_prices.columns and col not in seen:
            final_cols.append(col)
            seen.add(col)
            
    # DEFINE WINDOWS FOR RETURNS
    windows = ['1M', '3M', '6M','YTD', '1Y', 'INCEPTION']
    report_data = {}
    
    for col in final_cols:
        series = all_prices[col]
        col_stats = {}
        for win in windows:
            try:
                val = get_cumulative_return(series, win)
                col_stats[win] = val
            except Exception:
                col_stats[win] = 0.0
        report_data[col] = col_stats
        
    # 4. Print
    report_df = pd.DataFrame(report_data).T
    pd.options.display.float_format = '{:.2%}'.format
    print(report_df)
    

if __name__ == "__main__":
    run_pipeline()