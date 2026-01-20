import pandas as pd
import os

# --- 1. IMPORTS ---
from statement_ingestion import get_portfolio_holdings, auto_classify_asset
from yf_loader import*
from wrds_loader import*
from report_metrics import get_cumulative_return, get_cumulative_index

# ==========================================
# CONFIGURATION
# ==========================================
BENCHMARK_CONFIG = {
    'U.S. Equities':        ['SPY', 'IWV'],  
    'International Equities': ['ACWI', 'VXUS'], 
    'Fixed Income':         ['AGG', 'BND'],
    'Alternative Assets':   ['VNQ', 'GLD'],
    'Cash':                 ['BIL'],
    'Unclassified':         []
}

def run_pipeline():
    # --- PATH SETUP ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    IBKR_FILE = os.path.join(project_root, "data", "U21244041_20250730_20260112.csv")
    
    BENCHMARK_START_FIXED = "2025-07-30" # ISSUE: returning no data from 2025.

    # --- 1. INGESTION ---
    print(f"--- 1. Ingesting Portfolio (Internal) ---")
    if not os.path.exists(IBKR_FILE):
        print(f"CRITICAL ERROR: Could not find file at: {IBKR_FILE}")
        return

    try:
        holdings, report_date = get_portfolio_holdings(IBKR_FILE)
        
        # === 2. HYBRID CLASSIFICATION (With Reporting) ===
        print("   > Running Hybrid Classification (IBKR -> Yahoo Fallback)...")
        
        # Identify "Unclassified" positions
        unclassified_mask = holdings['asset_class'] == 'Unclassified'
        unknown_tickers = holdings.loc[unclassified_mask, 'ticker'].unique().tolist()
        
        if unknown_tickers:
            print(f"   > Found {len(unknown_tickers)} unclassified positions. Fetching details...")
            
            # Fetch Names
            name_map = fetch_security_names_yf(unknown_tickers)
            
            # Save Official Name to DF
            # We map ONLY the unknown ones to avoid overwriting existing logic if we wanted, 
            # but mapping all is fine too.
            holdings.loc[unclassified_mask, 'official_name'] = holdings.loc[unclassified_mask, 'ticker'].map(name_map)
            
            # Apply Logic
            def refine_bucket(row):
                if row['asset_class'] != 'Unclassified':
                    return row['asset_class']
                return auto_classify_asset(row['ticker'], row.get('official_name', ''))

            # Update Asset Class
            holdings['asset_class'] = holdings.apply(refine_bucket, axis=1)
            
            # --- NEW: PRINT THE CHANGE LOG ---
            print(f"\n   >>> RE-CLASSIFICATION REPORT <<<")
            changed_df = holdings[holdings['ticker'].isin(unknown_tickers)][['ticker', 'official_name', 'asset_class']].copy()
            changed_df.columns = ['Ticker', 'Security Name', 'New Asset Class']
            print(changed_df.to_string(index=False))
            print("-" * 50)
            # ---------------------------------
            
        else:
            print("   > All positions were already classified by IBKR Models.")
        # =======================================

        print(f"Statement Date: {report_date}")
        print(f"Successfully loaded {len(holdings)} positions.")
        
    except ValueError as e:
        print(f"Error unpacking data: {e}")
        return

    # --- 2. MARKET DATA ---
    print("\n--- 2. Fetching Benchmark Data (Yahoo) ---")
    all_benchmarks = [t for sublist in BENCHMARK_CONFIG.values() for t in sublist]
    unique_benchmarks = list(set(all_benchmarks))
    
    bench_growth = pd.DataFrame()
    
    try:
        # NO CONNECTION OBJECT NEEDED FOR YAHOO
        bench_returns = fetch_benchmark_returns_yf(unique_benchmarks, start_date=BENCHMARK_START_FIXED)
        
        if not bench_returns.empty:
            bench_growth = get_cumulative_index(bench_returns, start_value=100)
        else:
            print("Warning: No data returned from Yahoo.")
    except Exception as e:
        print(f"Yahoo Connection Error: {e}")
        
    # print("\n--- 2. Fetching Benchmark Data (WRDS) ---")
    # all_benchmarks = [t for sublist in BENCHMARK_CONFIG.values() for t in sublist]
    # unique_benchmarks = list(set(all_benchmarks))
    
    # bench_growth = pd.DataFrame()
    
    # try:
    #     db = get_wrds_connection()
    #     bench_returns = fetch_benchmark_returns_wrds(db, unique_benchmarks, start_date=BENCHMARK_START_FIXED)
    #     if not bench_returns.empty:
    #         bench_growth = get_cumulative_index(bench_returns, start_value=100)
    #     else:
    #         print("Warning: No data returned from WRDS.")
    # except Exception as e:
    #     print(f"WRDS Connection Error: {e}")
    #     print("Proceeding with Portfolio data only...")

    # --- 3. SUMMARY REPORT ---
    print("\n" + "="*80)
    print(f" PERFORMANCE REPORT (As of {report_date} since {BENCHMARK_START_FIXED})")
    print("="*80)
    
    report_rows = []
    if not holdings.empty:
        my_buckets = holdings['asset_class'].unique()
    else:
        my_buckets = []
    
    sorted_buckets = [b for b in BENCHMARK_CONFIG if b in my_buckets]
    for b in my_buckets:
        if b not in sorted_buckets:
            sorted_buckets.append(b)

    for bucket in sorted_buckets:
        bucket_data = holdings[holdings['asset_class'] == bucket]
        
        # Bucket Totals
        total_mv = bucket_data['raw_value'].sum()
        total_cost = bucket_data['avg_cost'].sum()
        total_divs = bucket_data['total_dividends'].sum()
        
        if total_cost != 0:
            my_return = (total_mv + total_divs) / total_cost - 1
        else:
            my_return = 0.0
            
        report_rows.append({
            'Asset Class': f"** {bucket} **", 
            'Type': 'Portfolio',
            'Return': my_return
        })
        
        # Benchmarks
        targets = BENCHMARK_CONFIG.get(bucket, [])
        for b_ticker in targets:
            b_val = 0.0
            if not bench_growth.empty and b_ticker in bench_growth.columns:
                b_val = get_cumulative_return(bench_growth[b_ticker], 'INCEPTION')
            
            report_rows.append({
                'Asset Class': f"   {b_ticker}", 
                'Type': 'Benchmark',
                'Return': b_val
            })

    if report_rows:
        summary_df = pd.DataFrame(report_rows)
        summary_df['Return'] = summary_df['Return'].apply(lambda x: f"{x:.2%}" if isinstance(x, (int, float)) else x)
        print(summary_df.to_string(index=False))
    else:
        print("No holdings found.")

    # --- 4. DETAILED HOLDINGS BREAKDOWN ---
    print("\n" + "="*80)
    print(" DETAILED HOLDINGS BREAKDOWN")
    print("="*80)

    for bucket in sorted_buckets:
        print(f"\n[{bucket}]")
        
        subset = holdings[holdings['asset_class'] == bucket].copy()
        
        # ADDED 'avg_cost' to the view here
        view = subset[['ticker', 'avg_cost', 'raw_value', 'weight', 'cumulative_return']].copy()
        
        # Sort by Weight
        view = view.sort_values('weight', ascending=False)
        
        # Formatting
        view['avg_cost'] = view['avg_cost'].apply(lambda x: f"${x:,.0f}")
        view['raw_value'] = view['raw_value'].apply(lambda x: f"${x:,.0f}")
        view['weight'] = view['weight'].apply(lambda x: f"{x:.2%}")
        view['cumulative_return'] = view['cumulative_return'].apply(lambda x: f"{x:.2%}")
        
        # Rename columns for the final report
        view.columns = ['Ticker', 'Cost Basis', 'Market Value', 'Weight', 'Return']
        
        print(view.to_string(index=False))

    print("\n" + "="*80)

if __name__ == "__main__":
    run_pipeline()