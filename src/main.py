import pandas as pd
import os

# --- 1. PROJECT IMPORTS ---
from statement_ingestion import get_portfolio_holdings, auto_classify_asset
from yf_loader import*
from wrds_loader import*
from report_metrics import get_cumulative_return, get_cumulative_index



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

# ==========================================
# CONFIGURATION
# ==========================================
BENCHMARK_CONFIG = {
    'U.S. Equities':        ['SPY', 'IWV'],  
    'International Equities': ['ACWI', 'VXUS'], 
    'Fixed Income':         ['AGG', 'BND'],
    'Alternative Assets':   ['VNQ', 'GLD'],
    'Unclassified':         []
}

def run_pipeline():
    # --- PATH SETUP ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    IBKR_FILE = os.path.join(project_root, "data", "U21244041_20250730_20260112.csv")
    BENCHMARK_START_FIXED = "2025-07-30" 

    print(f"--- 1. Ingesting Portfolio (Internal) ---")
    if not os.path.exists(IBKR_FILE):
        print(f"CRITICAL ERROR: Could not find file at: {IBKR_FILE}")
        return

    try:
        # 1. Load Data (Plus new NAV and Orphans)
        holdings, report_date, true_nav, orphaned_divs = get_portfolio_holdings(IBKR_FILE)
        
        print(f"   > File Date: {report_date}")
        print(f"   > File Net Asset Value: ${true_nav:,.2f}")
        print(f"   > Orphaned Dividends (e.g. ICSH): ${orphaned_divs:,.2f}")

        # === 2. AUTO-CLASSIFICATION (PRIMARY) ===
        print("   > Running Auto-Classification on ALL positions...")
        
        # Fetch Names for EVERYONE
        all_tickers = holdings['ticker'].unique().tolist()
        name_map = fetch_security_names_yf(all_tickers)
        
        holdings['official_name'] = holdings['ticker'].map(name_map).fillna('')
        
        # Auto classifcation function. See statement_ingestion.py for function specifics
        holdings['asset_class'] = holdings.apply(
            lambda row: auto_classify_asset(row['ticker'], row['official_name']), 
            axis=1
        )
        # ========================================

        # === 3. THE CASH PLUG ===
        # Sum of current positions
        positions_val = holdings['raw_value'].sum()
        
        # Difference = Cash Balance (Sweeps, etc)
        # If true_nav is 0 (parsing failed), fallback to 0 cash
        cash_plug_val = true_nav - positions_val if true_nav > 0 else 0.0
        
        if cash_plug_val > 1.0: # Ignore tiny rounding errors
            print(f"   > Implied Cash Balance found: ${cash_plug_val:,.2f}")
            
            # Create Synthetic Cash Row
            cash_row = {
                'ticker': 'CASH_BAL',
                'official_name': 'Cash & Sweeps (Reconciled)',
                'asset_class': 'Cash',
                'avg_cost': cash_plug_val, # Assume basis = value for pure cash
                'raw_value': cash_plug_val,
                'total_dividends': orphaned_divs, # ADD ORPHANED DIVS HERE (Income from sold cash-equivalents)
                'cumulative_return': 0.0 # Will be calculated below
            }
            # Append to holdings
            holdings = pd.concat([holdings, pd.DataFrame([cash_row])], ignore_index=True)

        # Recalculate weights now that Cash is included
        total_mv = holdings['raw_value'].sum()
        holdings['weight'] = holdings['raw_value'] / total_mv

        # Recalculate Cash Return with Orphans
        # (Cash Value + Orphans) / Cash Basis - 1
        # This gives "Return" on the cash bucket
        cash_mask = holdings['ticker'] == 'CASH_BAL'
        if cash_mask.any():
            # If we added dividends to the plug, recalculate that row's return
            c_cost = holdings.loc[cash_mask, 'avg_cost'].iloc[0]
            c_val = holdings.loc[cash_mask, 'raw_value'].iloc[0]
            c_div = holdings.loc[cash_mask, 'total_dividends'].iloc[0]
            if c_cost > 0:
                holdings.loc[cash_mask, 'cumulative_return'] = (c_val + c_div) / c_cost - 1.0

    except ValueError as e:
        print(f"Error unpacking data: {e}")
        return

    # --- 4. MARKET DATA (Benchmarks) ---
    print("\n--- 2. Fetching Benchmark Data (Yahoo) ---")
    all_benchmarks = [t for sublist in BENCHMARK_CONFIG.values() for t in sublist]
    unique_benchmarks = list(set(all_benchmarks))
    
    bench_growth = pd.DataFrame()
    try:
        bench_returns = fetch_benchmark_returns_yf(unique_benchmarks, start_date=BENCHMARK_START_FIXED)
        if not bench_returns.empty:
            bench_growth = get_cumulative_index(bench_returns, start_value=100)
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
    
    
    # --- 5. REPORT GENERATION ---
    print("\n" + "="*80)
    print(f" PERFORMANCE REPORT (As of {report_date})")
    print("="*80)
    
    # A. TOTAL PORTFOLIO RETURN
    grand_cost = holdings['avg_cost'].sum()
    grand_val = holdings['raw_value'].sum()
    grand_divs = holdings['total_dividends'].sum()
    
    total_ret = 0.0
    if grand_cost != 0:
        total_ret = (grand_val + grand_divs) / grand_cost - 1.0
        
    print(f" TOTAL PORTFOLIO RETURN: {total_ret:.2%}  (Basis: ${grand_cost:,.0f} -> Value: ${grand_val:,.0f} + Divs: ${grand_divs:,.0f})")
    print("="*80)
    
    # B. ASSET CLASS BREAKDOWN
    report_rows = []
    my_buckets = holdings['asset_class'].unique()
    
    sorted_buckets = [b for b in BENCHMARK_CONFIG if b in my_buckets]
    for b in my_buckets:
        if b not in sorted_buckets: sorted_buckets.append(b)

    for bucket in sorted_buckets:
        bucket_data = holdings[holdings['asset_class'] == bucket]
        
        # Totals
        b_mv = bucket_data['raw_value'].sum()
        b_cost = bucket_data['avg_cost'].sum()
        b_divs = bucket_data['total_dividends'].sum()
        
        if b_cost != 0:
            my_return = (b_mv + b_divs) / b_cost - 1
        else:
            my_return = 0.0
        
        # Show Cash Amount
        if bucket == 'Cash':
            display_return = f"${b_mv:,.0f}"
        else:
            display_return = my_return
        
        # Display Each Asset-Class's Returns
        report_rows.append({
            'Asset Class': f"** {bucket} **", 
            'Type': 'Portfolio',
            'Return': display_return
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

    # C. DETAILED HOLDINGS
    print("\n" + "="*80)
    print(" DETAILED HOLDINGS BREAKDOWN")
    print("="*80)

    for bucket in sorted_buckets:
        print(f"\n[{bucket}]")
        
        subset = holdings[holdings['asset_class'] == bucket].copy()
        view = subset[['ticker', 'official_name', 'avg_cost', 'raw_value', 'weight', 'cumulative_return']].copy()
        view = view.sort_values('weight', ascending=False)
        
        view['avg_cost'] = view['avg_cost'].apply(lambda x: f"${x:,.0f}")
        view['raw_value'] = view['raw_value'].apply(lambda x: f"${x:,.0f}")
        view['weight'] = view['weight'].apply(lambda x: f"{x:.2%}")
        view['cumulative_return'] = view['cumulative_return'].apply(lambda x: f"{x:.2%}")
        
        view.columns = ['Ticker', 'Name', 'Cost Basis', 'Market Value', 'Weight', 'Return']
        print(view.to_string(index=False))

    print("\n" + "="*80)

if __name__ == "__main__":
    run_pipeline()