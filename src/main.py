import pandas as pd
import os
import datetime

# --- IMPORTS ---
from statement_ingestion import get_portfolio_holdings, auto_classify_asset
from yf_loader import fetch_benchmark_returns_yf, fetch_security_names_yf
from report_metrics import get_cumulative_return, get_cumulative_index
from excel_writer import write_portfolio_report

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

# Map Tickers to Friendly Names
BENCHMARK_NAMES = {
    'SPY': 'S&P 500', 'IWV': 'Russell 3000',
    'ACWI': 'MSCI ACWI', 'VXUS': 'Total Int\'l Stock',
    'AGG': 'US Aggregate Bond', 'BND': 'Total Bond Market',
    'VNQ': 'Real Estate (REITs)', 'GLD': 'Gold',
    'BIL': '1-3 Month T-Bills'
}

def run_pipeline():
    # --- PATH SETUP ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    output_dir = os.path.join(project_root,"output")
    
    IBKR_FILE = os.path.join(project_root, "data", "U21244041_20250730_20260112.csv")
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    
    EXCEL_FILE = os.path.join(output_dir, f"Portfolio_Report_{timestamp}.xlsx")
    
    BENCHMARK_START_FIXED = "2025-07-30" # CURRENTLY STATIC 

    print(f"--- 1. Ingesting Portfolio (Internal) ---")
    if not os.path.exists(IBKR_FILE):
        print(f"CRITICAL ERROR: Could not find file at: {IBKR_FILE}")
        return

    try:
        # 1. Load Data
        portfolio_data = get_portfolio_holdings(IBKR_FILE, BENCHMARK_START_FIXED)
        holdings = portfolio_data.holdings
        account_title = portfolio_data.account_title
        report_date = portfolio_data.report_date
        total_nav = portfolio_data.total_nav
        orphaned_divs = portfolio_data.orphaned_divs

        
        # 2. Auto-Classify
        print("   > Running Auto-Classification...")
        all_tickers = holdings['ticker'].unique().tolist()
        name_map = fetch_security_names_yf(all_tickers)
        
        holdings['official_name'] = holdings['ticker'].map(name_map).fillna('')
        holdings['asset_class'] = holdings.apply(
            lambda row: auto_classify_asset(row['ticker'], row['official_name']), 
            axis=1
        )

        # 3. Cash Plug
        positions_val = holdings['raw_value'].sum()
        cash_plug_val = total_nav - positions_val if total_nav > 0 else 0.0
        
        if abs(cash_plug_val) > 1.0: 
            cash_row = {
                'ticker': 'CASH_BAL',
                'official_name': 'Cash & Sweeps (Reconciled)',
                'asset_class': 'Cash',
                'avg_cost': cash_plug_val,
                'raw_value': cash_plug_val,
                'total_dividends': orphaned_divs,
                'cumulative_return': 0.0
            }
            holdings = pd.concat([holdings, pd.DataFrame([cash_row])], ignore_index=True)

        # Recalculate weights
        total_mv = holdings['raw_value'].sum()
        holdings['weight'] = holdings['raw_value'] / total_mv if total_mv else 0

        # Recalculate Cash Return
        cash_mask = holdings['ticker'] == 'CASH_BAL'
        if cash_mask.any():
            c_cost = holdings.loc[cash_mask, 'avg_cost'].iloc[0]
            c_val = holdings.loc[cash_mask, 'raw_value'].iloc[0]
            c_div = holdings.loc[cash_mask, 'total_dividends'].iloc[0]
            if c_cost != 0:
                holdings.loc[cash_mask, 'cumulative_return'] = (c_val + c_div) / c_cost - 1.0

    except ValueError as e:
        print(f"Error unpacking data: {e}")
        return

    # 4. Market Data
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

    # --- 5. PREPARE DATA FOR EXCEL ---
    print("\n--- 3. Generating Excel Report ---")
    
    # A. Total Metrics
    grand_cost = holdings['avg_cost'].sum()
    grand_val = holdings['raw_value'].sum()
    grand_divs = holdings['total_dividends'].sum()
    
    total_ret_val = 0.0
    if grand_cost != 0:
        total_ret_val = (grand_val + grand_divs) / grand_cost - 1.0
        
    metrics = {
        'return': total_ret_val,
        'value': grand_val
    }
    
    # B. Summary DataFrame
    summary_rows = []
    my_buckets = holdings['asset_class'].unique()
    
    sorted_buckets = [b for b in BENCHMARK_CONFIG if b in my_buckets]
    for b in my_buckets:
        if b not in sorted_buckets: sorted_buckets.append(b)

    for bucket in sorted_buckets:
        bucket_data = holdings[holdings['asset_class'] == bucket]
        
        b_mv = bucket_data['raw_value'].sum()
        b_cost = bucket_data['avg_cost'].sum()
        b_divs = bucket_data['total_dividends'].sum()
        b_alloc = b_mv / grand_val if grand_val else 0.0
        
        if b_cost != 0:
            my_return = (b_mv + b_divs) / b_cost - 1
        else:
            my_return = 0.0
        
        summary_rows.append({
            'Type': 'Bucket',         
            'Name': bucket,
            'MarketValue': b_mv,
            'Allocation': b_alloc,
            'Return': my_return,
            'IsCash': (bucket == 'Cash')
        })
        
        targets = BENCHMARK_CONFIG.get(bucket, [])
        for b_ticker in targets:
            b_val = 0.0
            if not bench_growth.empty and b_ticker in bench_growth.columns:
                b_val = get_cumulative_return(bench_growth[b_ticker], 'INCEPTION')
            
            friendly_name = BENCHMARK_NAMES.get(b_ticker, b_ticker)
            
            summary_rows.append({
                'Type': 'Benchmark',     
                'Name': friendly_name,
                'MarketValue': None,
                'Allocation': None,
                'Return': b_val,
                'IsCash': False
            })

    summary_df = pd.DataFrame(summary_rows)

    # --- 6. WRITE TO EXCEL ---
    try:
        write_portfolio_report(
            account_title=account_title,
            summary_df=summary_df,
            holdings_df=holdings,
            total_metrics=metrics,
            report_date=report_date,
            output_path=EXCEL_FILE
        )
        print(f"DONE! Report generated: {os.path.basename(EXCEL_FILE)}")
    except Exception as e:
        print(f"Failed to write Excel: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_pipeline()