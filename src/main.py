import pandas as pd
import os
import datetime

# --- IMPORTS ---
from statement_ingestion import get_portfolio_holdings
from yf_loader import fetch_benchmark_returns_yf, fetch_security_names_yf
from report_metrics import get_cumulative_return, get_cumulative_index
from pdf_writer import write_portfolio_report
from excel_writer import write_portfolio_report_xlsx
from risk_analytics import calculate_portfolio_risk


#  ==========================================
#   CONFIGURATION
#  ==========================================
BENCHMARK_CONFIG = {
    'U.S. Equities':        ['SPY'],  
    'International Equities': ['ACWI'], 
    'Fixed Income':         ['AGG'],
    'Alternative Assets':   ['QAI'],
    'Cash':                 ['BIL'],
}

BENCHMARK_NAMES = {
    'SPY': 'S&P 500',
    'ACWI': 'MSCI ACWI',
    'AGG': 'US Aggregate Bond',
    'QAI': 'NYLI Hedge Multi-Strategy',
    'BIL': '1-3 Month T-Bills',
}

# 1. Exact Matches: Tickers to remove if they match exactly (Case Insensitive)
IGNORE_EXACT = [
    'USD', 
    'CASH', 
    'TOTAL CASH',
]

# 2. Starts With: Useful for weird bonds, expired options, or specific series.
IGNORE_STARTSWITH = [
    '912797PN1',  # Treasury Bond Series
]

RISK_BENCHMARK_TCKR = 'AGG'
RISK_TIME_HORIZON = 1

# --- LOCAL AUTO-CLASSIFY ---
def auto_classify_asset(ticker: str, security_name: str) -> str:
    t = str(ticker).upper().strip()
    n = str(security_name).upper().strip()
    
    # 1. Hardcoded
    if t in ['ICSH']: return 'Cash'
    if t in ['VEA', 'VWO', 'IMTM', 'VXUS']: return 'International Equities'
    if t in ['BND', 'VGSH', 'VGIT']: return 'Fixed Income'
    if t in ['VNQ', 'BCI']: return 'Alternative Assets'
    if t == 'CASH_BAL': return 'Cash'
    
    # 2. Keywords
    if any(k in n for k in ['INTL', 'EMERGING', 'EUROPE', 'ASIA', 'DEVELOPED']): return 'International Equities'
    if any(k in n for k in ['BOND', 'TREASURY', 'FIXED INC', 'AGGREGATE']): return 'Fixed Income'
    if any(k in n for k in ['REIT', 'REAL ESTATE', 'GOLD', 'COMMODITY', 'BITCOIN']): return 'Alternative Assets'

    return 'U.S. Equities'

#  ==========================================
#   MAIN PIPELINE
#  ==========================================
def run_pipeline():
    # --- PATHS SETUP ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    output_dir = os.path.join(project_root,"output")
    
    IBKR_FILE = os.path.join(project_root, "data", "U21244041_20250730_20260112.csv")

    BENCHMARK_START_FIXED = "2025-07-30" # CURRENTLY STATIC

    INFO_FILE = os.path.join(project_root, "data", "info_for_pdf.xlsx")
    pdf_info = {}
    if os.path.exists(INFO_FILE):
        print("   > Loading PDF Info Sheet...")
        try:
            info_df = pd.read_excel(INFO_FILE, header=2)
            info_df.columns = [c.strip() if isinstance(c, str) else c for c in info_df.columns]
            
            if 'key' in info_df.columns and 'value' in info_df.columns:
                info_df = info_df.dropna(subset=['key'])
                pdf_info = dict(zip(info_df['key'], info_df['value']))
            else:
                print(f"Warning: Exprected columns 'key' and 'value' not found. Found: {info_df.columns.to_list()}")
        except Exception as e:
            print(f"Warning: Could not load info file: {e}")
    else:
        print(f"Warning: Info file not found at {INFO_FILE}")
    
    TEXT_LOGO_FILE = os.path.join(project_root, "data", "gaard_text_logo.png")
    LOGO_FILE = os.path.join(project_root, "data", "gaard_logo.png")
    
    print(f"--- 1. Ingesting Portfolio (Internal) ---")
    if not os.path.exists(IBKR_FILE):
        print(f"CRITICAL ERROR: Could not find file at: {IBKR_FILE}")
        return

    try:
        # === 1. Load Data ===
        portfolio_data = get_portfolio_holdings(IBKR_FILE, BENCHMARK_START_FIXED)
        holdings = portfolio_data.holdings
        account_title = portfolio_data.account_title
        report_date = portfolio_data.report_date
        total_nav = portfolio_data.total_nav
        settled_cash = portfolio_data.settled_cash
        nav_performance = portfolio_data.nav_performance
        print(f"   > Parsed NAV Performance: {nav_performance}")

        # --- 1a. OUTPUT FILE NAMES ---
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        PDF_FILE = os.path.join(output_dir, f"{account_title}_Portfolio_Report_{timestamp}.pdf")
        EXCEL_FILE = os.path.join(output_dir, f"{account_title}_Portfolio_Report_{timestamp}.xlsx")
        
        # --- 1b. CLEAN TICKERS ---
        print("   > Cleaning up Tickers...")
        # Apply Exact Match Filter
        holdings = holdings[~holdings['ticker'].astype(str).str.upper().isin(IGNORE_EXACT)].copy()
        # Apply "Starts With" Filter (Loop through the config list)
        for prefix in IGNORE_STARTSWITH:
            holdings = holdings[~holdings['ticker'].astype(str).str.startswith(prefix)].copy()
        # Remove Zero Value rows
        holdings = holdings[holdings['raw_value'].abs() > 0.01].copy()
        
        # === 2. Auto-Classify ===
        print("   > Running Auto-Classification...")
        all_tickers = holdings['ticker'].unique().tolist()
        name_map = fetch_security_names_yf(all_tickers)
        
        holdings['official_name'] = holdings['ticker'].map(name_map).fillna('')
        holdings['asset_class'] = holdings.apply(
            lambda row: auto_classify_asset(row['ticker'], row['official_name']), 
            axis=1
        )

        # === 3. Cash Logic ===
        # Insert the Settled Cash Row (Strictly using the extracted number)
        if settled_cash > 1.0:
            cash_row = {
                'ticker': 'CASH_BAL',
                'official_name': 'Settled Cash',
                'asset_class': 'Cash',
                'avg_cost': settled_cash,   
                'raw_value': settled_cash,
                'realized_pl': 0.0,
                'total_dividends': 0.0, 
                'cumulative_return': 0.0
            }
            holdings = pd.concat([holdings, pd.DataFrame([cash_row])], ignore_index=True)

        # Reconcile to Official NAV
        # We calculate: (Official NAV) - (Stocks + Settled Cash) = Accruals/Rounding
        current_total = holdings['raw_value'].sum()
        discrepancy = total_nav - current_total

        if abs(discrepancy) > 1.0:
            adj_row = {
                'ticker': 'ACCRUALS',
                'official_name': 'Portfolio Accruals/Other',
                'asset_class': 'Other',
                'avg_cost': discrepancy,
                'raw_value': discrepancy,
                'realized_pl': 0.0,
                'total_dividends': 0.0,
                'cumulative_return': 0.0
            }
            holdings = pd.concat([holdings, pd.DataFrame([adj_row])], ignore_index=True)
            
        # === 4. Calculate Weights ===
        total_value = holdings['raw_value'].sum()
        
        if total_value != 0:
            holdings['weight'] = holdings['raw_value'] / total_value
        else:
            holdings['weight'] = 0.0
            
        holdings['weight'] = holdings['weight'].fillna(0.0)
        holdings['cumulative_return'] = holdings['cumulative_return'].fillna(0.0)
            
    except ValueError as e:
        print(f"Error unpacking data: {e}")
        return

    # === 5. Market Data ===
    # --- 5a. Calculate Benchmark Returns
    print("\n--- 2. Fetching Benchmark Data (Yahoo) ---")
    all_benchmarks = [t for sublist in BENCHMARK_CONFIG.values() for t in sublist]
    unique_benchmarks = list(set(all_benchmarks))
    
    bench_growth = pd.DataFrame()
    try:
        bench_returns = fetch_benchmark_returns_yf(unique_benchmarks, start_date=BENCHMARK_START_FIXED, end_date=report_date)
        if not bench_returns.empty:
            bench_growth = get_cumulative_index(bench_returns, start_value=100)
    except Exception as e:
        print(f"Yahoo Connection Error: {e}")

    # --- 5b. Calculate Risk Metrics ---
    print("\n--- 2b. Calculating Risk Profile ---")
    risk_metrics = {}
    try:
        risk_metrics = calculate_portfolio_risk(holdings, benchmark_ticker=RISK_BENCHMARK_TCKR, lookback_years= RISK_TIME_HORIZON)
        print("   > Risk Metrics Calculated:", risk_metrics)
    except Exception as e:
        print(f"Risk Calc Error: {e}")
        risk_metrics = {'Beta': 0, 'R2': 0, 'Volatility': 0, 'Sharpe': 0}

    # === 6. PREPARE DATA FOR PDF / EXCEL ===
    print("\n--- 3. Generating Excel Report ---")
    
    # Total Metrics
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

    # === 7. WRITE TO PDF / EXCEL ===
    try:
        write_portfolio_report(
            account_title=account_title,
            report_date=report_date,
            summary_df=summary_df,
            nav_performance=nav_performance,
            holdings_df=holdings,
            total_metrics=metrics,
            
            risk_metrics=risk_metrics,
            risk_benchmark_tckr=RISK_BENCHMARK_TCKR,
            risk_time_horizon=RISK_TIME_HORIZON,
            
            pdf_info=pdf_info,
            text_logo_path=TEXT_LOGO_FILE,
            logo_path=LOGO_FILE,
            output_path=PDF_FILE,
        )
        print(f"DONE! Report Generated: {os.path.basename(PDF_FILE)}")
    # try:
    #     write_portfolio_report_xlsx(
    #         account_title=account_title,
    #         summary_df=summary_df,
    #         holdings_df=holdings,
    #         total_metrics=metrics,
    #         report_date=report_date,
    #         output_path=EXCEL_FILE
    #     )
    #     print(f"DONE! Report generated: {os.path.basename(EXCEL_FILE)}")
    except Exception as e:
        print(f"Failed to write PDF: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_pipeline()