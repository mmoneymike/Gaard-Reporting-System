import pandas as pd
import os
import datetime

# --- IMPORTS ---
from statement_ingestion import get_portfolio_holdings, parse_performance_csv
from yf_loader import fetch_benchmark_returns_yf, fetch_security_names_yf
from return_metrics import*
from pdf_writer import write_portfolio_report
from excel_writer import write_portfolio_report_xlsx
from risk_metrics import calculate_portfolio_risk


#  ==========================================
#   CONFIGURATION
#  ==========================================
COMPOSITE_BENCHMARK_CONFIG = {
    'SPY':         {'SPY': 1.0},
    'AGG':         {'AGG': 1.0},
    '60/40 SPY/AGG':    {'SPY': 0.6, 'AGG': 0.4},
    '40/60 SPY/AGG':    {'SPY': 0.4, 'AGG': 0.6},
} # Options for SELECTED_COMP_KEY

BENCHMARK_CONFIG = {
    'U.S. Equities':        ['SPY'],  
    'International Equities': ['ACWI'], 
    'Fixed Income':         ['AGG'],
    'Alternative Assets':   ['QAI'],
    'Cash':                 ['BIL'],
} # Chosen Benchmark Tickers

BENCHMARK_NAMES = {
    'SPY': 'S&P 500',
    'ACWI': 'MSCI ACWI',
    'AGG': 'US Aggregate Bond',
    'QAI': 'NYLI Hedge Multi-Strategy',
    'BIL': '1-3 Month T-Bills',
} # Chosen Benchmark Names

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
    
    # **************************************************************************************************************************************** #
    IBKR_FILE = os.path.join(project_root, "data", "U21244041_20250730_20260112.csv")                       # GENERAL INTERACTIVE BROKERS FILE
    PERF_FILE = os.path.join(project_root, "data", "Gaard_Capital_LLC_July_30_2025_January_12_2026.csv")    # DAILY NAV FILE
    INFO_FILE = os.path.join(project_root, "data", "info_for_pdf.xlsx")                                     # INFO NECESSARY FOR PDF STYLING
    
    LOGO_FILE = os.path.join(project_root, "data", "gaard_logo.png")
    TEXT_LOGO_FILE = os.path.join(project_root, "data", "gaard_text_logo.png")
    
    SELECTED_COMP_BENCHMARK_KEY = '60/40 SPY/AGG'                                                           # SEE CONFIGURATION ABOVE FOR OPTIONS
    RISK_TIME_HORIZON = 1
    # **************************************************************************************************************************************** #
    
    pdf_info = {}
    if os.path.exists(INFO_FILE):
        print("   > SETUP: Loading PDF Info Sheet...")
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
    
    print(f"\n === 1. Ingesting Portfolio (Internal) ===")
    if not os.path.exists(IBKR_FILE):
        print(f"CRITICAL ERROR: Could not find file at: {IBKR_FILE}")
        return

    try:
        # === 1. Load Data ===
        PERIOD_FALLBACK_DATE = (pd.Timestamp.today().to_period("Q") - 1).start_time.strftime("%Y-%m-%d") # Fallback Date: Last Quarter
        portfolio_data = get_portfolio_holdings(IBKR_FILE, PERIOD_FALLBACK_DATE)
        holdings = portfolio_data.holdings
        account_title = portfolio_data.account_title
        report_date = portfolio_data.report_date
        statement_start_date = portfolio_data.period_start_date
        print(f"   > 1a: Statement Period: {statement_start_date} to {report_date}")
        legal_notes = portfolio_data.legal_notes
        total_nav = portfolio_data.total_nav
        settled_cash = portfolio_data.settled_cash
        nav_performance = portfolio_data.nav_performance
        print(f"   > 1b: Parsed NAV Performance: {nav_performance}")

        # --- PERFORMANCE HISTORY ---
        daily_history = parse_performance_csv(PERF_FILE)
    
        # --- CALCULATE METRICS ---
        window_returns, period_label = calculate_period_returns(daily_history, report_date)
        
        # --- OUTPUT FILE NAMES ---
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        PDF_FILE = os.path.join(output_dir, f"{account_title}_Portfolio_Report_{timestamp}.pdf")
        
        # --- CLEAN TICKERS ---
        print("   > 1c: Cleaning up Tickers...")
        # Apply Exact Match Filter
        holdings = holdings[~holdings['ticker'].astype(str).str.upper().isin(IGNORE_EXACT)].copy()
        # Apply "Starts With" Filter (Loop through the config list)
        for prefix in IGNORE_STARTSWITH:
            holdings = holdings[~holdings['ticker'].astype(str).str.startswith(prefix)].copy()
        # Remove Zero Value rows
        holdings = holdings[holdings['raw_value'].abs() > 0.01].copy()
        
        # === Auto-Classify ===
        print("   > 1d: Running Auto-Classification...")
        all_tickers = holdings['ticker'].unique().tolist()
        name_map = fetch_security_names_yf(all_tickers)
        
        holdings['official_name'] = holdings['ticker'].map(name_map).fillna('')
        holdings['asset_class'] = holdings.apply(
            lambda row: auto_classify_asset(row['ticker'], row['official_name']), 
            axis=1
        )

        # === Cash Logic ===
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
            
        # === Calculate Weights ===
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

    # === 2. Market Data ===
    print("\n=== 2. Fetching Market Data (External) ===")
    
    # Get User Selection for Main Benchmark
    main_benchmark_weights = COMPOSITE_BENCHMARK_CONFIG.get(SELECTED_COMP_BENCHMARK_KEY, {'SPY': 1.0})
    main_bench_constituents = list(main_benchmark_weights.keys())
    
    # Gather ALL benchmarks needed (Standard + Main Constituents)
    standard_benchmarks = [t for sublist in BENCHMARK_CONFIG.values() for t in sublist]
    all_needed_tickers = list(set(standard_benchmarks + main_bench_constituents))
    
    # Fetch Long History (5 Years) for Calculation (1Y/3Y stats)
    rd_dt = pd.to_datetime(report_date)
    long_start_dt = rd_dt - pd.DateOffset(years=5)
    long_start_str = long_start_dt.strftime('%Y-%m-%d')
    
    bench_returns_df = pd.DataFrame()
    bench_growth_period = pd.DataFrame()
    
    try:
        # Fetch ALL returns (Long History)
        bench_returns_df = fetch_benchmark_returns_yf(all_needed_tickers, start_date=long_start_str, end_date=report_date)
        
        # Extract specific period for Asset Allocation Table (Statement Period Only)
        # Note: We rely on string slicing here which works if index is DatetimeIndex
        period_subset = bench_returns_df.loc[statement_start_date:report_date].copy()
        if not period_subset.empty:
            bench_growth_period = get_cumulative_index(period_subset, start_value=100)
            
        print("   > 2a. Fetched Benchmark Data (Yahoo)")
    except Exception as e:
        print(f"Yahoo Connection Error: {e}")

    # --- Calculate Composite & Windows for Chart & Table ---
    print(f"   > Calculating Composite Benchmark: {SELECTED_COMP_BENCHMARK_KEY}")
    
    # Composite Daily Returns (Full 5Y History)
    main_benchmark_series = calculate_composite_benchmark_return(bench_returns_df, main_benchmark_weights)
    
    # Chart Data (Uses Full History to match Portfolio Inception)
    chart_data = prepare_chart_data(daily_history, main_benchmark_series, benchmark_name=SELECTED_COMP_BENCHMARK_KEY)

    # Calculate Benchmark Windows (1M, YTD, 1Y, 3Y, etc.)
    # Create NAV-like DF for the calculator
    bench_nav_df = pd.DataFrame({
        'date': main_benchmark_series.index,
        'nav': (1 + main_benchmark_series).cumprod() * 100
    })
    
    # Calculate Standard Stats
    bench_windows, _ = calculate_period_returns(bench_nav_df, report_date)
    
    # --- CRITICAL: OVERRIDE INCEPTION & PERIOD ---
    # We must ensure "Inception" matches Portfolio Inception, not the 5Y fetch start.
    if not bench_nav_df.empty:
        # Helper to calculate return between two dates
        def get_ret(s_date, e_date):
            if bench_nav_df.empty: return 0.0
            row_start = bench_nav_df[bench_nav_df['date'] <= s_date]
            val_start = row_start.iloc[-1]['nav'] if not row_start.empty else bench_nav_df.iloc[0]['nav']
            
            row_end = bench_nav_df[bench_nav_df['date'] <= e_date]
            val_end = row_end.iloc[-1]['nav'] if not row_end.empty else bench_nav_df.iloc[-1]['nav']
            
            return (val_end / val_start) - 1.0 if val_start != 0 else 0.0

        # Override Period (Statement Start)
        bench_windows['Period'] = get_ret(pd.to_datetime(statement_start_date), rd_dt)
        
        # Override Inception (Portfolio Inception)
        if not daily_history.empty:
            port_inception = daily_history['date'].min()
            bench_windows['Inception'] = get_ret(port_inception, rd_dt)

    # --- 2b. Calculate Risk Metrics ---
    print(f"   > 2b. Calculating Risk Profile (Horizon: {RISK_TIME_HORIZON or 'Full'} Yrs) ---")
    risk_metrics = {}
    try:
        # Pass lookback_years here
        risk_metrics = calculate_portfolio_risk(
            daily_history, 
            main_benchmark_series, 
            lookback_years=RISK_TIME_HORIZON
        )
        print(f"   > Risk Metrics Calculated Successfully: {risk_metrics}")
    except Exception as e:
        print(f"Risk Calc Error: {e}")
        risk_metrics = {}

    # === 3. PREPARE DATA FOR PDF / EXCEL ===
    print("\n=== 3. Generating PDF Report ===")
    
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
            # Use bench_growth_period (Specific to Statement Period)
            if not bench_growth_period.empty and b_ticker in bench_growth_period.columns:
                b_val = get_cumulative_return(bench_growth_period[b_ticker], 'INCEPTION')
            
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

    # === WRITE TO PDF ===
    try:
        write_portfolio_report(
            account_title=account_title,
            report_date=report_date,
            summary_df=summary_df,
            nav_performance=nav_performance,
            holdings_df=holdings,
            total_metrics=metrics,
            main_benchmark_tckr=SELECTED_COMP_BENCHMARK_KEY,
            
            performance_windows=window_returns,
            benchmark_performance_windows=bench_windows,
            performance_chart_data=chart_data,
            period_label=period_label,
            
            risk_metrics=risk_metrics,
            risk_time_horizon=RISK_TIME_HORIZON,
            
            legal_notes=legal_notes,
            pdf_info=pdf_info,
            text_logo_path=TEXT_LOGO_FILE,
            logo_path=LOGO_FILE, 
            output_path=PDF_FILE,
        )
        print(f"* DONE! Report Generated: {os.path.basename(PDF_FILE)} *")

    except Exception as e:
        print(f"Failed to write PDF: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_pipeline()