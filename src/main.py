import pandas as pd
import os
import datetime

# --- IMPORTS ---
from statement_ingestion import get_portfolio_holdings, parse_since_inception_csv
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
    if t in ['USD', 'ICSH']: return 'Cash'
    if t in ['VEA', 'VWO', 'IMTM', 'VXUS']: return 'International Equities'
    if t in ['BND', 'VGSH', 'VGIT']: return 'Fixed Income'
    if t in ['VNQ', 'BCI']: return 'Alternative Assets'
    
    # 2. Keywords
    if any(k in n for k in ['INTL', 'EMERGING', 'EUROPE', 'ASIA', 'DEVELOPED']): return 'International Equities'
    if any(k in n for k in ['BOND', 'TREASURY', 'FIXED INC', 'AGGREGATE']): return 'Fixed Income'
    if any(k in n for k in ['REIT', 'REAL ESTATE', 'GOLD', 'COMMODITY', 'BITCOIN']): return 'Alternative Assets'

    # 3. If no keywords found, classify as US Equity
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
    # DUAL-FILE ARCHITECTURE PATHS
    QUARTER_STMT_CSV = os.path.join(
        project_root, "data", "Gaard_Capital_LLC_2025_Q4_2025_Q4", 
        "Brogaard_Asset_Protection_Trust_U21244041_October_01_2025_December_31_2025.csv"
    )                                       
    SINCE_INCEPTION_STMT_CSV = os.path.join(                                                                    
        project_root, "data", "Gaard_Capital_LLC_Inception_February_23_2026", 
        "Brogaard_Asset_Protection_Trust_U21244041_July_30_2025_February_23_2026.csv"
    )            
    
    INFO_FILE = os.path.join(project_root, "data", "info_for_pdf.xlsx")                                         # INFO NECESSARY FOR PDF STYLING
    LOGO_FILE = os.path.join(project_root, "data", "pdf_resources", "logos", "gaard_logo.png")
    TEXT_LOGO_FILE = os.path.join(project_root, "data", "pdf_resources", "logos", "gaard_text_logo.png")
    
    SELECTED_COMP_BENCHMARK_KEY = '60/40 SPY/AGG'                                                           # SEE CONFIGURATION ABOVE FOR OPTIONS
    RISK_TIME_HORIZON = 1                                                                  # USED FOR RISK METRICS: IDIOSYNCHRATIC RISK & FACTORS
    # * OUTPUT FILE NAMING BELOW
    # **************************************************************************************************************************************** #
    
    # === Load PDF Info ===
    pdf_info = {}
    if os.path.exists(INFO_FILE):
        print("   > SETUP: Loading PDF Info Sheet...")
        try:
            info_df = pd.read_excel(INFO_FILE, header=2)
            info_df.columns = [c.strip() if isinstance(c, str) else c for c in info_df.columns]
            if 'key' in info_df.columns and 'value' in info_df.columns:
                pdf_info = dict(zip(info_df['key'].dropna(), info_df['value'].dropna()))
            else:
                print(f"Warning: Exprected columns 'key' and 'value' not found. Found: {info_df.columns.to_list()}")
        except Exception as e:
            print(f"Warning: Could not load Info File for PDF: {e}")
    
    # === Load All Data ===
    print(f"\n === 1. Ingesting Portfolio (Internal) ===")
    if not os.path.exists(QUARTER_STMT_CSV):
        print(f"CRITICAL ERROR: Could not find file at: {QUARTER_STMT_CSV}")
        import traceback
        traceback.print_exc()
        return

    try:
        # === 1. Load Data from Statements. Calculate, Classify, and Weight Assets ===
        PERIOD_FALLBACK_DATE = (pd.Timestamp.today().to_period("Q") - 1).start_time.strftime("%Y-%m-%d") # Fallback Date: Manually Compute Last Quarter
        portfolio_data = get_portfolio_holdings(QUARTER_STMT_CSV, PERIOD_FALLBACK_DATE) # Benchmark Default Date will be overwritten to exact end date
        
        # --- QUARTER STATEMENT ---
        holdings = portfolio_data.holdings
        account_title = portfolio_data.account_title
        report_date = portfolio_data.report_date
        quarter_start_date = portfolio_data.quarter_start_date
        key_stats = portfolio_data.key_statistics
        total_nav = portfolio_data.total_nav
        legal_notes = portfolio_data.legal_notes
        print(f"   > 1a: Quarter Statement Period Check: {quarter_start_date} to {report_date}")
        
        # --- SINCE INCEPTION STATEMENT ---
        inception_data = parse_since_inception_csv(SINCE_INCEPTION_STMT_CSV)
        daily_history = inception_data.daily_returns
        inception_risk_measures = inception_data.risk_measures
        # Setup Dates (Ensuring everything is a proper Pandas Timestamp)
        rd_date = pd.to_datetime(report_date)
        qs_date = pd.to_datetime(quarter_start_date)
        
        # --- ENSURE DATA TYPES & ALIGNMENT ---
        if not daily_history.empty:
            # Standardize columns to lowercase and drop duplicates
            daily_history = daily_history.loc[:, ~daily_history.columns.duplicated()].copy()

            # Force date conversion with SPECIFIC format: Format %m/%d/%y matches Inception CSV's "07/30/25"
            daily_history['date'] = pd.to_datetime(daily_history['Date'], format='%m/%d/%y', errors='coerce').dt.normalize()
            
            # 3. Create 'nav' Wealth Index from the 'return' column
            # This turns 0.0556 into 105.56, allowing (105.56 / 100) - 1 = 5.56%
            if 'return' in daily_history.columns:
                print("   > Converting Cumulative Returns to Wealth Index (NAV)...")
                daily_history['nav'] = (1 + daily_history['Return'].astype(float) / 100) * 100
                
            daily_history = daily_history.sort_values('date').reset_index(drop=True)
            daily_history = daily_history[daily_history['date'] <= rd_date].copy()
        
        # Check for matching end dates
        max_inception_dt = daily_history['date'].max()
        if not pd.isna(max_inception_dt) and max_inception_dt.strftime('%Y-%m-%d') != rd_date.strftime('%Y-%m-%d'):
            print(f"WARNING: Date Mismatch! Quarter ends {rd_date.date()}, Inception data ends {max_inception_dt.date()}")
    
        # --- CALCULATE PERIOD RETURNS ---
        # Pass the normalized Timestamp 'rd_date' instead of the string 'report_date'
        window_returns, quarter_label = calculate_period_returns(daily_history, rd_date)
        
        # 'Quarter' Return (Direct from Quarter statement)
        window_returns['Quarter'] = key_stats.get('CumulativeReturn', 0.0)
        
        # 'Inception' Return (Calculated from the Wealth Index)
        if not daily_history.empty:
            first_val = daily_history['nav'].iloc[0]
            last_val = daily_history['nav'].iloc[-1]
            window_returns['Inception'] = (last_val / first_val) - 1.0 if first_val != 0 else 0.0
            
        # *** OUTPUT FILE NAME ***
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        PDF_FILE = os.path.join(output_dir, f"{account_title}_Quarter_Portfolio_Report_{timestamp}.pdf")

        # --- CLEAN TICKERS ---
        print("   > 1c. Cleaning up Tickers...")
        # Apply Exact Match Filter
        holdings = holdings[~holdings['ticker'].astype(str).str.upper().isin(IGNORE_EXACT)].copy()
        # Apply "Starts With" Filter
        for prefix in IGNORE_STARTSWITH:
            holdings = holdings[~holdings['ticker'].astype(str).str.startswith(prefix)].copy()
        # Remove Zero Value rows
        holdings = holdings[holdings['raw_value'].abs() > 0.01].copy()
        
        # --- AUTO CLASSIFY ---
        print("   > 1d. Running Auto-Classification...")
        all_tickers = holdings['ticker'].unique().tolist()
        name_map = fetch_security_names_yf(all_tickers)
        # name_map['USD'] = 'Settled Cash'  # Override YF for Settled Cash
        
        holdings['official_name'] = holdings['ticker'].map(name_map).fillna('')
        holdings['asset_class'] = holdings.apply(
            lambda row: auto_classify_asset(row['ticker'], row['official_name']), 
            axis=1
        )
            
        # === Calculate Weights ===
        total_value = holdings['raw_value'].sum()
        holdings['weight'] = (holdings['raw_value'] / total_value).fillna(0.0) if total_value else 0.0
        holdings['cumulative_return'] = holdings['cumulative_return'].fillna(0.0)
            
    except ValueError as e:
        print(f"Error unpacking data: {e}")
        import traceback
        traceback.print_exc()
        return

    # === 2. Fetch Market Data from YF ===
    print("\n=== 2. Fetching Market Data (External) ===")
    
    # Get User Selection for Main Benchmark
    main_benchmark_weights = COMPOSITE_BENCHMARK_CONFIG.get(SELECTED_COMP_BENCHMARK_KEY, {'SPY': 1.0}) # Fallback Benchmark: SPY
    main_bench_constituents = list(main_benchmark_weights.keys())
    
    # Gather ALL benchmarks needed (Standard + Main Constituents)
    standard_benchmarks = [t for sublist in BENCHMARK_CONFIG.values() for t in sublist]
    all_needed_tickers = list(set(standard_benchmarks + main_bench_constituents))
    
    # --- CALCULATE DATES FOR TRAILING RETURNS & RISK METRICS ---  
    # Pull Inception Date
    if not daily_history.empty:
        portfolio_inception_date = pd.to_datetime(daily_history['date'].min())
    else:
        portfolio_inception_date = qs_date # Fallback: Quarter Start Date
    
    # Still need 5 Years Ago Date for 1Y/3Y/5Y metrics *** UPDATE IF LONGER RETURN PERIODS ARE ADDED ***
    five_years_ago_date = rd_date - pd.DateOffset(years=5)
    
    # Pick OLDEST date between Inception and 5 Years Ago (+ 7 day buffer to ensure first day isn't missed)
    fetched_start_date = min(portfolio_inception_date, five_years_ago_date) - pd.Timedelta(days=7)
    
    # --- FETCH BENCHMARK RETURNS ---
    bench_returns_df = pd.DataFrame()
    bench_growth_period = pd.DataFrame()
    
    try:
        bench_returns_df = fetch_benchmark_returns_yf(
            all_needed_tickers, 
            start_date=fetched_start_date.strftime('%Y-%m-%d'), 
            end_date=report_date
        )
        
        # Slice specifically for the quarter
        period_subset = bench_returns_df.loc[quarter_start_date:report_date].copy()
        if not period_subset.empty:
            bench_growth_period = get_cumulative_index(period_subset, start_value=100)
            
        print("   > Fetched Benchmark Data from Yahoo Finance")
    except Exception as e:
        print(f"Yahoo Connection Error: {e}")
        import traceback
        traceback.print_exc()

    # --- CALCULATE COMPOSITE BENCHMARK ---
    # Calculate Composite Benchmark for Performance History Chart and Table
    print(f"   > 2b. Calculating Composite Benchmark: {SELECTED_COMP_BENCHMARK_KEY}")
    main_benchmark_series = calculate_composite_benchmark_return(bench_returns_df, main_benchmark_weights)
    chart_data = prepare_chart_data(daily_history, main_benchmark_series, benchmark_name=SELECTED_COMP_BENCHMARK_KEY)
    print(f"   > Performance Chart: Prepared {len(chart_data)} daily data points.")

    # Calculate Benchmark Windows
    bench_nav_df = pd.DataFrame({
        'date': main_benchmark_series.index,
        'nav': (1 + main_benchmark_series).cumprod() * 100
    })
    
    # --- CALCULATE BENCHMARK PERIODS ---
    # Get standard calendar windows (1M, 3M, YTD, 1Y, 3Y, 5Y)
    bench_windows, _ = calculate_period_returns(bench_nav_df, report_date)
    
    # Match the exact Portfolio dates for 'Period' and 'Inception'
    if not bench_nav_df.empty:
        nav_series = bench_nav_df.set_index('date')['nav']
        
        def get_exact_ret(start_dt, end_dt):
            val_start = nav_series.asof(start_dt)
            val_end = nav_series.asof(end_dt)
            
            if pd.isna(val_start) or pd.isna(val_end) or val_start == 0:
                return 0.0
            return (val_end / val_start) - 1.0

        # Calculate benchmark return for the exact Statement Quarter
        bench_windows['Period'] = get_exact_ret(qs_date, rd_date)
        
        # Calculate benchmark return for the exact Lifetime of the account
        if not daily_history.empty:
            bench_windows['Inception'] = get_exact_ret(portfolio_inception_date, rd_date)

    # --- CALCULATE RISK METRICS ---
    print(f"   > 2c. Calculating Risk Profile (Horizon: {RISK_TIME_HORIZON or 'Full'} Yrs) ---")
    try:
        # Custom metrics: Idiosynchratic Risk, Factors
        risk_metrics = calculate_portfolio_risk(daily_history, main_benchmark_series, lookback_years=RISK_TIME_HORIZON)
        
        # Merge clean Inception CSV metrics
        if isinstance(inception_risk_measures, dict) and inception_risk_measures:
            risk_metrics.update(inception_risk_measures)  
            
    except Exception as e:
        print(f"Risk Calculation Error: {e}")
        import traceback
        traceback.print_exc()
        risk_metrics = {}

    # === 3. PREPARE DATA FOR PDF / EXCEL ===
    print("\n=== 3. Generating PDF Report ===")

    # Aggregate Grand Totals
    grand_cost = holdings['avg_cost'].sum()
    grand_value = holdings['raw_value'].sum()
    grand_divs = holdings['total_dividends'].sum()
    grand_realized = holdings['realized_pl'].sum()
    
    metrics = {
        'return': ((grand_value - grand_cost) + grand_divs + grand_realized) / grand_cost if grand_cost != 0 else 0.0,
        'value': grand_value
    }
    
    # Build Asset Allocation Summary Rows
    summary_rows = []
    my_buckets = holdings['asset_class'].unique()
    
    sorted_buckets = [b for b in BENCHMARK_CONFIG if b in my_buckets]
    for b in my_buckets:
        if b not in sorted_buckets: sorted_buckets.append(b)

    for bucket in sorted_buckets:
        bucket_data = holdings[holdings['asset_class'] == bucket]
        
        # Aggregate bucket-level metrics
        b_mv = bucket_data['raw_value'].sum()
        b_cost = bucket_data['avg_cost'].sum()
        b_divs = bucket_data['total_dividends'].sum()
        b_realized = bucket_data['realized_pl'].sum()
        
        # Add Bucket Row
        summary_rows.append({
            'Type': 'Bucket',         
            'Name': bucket,
            'MarketValue': b_mv,
            'Allocation': b_mv / grand_value if grand_value else 0.0,
            'Return': ((b_mv - b_cost) + b_divs + b_realized) / b_cost if b_cost != 0 else 0.0,
            'IsCash': (bucket == 'Cash')
        })
        
        # Add Corresponding Benchmark Row(s)
        for b_ticker in BENCHMARK_CONFIG.get(bucket, []):
            b_val = 0.0
            if not bench_growth_period.empty and b_ticker in bench_growth_period.columns:
                b_val = get_cumulative_return(bench_growth_period[b_ticker], 'INCEPTION')
            
            summary_rows.append({
                'Type': 'Benchmark',     
                'Name': BENCHMARK_NAMES.get(b_ticker, b_ticker),
                'MarketValue': None,
                'Allocation': None,
                'Return': b_val,
                'IsCash': False
            })

    summary_df = pd.DataFrame(summary_rows)

    # -- WRITE TO PDF ---
    try:
        write_portfolio_report(
            account_title=account_title,
            report_date=report_date,
            summary_df=summary_df,
            key_statistics=key_stats,
            holdings_df=holdings,
            total_metrics=metrics,
            main_benchmark_tckr=SELECTED_COMP_BENCHMARK_KEY,
            
            performance_windows=window_returns,
            benchmark_performance_windows=bench_windows,
            performance_chart_data=chart_data,
            quarter_label=quarter_label,
            
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