import pandas as pd
import os
import re
import csv
import datetime
import traceback

# --- IMPORTS ---
from ib_connector import fetch_files_via_sftp, decrypt_pgp_files
from statement_ingestion import get_portfolio_holdings, parse_since_inception_csv
from yf_loader import fetch_benchmark_returns_yf, fetch_security_names_yf
from return_metrics import*
from pdf_writer import write_portfolio_report
from excel_writer import write_portfolio_report_xlsx
from risk_metrics import calculate_portfolio_risk


#  ==========================================
#   CONFIGURATION
#  ==========================================
# SFTP & PGP CONFIGURATION
IB_SFTP_HOST = 'ftp2.interactivebrokers.com'
IB_USERNAME = 'gaardcapital'
REMOTE_STATEMENT_DIR = 'outgoing'

# MM KEY PATHS
SSH_PRIVATE_KEY_PATH = '/Users/michaelmolenaar/Desktop/BROGAARD/Gaard/Gaard Reporting System/data/Gaard_Keys/IB_SSH_Private.txt'
SSH_PUBLIC_KEY_PATH  = '/Users/michaelmolenaar/Desktop/BROGAARD/Gaard/Gaard Reporting System/data/Gaard_Keys/IB_SSH_Public.txt'
PGP_PRIVATE_KEY_PATH = '/Users/michaelmolenaar/Desktop/BROGAARD/Gaard/Gaard Reporting System/data/Gaard_Keys/IB_PGP_Private.txt'
PGP_PUBLIC_KEY_PATH  = '/Users/michaelmolenaar/Desktop/BROGAARD/Gaard/Gaard Reporting System/data/Gaard_Keys/IB_PGP_Public.txt'
PGP_PASSPHRASE = None

COMPOSITE_BENCHMARK_CONFIG = {
    'SPY':              {'SPY': 1.0},
    'AGG':              {'AGG': 1.0},
    '60% SPY / 40% AGG':    {'SPY': 0.6, 'AGG': 0.4},
    '40% SPY / 60% AGG':    {'SPY': 0.4, 'AGG': 0.6},
} # Options for SELECTED_COMP_KEY

BENCHMARK_CONFIG = {
    'U.S. Equities':            ['SPY'],  
    'International Equities':   ['ACWX'], 
    'Fixed Income':             ['AGG'],
    'Alternative Assets':       ['QAI'],
    'Cash':                     ['BIL'],
} # Chosen Benchmark Tickers

BENCHMARK_NAMES = {
    'SPY': 'S&P 500',
    'ACWX': 'MSCI ACWI ex US',
    'AGG': 'US Aggregate Bond',
    'QAI': 'NYLI Hedge Multi-Strategy',
    'BIL': '1-3 Month T-Bills',
} # Chosen Benchmark Names

# Ignore Specific Holdings
# 1. Exact Matches: Tickers to remove if they match exactly (Case Insensitive)
IGNORE_EXACT = [
    'CASH',
    'TOTAL CASH',
]

# 2. Starts With: Useful for weird bonds, expired options, or specific series.
IGNORE_STARTSWITH = [
    '912797PN1',  # Treasury Bond Series
]

SELECTED_COMP_BENCHMARK_KEY = '60% SPY / 40% AGG'                                           # SEE CONFIGURATION ABOVE FOR OPTIONS
RISK_TIME_HORIZON = None                                                                # Since Inception (full history)
USE_TEST_DATA = True                                                                    # True = bypass SFTP, use data/test_data/


#  ==========================================
#   HELPERS
#  ==========================================
# --- LOCAL AUTO-CLASSIFY ---
def auto_classify_asset(ticker: str, security_name: str) -> str:
    t = str(ticker).upper().strip()
    n = str(security_name).upper().strip()
    
    # 1. Hardcoded
    if t in ['USD', 'ICSH']: return 'Cash'
    if t in ['VEA', 'VWO', 'IMTM', 'VXUS']: return 'International Equities'
    if t in ['BND', 'VGIT', 'VGSH']: return 'Fixed Income'
    if t in ['BCI', 'GSG', 'VNQ']: return 'Alternative Assets'
    
    # 2. Keywords
    if any(k in n for k in ['INTL', 'EMERGING', 'EUROPE', 'ASIA', 'DEVELOPED']): return 'International Equities'
    if any(k in n for k in ['BOND', 'TREASURY', 'FIXED INC', 'AGGREGATE']): return 'Fixed Income'
    if any(k in n for k in ['REIT', 'REAL ESTATE', 'GOLD', 'COMMODITY', 'BITCOIN']): return 'Alternative Assets'

    # 3. If no keywords found, classify as US Equity
    return 'U.S. Equities'


def derive_account_id(raw_account: str, raw_name: str) -> str:
    """Derives a stable account pairing key from raw CSV metadata.
    
    For individual IBKR accounts (e.g. 'U20343697'), returns the ID as-is.
    For consolidated/flex-query CSVs (Account='Consolidated' or a person name),
    generates a synthetic key from the Name field so that quarterly and inception
    CSVs for the same person pair correctly.
    """
    raw_account = str(raw_account).strip()
    if re.match(r'^U\d+$', raw_account):
        return raw_account
    base_name = re.sub(r'\s+(Quarterly|Inception)$', '', str(raw_name).strip(), flags=re.IGNORECASE).strip()
    return f"CONSOL_{base_name}"


def extract_account_info(csv_path):
    """Quick-reads a CSV's Introduction row to extract account ID and name."""
    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 4 and row[0].strip() == 'Introduction' and row[1].strip() == 'Data':
                    raw_account = row[3].strip()
                    raw_name = row[2].strip()
                    account_id = derive_account_id(raw_account, raw_name)
                    return account_id, raw_name
    except Exception:
        pass
    return None, None


def discover_and_pair_accounts(decrypted_results):
    """Scans all decrypted CSVs, extracts account IDs, and pairs inception + quarterly by account.
    
    Returns: { account_id: { 'name': str, 'inception': path|None, 'quarterly': path|None } }
    """
    accounts = {}
    
    for category in ['inception', 'quarterly']:
        for csv_path in decrypted_results.get(category, []):
            if not csv_path.endswith('.csv'):
                continue
            account_id, account_name = extract_account_info(csv_path)
            if not account_id:
                print(f"   > Warning: Could not read account ID from {os.path.basename(csv_path)}")
                continue
            
            if account_id not in accounts:
                accounts[account_id] = {'name': account_name, 'inception': None, 'quarterly': None}
            
            # Keep the most recent file per account per category (by file modification time)
            existing = accounts[account_id][category]
            if existing is None or os.path.getmtime(csv_path) > os.path.getmtime(existing):
                accounts[account_id][category] = csv_path
    
    return accounts


# --- TEST DATA HELPERS ---
def extract_csv_metadata(csv_path):
    """Reads a CSV's Introduction row to extract account ID, name, and analysis end date."""
    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 8 and row[0].strip() == 'Introduction' and row[1].strip() == 'Data':
                    raw_account = row[3].strip()
                    account_name = row[2].strip()
                    account_id = derive_account_id(raw_account, account_name)
                    analysis_period = row[7].strip()
                    
                    end_date = None
                    if ' to ' in analysis_period:
                        end_part = analysis_period.split(' to ')[1]
                        paren_idx = end_part.find('(')
                        if paren_idx > 0:
                            end_part = end_part[:paren_idx].strip()
                        try:
                            end_date = pd.to_datetime(end_part).strftime('%Y-%m-%d')
                        except Exception:
                            pass
                    
                    return account_id, account_name, end_date
    except Exception:
        pass
    return None, None, None


def scan_test_data_folder(test_data_dir):
    """Scans a test_data folder for pre-decrypted CSVs organized in subdirectories.
    Classifies subdirectories as 'inception' or 'quarterly' based on folder name.
    
    Returns: { 'inception': [csv_paths], 'quarterly': [csv_paths], 'other': [] }
    """
    results = {'inception': [], 'quarterly': [], 'other': []}
    
    if not os.path.exists(test_data_dir):
        print(f"   > Test data directory not found: {test_data_dir}")
        return results
    
    for entry in sorted(os.listdir(test_data_dir)):
        entry_path = os.path.join(test_data_dir, entry)
        
        if not os.path.isdir(entry_path):
            continue
        
        category = 'inception' if 'inception' in entry.lower() else 'quarterly'
        csv_count = 0
        
        for filename in os.listdir(entry_path):
            if filename.endswith('.csv'):
                results[category].append(os.path.join(entry_path, filename))
                csv_count += 1
        
        print(f"   > [{category.upper():>9}] {entry}: {csv_count} CSV(s)")
    
    return results


def discover_and_pair_accounts_by_date(decrypted_results):
    """Date-aware account pairing. Pairs inception + quarterly CSVs by account ID,
    preferring inception files whose end date matches the quarterly end date.
    
    Returns: { account_id: { 'name': str, 'inception': path|None, 'quarterly': path|None } }
    """
    inventory = {'inception': [], 'quarterly': []}
    
    for category in ['inception', 'quarterly']:
        for csv_path in decrypted_results.get(category, []):
            if not csv_path.endswith('.csv'):
                continue
            account_id, account_name, end_date = extract_csv_metadata(csv_path)
            if not account_id:
                print(f"   > Warning: Could not read metadata from {os.path.basename(csv_path)}")
                continue
            inventory[category].append({
                'path': csv_path, 'account_id': account_id,
                'name': account_name, 'end_date': end_date
            })
    
    # Group quarterly by account_id (keep newest end_date per account)
    quarterly_by_account = {}
    for item in inventory['quarterly']:
        acct = item['account_id']
        if acct not in quarterly_by_account or (
            item['end_date'] and (quarterly_by_account[acct]['end_date'] is None or
            item['end_date'] > quarterly_by_account[acct]['end_date'])
        ):
            quarterly_by_account[acct] = item
    
    # Group inception by account_id (keep ALL candidates for date matching)
    inception_by_account = {}
    for item in inventory['inception']:
        acct = item['account_id']
        if acct not in inception_by_account:
            inception_by_account[acct] = []
        inception_by_account[acct].append(item)
    
    # Pair by account ID with date-aware inception selection
    accounts = {}
    all_ids = set(list(quarterly_by_account.keys()) + list(inception_by_account.keys()))
    
    for account_id in sorted(all_ids):
        q = quarterly_by_account.get(account_id)
        i_candidates = inception_by_account.get(account_id, [])
        
        name = q['name'] if q else (i_candidates[0]['name'] if i_candidates else 'Unknown')
        quarterly_path = q['path'] if q else None
        q_end = q['end_date'] if q else None
        inception_path = None
        
        if i_candidates:
            if q_end:
                # 1st: exact end-date match
                exact = [c for c in i_candidates if c['end_date'] == q_end]
                if exact:
                    inception_path = exact[0]['path']
                    print(f"   > {name} ({account_id}): Date match ✓ — both end {q_end}")
                else:
                    # 2nd: inception whose end_date covers the quarterly end (closest >=)
                    covering = sorted(
                        [c for c in i_candidates if c['end_date'] and c['end_date'] >= q_end],
                        key=lambda c: c['end_date']
                    )
                    if covering:
                        chosen = covering[0]
                        inception_path = chosen['path']
                        print(f"   > {name} ({account_id}): No exact date match — "
                              f"Quarterly ends {q_end}, using Inception ending {chosen['end_date']}")
                    else:
                        # 3rd: newest inception file (end date < quarterly — data gap warning)
                        fallback = sorted(i_candidates, key=lambda c: c['end_date'] or '', reverse=True)
                        inception_path = fallback[0]['path']
                        print(f"   > WARNING: {name} ({account_id}): Inception ends "
                              f"{fallback[0]['end_date']} — BEFORE quarterly end {q_end}")
            else:
                fallback = sorted(i_candidates, key=lambda c: c['end_date'] or '', reverse=True)
                inception_path = fallback[0]['path']
        
        accounts[account_id] = {
            'name': name,
            'quarterly': quarterly_path,
            'inception': inception_path
        }
    
    return accounts


#  ==========================================
#   PER-ACCOUNT REPORT GENERATION
#  ==========================================
def generate_report_for_account(quarter_csv, inception_csv, shared):
    """Generates a single PDF report for one account.
    
    Parameters:
        quarter_csv:   Path to the quarterly statement CSV
        inception_csv: Path to the since-inception statement CSV
        shared:        Dict with shared config/resources
        
    Returns:
        True on success, None on failure.
    """
    output_dir       = shared['output_dir']
    pdf_info         = shared['pdf_info']
    LOGO_FILE        = shared['logo_file']
    TEXT_LOGO_FILE   = shared['text_logo_file']
    
    # === 1. Load Data from Statements ===
    print(f"\n === 1. Ingesting Portfolio (Internal) ===")
    if not os.path.exists(quarter_csv):
        print(f"CRITICAL ERROR: Could not find Quarterly file at: {quarter_csv}")
        return None
    
    try:
        # === 1a. Load Quarter Statement ===
        PERIOD_FALLBACK_DATE = (pd.Timestamp.today().to_period("Q") - 1).start_time.strftime("%Y-%m-%d")
        portfolio_data = get_portfolio_holdings(quarter_csv, PERIOD_FALLBACK_DATE)
        
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
        inception_data = parse_since_inception_csv(inception_csv)
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
        traceback.print_exc()
        return None

    # === 2. Fetch Market Data from YF ===
    print("\n=== 2. Fetching Market Data (External) ===")
    
    # Get User Selection for Main Benchmark
    main_benchmark_weights = COMPOSITE_BENCHMARK_CONFIG.get(SELECTED_COMP_BENCHMARK_KEY, {'SPY': 1.0})
    main_bench_constituents = list(main_benchmark_weights.keys())
    
    # Gather ALL benchmarks needed (Standard + Main Constituents)
    standard_benchmarks = [t for sublist in BENCHMARK_CONFIG.values() for t in sublist]
    all_needed_tickers = list(set(standard_benchmarks + main_bench_constituents))
    
    # --- CALCULATE DATES FOR TRAILING RETURNS & RISK METRICS ---  
    if not daily_history.empty:
        portfolio_inception_date = pd.to_datetime(daily_history['date'].min())
    else:
        portfolio_inception_date = qs_date

    # Null out returns for periods where account hasn't been held long enough
    data_span_days = (rd_date - portfolio_inception_date).days
    if data_span_days < 365:
        window_returns['1Y'] = None
    if data_span_days < 365 * 3:
        window_returns['3Y'] = None

    five_years_ago_date = rd_date - pd.DateOffset(years=5)
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
        traceback.print_exc()

    # --- CALCULATE COMPOSITE BENCHMARK ---
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
    bench_windows, _ = calculate_period_returns(bench_nav_df, report_date)
    
    # Match the exact Portfolio dates for 'Quarter' and 'Inception'
    if not bench_nav_df.empty:
        nav_series = bench_nav_df.set_index('date')['nav']
        
        def get_exact_ret(start_dt, end_dt):
            val_start = nav_series.asof(start_dt)
            val_end = nav_series.asof(end_dt)
            
            if pd.isna(val_start) or pd.isna(val_end) or val_start == 0:
                return 0.0
            return (val_end / val_start) - 1.0

        # Calculate benchmark return for the exact Statement Quarter
        bench_windows['Quarter'] = get_exact_ret(qs_date, rd_date)
        
        # Calculate benchmark return for the exact Lifetime of the account
        if not daily_history.empty:
            bench_windows['Inception'] = get_exact_ret(portfolio_inception_date, rd_date)

    # Mirror account nulls: benchmark should dash where account dashes
    if data_span_days < 365:
        bench_windows['1Y'] = None
    if data_span_days < 365 * 3:
        bench_windows['3Y'] = None

    # --- CALCULATE RISK METRICS ---
    print(f"   > 2c. Calculating Risk Profile (Horizon: Since Inception) ---")
    try:
        risk_metrics = calculate_portfolio_risk(daily_history, main_benchmark_series)
        
        # Merge clean Inception CSV metrics
        if isinstance(inception_risk_measures, dict) and inception_risk_measures:
            risk_metrics.update(inception_risk_measures)  
            
    except Exception as e:
        print(f"Risk Calculation Error: {e}")
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
            portfolio_inception_date=portfolio_inception_date,
        )
        print(f"* DONE! Report Generated: {os.path.basename(PDF_FILE)} *")

    except Exception as e:
        print(f"Failed to write PDF: {e}")
        traceback.print_exc()
    
    return True


#  ==========================================
#   MAIN PIPELINE
#  ==========================================
def run_pipeline():
    # --- PATHS SETUP ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    output_dir = os.path.join(project_root, "output")
    
    if USE_TEST_DATA:
        # ---------------------------------------------------------
        # 0. TEST DATA MODE — Bypass SFTP, scan local test_data
        # ---------------------------------------------------------
        test_data_dir = os.path.join(project_root, "data", "test_data")
        print("\n === 0. TEST DATA MODE — Scanning local test_data folder ===")
        print(f"   > Source: {test_data_dir}")
        decrypted_results = scan_test_data_folder(test_data_dir)
        
        # Date-aware account pairing
        print("\n === 0b. Discovering & Pairing Accounts (Date-Aware) ===")
        account_pairs = discover_and_pair_accounts_by_date(decrypted_results)
    
    else:
        # ---------------------------------------------------------
        # 0. Fetch & Decrypt Files from IBKR
        # ---------------------------------------------------------
        raw_download_dir = os.path.join(project_root, "data", "raw_encrypted_downloads")
        decrypted_dir = os.path.join(project_root, "data", "raw_downloads")
        
        decrypted_results = {'inception': [], 'quarterly': [], 'other': []}
        
        print("\n === 0. Fetching Data from Remote Server ===")
        try:
            downloaded_files = fetch_files_via_sftp(
                host=IB_SFTP_HOST, 
                username=IB_USERNAME, 
                ssh_key_path=SSH_PRIVATE_KEY_PATH, 
                ssh_public_key_path=SSH_PUBLIC_KEY_PATH,  
                remote_dir=REMOTE_STATEMENT_DIR, 
                local_download_dir=raw_download_dir
            )
            
            if downloaded_files:
                decrypted_results = decrypt_pgp_files(
                    pgp_private_key_path=PGP_PRIVATE_KEY_PATH,
                    pgp_public_key_path=PGP_PUBLIC_KEY_PATH,  
                    encrypted_files=downloaded_files,
                    output_dir=decrypted_dir,
                    pgp_passphrase=PGP_PASSPHRASE
                )
        except Exception as e:
            print(f"Warning: Failed to fetch/decrypt new data. Proceeding with existing local files. Error: {e}")
        
        # ---------------------------------------------------------
        # 0b. Discover Accounts & Pair CSVs by Account ID
        # ---------------------------------------------------------
        print("\n === 0b. Discovering & Pairing Accounts ===")
        account_pairs = discover_and_pair_accounts(decrypted_results)
    
    if not account_pairs:
        print("   > No accounts discovered from SFTP downloads.")
        print("   > Tip: Ensure Inception and/or Quarterly Flex Query deliveries are configured in IBKR.")
        return
    
    # Print discovery summary
    for acct_id, info in account_pairs.items():
        status = []
        status.append("Quarterly ✓" if info['quarterly'] else "Quarterly ✗")
        status.append("Inception ✓" if info['inception'] else "Inception ✗")
        print(f"   > {info['name']} ({acct_id}): {', '.join(status)}")
    
    # Separate accounts into ready (both files) vs incomplete
    ready_accounts = {k: v for k, v in account_pairs.items() 
                      if v['inception'] and v['quarterly']}
    inception_only = {k: v for k, v in account_pairs.items() 
                      if v['inception'] and not v['quarterly']}
    quarterly_only = {k: v for k, v in account_pairs.items() 
                      if v['quarterly'] and not v['inception']}
    
    if inception_only:
        print(f"\n   > {len(inception_only)} account(s) skipped — missing Quarterly file:")
        for acct_id, info in inception_only.items():
            print(f"     - {info['name']} ({acct_id})")
    
    if quarterly_only:
        print(f"\n   > {len(quarterly_only)} account(s) skipped — missing Inception file:")
        for acct_id, info in quarterly_only.items():
            print(f"     - {info['name']} ({acct_id})")
    
    if not ready_accounts:
        print("\n   > No accounts have both Quarterly and Inception files. Cannot generate reports.")
        print("   > Ensure both Flex Query deliveries (Inception + Quarterly) are configured in IBKR.")
        return
    
    print(f"\n   > {len(ready_accounts)} account(s) ready for report generation")
    
    # ---------------------------------------------------------
    # 0c. Load Shared Resources (once for all accounts)
    # ---------------------------------------------------------
    INFO_FILE = os.path.join(project_root, "data", "info_for_pdf.xlsx")
    LOGO_FILE = os.path.join(project_root, "data", "pdf_resources", "logos", "gaard_logo.png")
    TEXT_LOGO_FILE = os.path.join(project_root, "data", "pdf_resources", "logos", "gaard_text_logo.png")
    
    pdf_info = {}
    if os.path.exists(INFO_FILE):
        print("   > SETUP: Loading PDF Info Sheet...")
        try:
            info_df = pd.read_excel(INFO_FILE, header=2)
            info_df.columns = [c.strip() if isinstance(c, str) else c for c in info_df.columns]
            if 'key' in info_df.columns and 'value' in info_df.columns:
                pdf_info = dict(zip(info_df['key'].dropna(), info_df['value'].dropna()))
            else:
                print(f"Warning: Expected columns 'key' and 'value' not found. Found: {info_df.columns.to_list()}")
        except Exception as e:
            print(f"Warning: Could not load Info File for PDF: {e}")
    
    shared = {
        'output_dir': output_dir,
        'pdf_info': pdf_info,
        'logo_file': LOGO_FILE,
        'text_logo_file': TEXT_LOGO_FILE,
    }
    
    # ---------------------------------------------------------
    # 1-3. Loop: Generate Report for Each Paired Account
    # ---------------------------------------------------------
    individual_success = 0
    individual_fail = 0
    consolidated_success = 0
    consolidated_fail = 0
    
    for i, (acct_id, info) in enumerate(ready_accounts.items(), 1):
        is_consolidated = acct_id.startswith('CONSOL_')
        label = "CONSOLIDATED" if is_consolidated else "INDIVIDUAL"
        
        print(f"\n{'='*70}")
        print(f"  [{label}] PROCESSING {i}/{len(ready_accounts)}: {info['name']} ({acct_id})")
        print(f"  Quarterly:  {os.path.basename(info['quarterly'])}")
        print(f"  Inception:  {os.path.basename(info['inception'])}")
        print(f"{'='*70}")
        
        try:
            result = generate_report_for_account(
                quarter_csv=info['quarterly'],
                inception_csv=info['inception'],
                shared=shared
            )
            if result is not None:
                if is_consolidated:
                    consolidated_success += 1
                else:
                    individual_success += 1
            else:
                if is_consolidated:
                    consolidated_fail += 1
                else:
                    individual_fail += 1
        except Exception as e:
            print(f"ERROR generating report for {info['name']} ({acct_id}): {e}")
            traceback.print_exc()
            if is_consolidated:
                consolidated_fail += 1
            else:
                individual_fail += 1
    
    # --- FINAL SUMMARY ---
    print(f"\n{'='*70}")
    print(f"  PIPELINE COMPLETE")
    print(f"  Individual Reports Generated: {individual_success}")
    print(f"  Consolidated Reports Generated: {consolidated_success}")
    if individual_fail:
        print(f"  Individual Failed: {individual_fail}")
    if consolidated_fail:
        print(f"  Consolidated Failed: {consolidated_fail}")
    if inception_only:
        print(f"  Skipped (no Quarterly): {len(inception_only)}")
    if quarterly_only:
        print(f"  Skipped (no Inception): {len(quarterly_only)}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    run_pipeline()
