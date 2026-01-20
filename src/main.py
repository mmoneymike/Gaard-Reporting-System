import pandas as pd
from src.statement_ingestion import get_portfolio_holdings
from src.wrds_index_loader import get_wrds_connection, fetch_benchmark_returns_wrds

# ==========================================
# HARDCODED BENCHMARK CONFIGURATION - Update as necessary
# ==========================================
BENCHMARK_CONFIG = {
    'U.S. Equities':        ['SPY', 'IWV'],  
    'International Equities': ['ACWI', 'VXUS'], 
    'Fixed Income':         ['AGG', 'BND'],
    'Real Assets':          ['VNQ', 'GLD'],  
    'Alternative Assets':   ['VNQ', 'GLD'], # Mapped Alternatives here too
    'Cash':                 ['BIL'],
    'Unclassified':         []
}

def run_pipeline():
    IBKR_FILE = "data/U21244041_20250730_20260112.csv"  # !! HARDCODED !!
    
    print("--- 1. Ingesting Portfolio (Internal) ---")
    try:
        # UNPACKING TUPLE: Now we get the Date automatically
        holdings, report_date = get_portfolio_holdings(IBKR_FILE)
        
        print(f"Successfully loaded {len(holdings)} positions.")
        print(f"Report Generated Date: {report_date}")
        
    except FileNotFoundError:
        print(f"Error: File not found at {IBKR_FILE}")
        return