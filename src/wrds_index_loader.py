import pandas as pd
try:
    import wrds
except ImportError:
    wrds = None

def get_wrds_connection():
    """Establishes connection to WRDS."""
    if wrds is None:
        raise ImportError("WRDS library not installed.")
    print("Connecting to WRDS...")
    return wrds.Connection()

def fetch_benchmark_returns_wrds(connection, tickers, start_date=None):
    """
    Fetches Daily Total Returns (including divs) for benchmarks from CRSP.
    """
    clean_tickers = [t.upper() for t in tickers]
    formatted_tickers = "', '".join(clean_tickers)
    
    # 1. Get Dates & PERMNOs (IDs)
    date_clause = f"AND dsf.date >= '{start_date}'" if start_date else ""
    
    # Query: Join StockNames (to get Ticker) with DSF (Daily Stock File)
    # FIX: Changed 'nameendt' to 'nameenddt'
    query = f"""
        SELECT 
            dsf.date, 
            sn.ticker, 
            dsf.ret 
        FROM crsp.dsf AS dsf
        JOIN crsp.stocknames AS sn
            ON dsf.permno = sn.permno 
            AND dsf.date BETWEEN sn.namedt AND sn.nameenddt
        WHERE sn.ticker IN ('{formatted_tickers}')
        {date_clause}
        ORDER BY dsf.date
    """
    
    print(f"Querying WRDS for {len(clean_tickers)} benchmarks...")
    try:
        data = connection.raw_sql(query)
    except Exception as e:
        print(f"WRDS Query Failed: {e}")
        return pd.DataFrame()
        
    if data.empty:
        return pd.DataFrame()

    # 2. Pivot to Time Series (Date x Ticker)
    data['date'] = pd.to_datetime(data['date'])
    pivot_rets = data.pivot(index='date', columns='ticker', values='ret')
    
    return pivot_rets
