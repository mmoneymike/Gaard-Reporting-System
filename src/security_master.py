import pandas as pd
import os

class SecurityMaster:
    def __init__(self, master_path):
        """
        Initializes the Security Master by loading the CSV map.    
        :param master_path: Path to the security_master.csv
        """
        self.master_path = master_path
        self._data = self._load_master()
        
        
    def _load_master(self):
        """Loads the CSV and sets the index to Ticker for fast lookups."""
        if not os.path.exists(self.master_path):
            raise FileNotFoundError(f"Security Master File not found at: {self.master_path}")
        
        df = pd.read_csv(self.master_path)
        
        # Clean headers (strip spaces, lowercase) to prevent KeyErrors
        df.columns = [c.strip().lower() for c in df.columns]
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        
        # Standardize tickers to uppercase
        df['ticker'] = df['ticker'].str.upper()
        return df.set_index('ticker')
    
    
    def get_asset_class(self, ticker):
        """Returns the asset class (Bucket) for a given ticker."""
        clean_ticker = ticker.strip().upper()
        
        try:
            return self._data.loc[clean_ticker, 'asset_class']
        except KeyError:
            return "Unclassified"
        
    
    def get_benchmarks(self, ticker):
        """
        REQUIRES A TICKER. 
        Returns the specific, ordered benchmarks ([Primary, Secondary]) for ONE asset.
        """
        clean_ticker = ticker.strip().upper()
        try:
            row = self._data.loc[clean_ticker]
        except KeyError:
            return []
        
        benchmarks = []
        if pd.notna(row.get('benchmark_primary')):
            benchmarks.append(row['benchmark_primary'])
        if pd.notna(row.get('benchmark_secondary')):
            benchmarks.append(row['benchmark_secondary'])
            
        return list(dict.fromkeys(benchmarks))


    def get_unique_benchmarks(self):
        """
        NO TICKER REQUIRED.
        Scans the entire CSV to find every unique benchmark used.
        """
        # 1. Grab all columns that have 'benchmark' in the name
        b_cols = [c for c in self._data.columns if 'benchmark' in c]
        
        # 2. Flatten all those columns into one big list of values
        all_values = self._data[b_cols].values.ravel('K')
        
        # 3. Filter for unique, non-empty strings
        unique_benchmarks = pd.unique(all_values)
        
        clean_list = [
            b for b in unique_benchmarks 
            if pd.notna(b) and str(b).strip() != ''
        ]
        
        return clean_list
    
    
    def get_all_assets_in_bucket(self, asset_class):
        """Returns a list of tickers taht belong to a specific bucket."""
        subset = self._data[self._data['asset_class'] == asset_class]
        if subset.empty:
            return []
        return subset.index.tolist()
    
    
    def get_all_downloaded_tickers(self):
        """Returns a master list of all Assets + Benchmarks."""
        # 1. Get all Assets
        assets = self._data.index.tolist()
        
        #2 Get all Benchmarks (Primary & Secondary)
        b_cols = [c for c in self._data.columns if 'benchmark' in c]
        
        # Extract unique values from these columns   
        raw_benchmarks = pd.unique(self._data[b_cols].values.ravel('K'))  
        
        # Clean out NaNs
        clean_benchmarks = [b for b in raw_benchmarks if pd.notna(b)]
        
        # 3 Combine and Remove Duplicates
        return list(set(assets + clean_benchmarks))  
    
    