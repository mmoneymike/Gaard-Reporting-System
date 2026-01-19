import pandas as pd
import numpy as np

class PortfolioAggregator:
    def __init__(self, holdings_df, prices_df, start_value=100.0):
        """
        :param holdings_df: DF with 'ticker', 'weight'
        :param prices_df: DF with Close Prices (returned DF from data_ingestion.py)
        """
        self.holdings = holdings_df
        self.prices = prices_df
        self.start_value = start_value
        # Calculate daily % returns for the raw assets
        self.asset_returns = self.prices.pct_change().fillna(0)
    
    def calculate_bucket_indices(self):
        """
        Calculates a 'Synthetic Price History' for:
        1. Total Portfolio
        2. Each Asset Class
        
        Returns: DF of Prices starting at start_value (currently hardcoded)
        """
        weighted_returns = pd.DataFrame(index=self.asset_returns.index)
        start_value = self.start_value
        
        # 1. Total Portfolio Return
        weighted_returns['Portfolio'] = self._get_weighted_return(
            self.holdings['ticker'].tolist(),
            self.holdings['weight'].tolist()
        )
        
        # 2. Asset Class Buckets
        unique_classes = self.holdings['asset_class'].unique()
        for asset_class in unique_classes:
            if asset_class == 'Unclassified': continue
            
            # Filter holdings jsut for this class
            ind_class = self.holdings[self.holdings['asset_class'] == asset_class]
            
            # Normalize weights to see how unique bucket performed on its own
            total_weight = ind_class['weight'].sum()
            if total_weight == 0: continue # no assets in this class
            norm_weights = (ind_class['weight'] / total_weight).tolist()
            
            weighted_returns[asset_class] = self._get_weighted_return(
                ind_class['ticker'].tolist(),
                norm_weights
            )
        
        # Create Index (Start at Price = 100)
        return start_value * (1 + weighted_returns).cumprod()
    
    def get_aligned_benchmarks(self, benchmark_tickers):
        """
        Takes raw benchmarks (like SPY at $500), aligns them to
        portfolio dates, and normalizes them to start at 100.
        """
        valid = [b for b in benchmark_tickers if b in self.prices.columns]
        if not valid: return pd.DataFrame()
            
        # Extract and Align dates
        bench_data = self.prices[valid].copy()
        bench_data = bench_data.reindex(self.asset_returns.index).ffill()
        
        # Normalize to start_value so they compare fairly with your buckets
        # (Avoids division by zero if data is missing)
        first_valid = bench_data.iloc[0].replace(0, np.nan)
        normalized = bench_data / first_valid * self.start_value
        
        return normalized
    
    def _get_weighted_return(self, tickers, weights):
        """Helper: Dot product of Treutnr * Weights"""
        valid_tickers = [t for t in tickers if t in self.asset_returns.columns]
        if not valid_tickers: return 0.0
        
        # Align data
        subset_returns = self.asset_returns[valid_tickers]
        weight_map = dict(zip(tickers, weights))
        aligned_weights = [weight_map[t] for t in valid_tickers]
        
        # Matrix Max: Returns * Weights
        return subset_returns.dot(aligned_weights)
    


def get_cumulative_return(prices: pd.Series, window: str):
    """
    Universal functon to calculate returns over any window.
    
    :param prices: A panda Series of daily prices (Adj Close).
    :param window: '1M', '3M', '6M', 'YTD', '1Y', 'INCEPTION', or 'SINCE'
    """
    if prices is None or prices.empty:
        return 0.0
    
    # Clean Data
    price_series = prices.dropna().sort_index()
    if price_series.empty:
        return 0.0
    if not isinstance(price_series.index, pd.DatetimeIndex):
        raise TypeError("Prices must have a DatetimeIndex")
    
    end_date = price_series.index[-1]
    end_price = float(price_series.iloc[-1])
    
    # 1. Determine Start Date
    window = window.upper()
    
    if window == "INCEPTION":
        start_date = price_series.index[0]
        
    elif window == "YTD":
        start_date = pd.Timestamp(end_date.year, 1, 1)
        
    elif window.startswith("SINCE_"):
        start_date = pd.Timestamp(window.replace("SINCE_",""))
    
    elif window.endswith('Y') and window[:-1].isdigit():
            years = int(window[:-1])
            start_date = end_date - pd.DateOffset(years=years)
    
    elif window.endswith('M') and window[:-1].isdigit():
        months = int(window[:-1])
        start_date = end_date - pd.DateOffset(months=months)
    else:
        try:
            start_date = end_date - pd.tseries.frequencies.to_offset(window)
        except Exception:
            raise ValueError(f"Invalid window spec: {window}. Valid units are M, Y, YTD, INCEPTION, SINCE_date")
        
    # 2. Get Start Price
    # Case: Requested Start Date is earlier than earliest available Data
    if start_date is None or start_date < price_series.index[0]:
        start_date = price_series.index[0]
    
    start_price = price_series.asof(start_date)
    
    # Case: Missing or 0 Start Price
    if pd.isna(start_price) or start_price == 0:
        start_price = float(price_series.iloc[0])
    
    return (end_price / start_price) - 1.0
    
    