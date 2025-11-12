import polars as pl
import numpy as np 

class DataTransformer:
    def calculate_returns(self, df: pl.DataFrame):
        '''Calculate both daily and cumulative returns'''

        return df.sort(["symbol", "Date"]).with_columns([
            # daily (% change from previous day)
            (pl.col("Close").pct_change().over("symbol")).alias("daily_return"),
            # log returns (better for compounding) (log(today) - log(yesterday) = log return)
            (pl.col("Close").log() - pl.col("Close").shift(1).log()).over("symbol").alias("log_return")

        ])
    
    def add_technical_indicators(self, df: pl.DataFrame):
        '''Calculate moving averages + volatility'''
        return df.sort(["symbol", "Date"]).with_columns([
            # Simple moving averages, (20 approximate trading month, 50 approx 2-3 trading months, 200 approx trading year)
            pl.col("Close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
            pl.col("Close").rolling_mean(window_size=50).over("symbol").alias("sma_50"),
            pl.col("Close").rolling_mean(window_size=200).over("symbol").alias("sma_200")

            # Volatility (20 day rolling std of returns)
            pl.col("daily_return").rolling_std(window_size=20).over("symbol").alias("volatility_20d"),

              # Volume moving average
            pl.col("Volume").rolling_mean(window_size=20).over("symbol").alias("avg_volume_20d"),

        ])
    
    def normalise_prices(self, df: pl.DataFrame):
        '''Normalise Prices to start at 100 (by dividing all by initial price)'''
        return df.with_columns([
            (pl.col("Close") / pl.col("Close").first()).over("symbol").alias("normalised_prices")
        ])
    
    def transform_pipeline(self, df: pl.dataFrame):
        '''Run full tranformation pipeline'''

        df = self.calculate_returns(df)
        df = self.add_technical_indicators(df)
        df = self.normalise_prices(df)
        return df