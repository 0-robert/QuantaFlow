import yfinance as yf
from config.settings import settings

import requests
from datetime import datetime, timedelta
import polars as pl

import time

class DataExtractor:

    def fetch_yahoo_prices(self, symbols: list, period="1y"):
        '''Fetch OHLCV (open,high,low,close,volume) (yahoo finance)'''

        data_frames = []

        for symbol in symbols:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period)
            df = pl.from_pandas(df.reset_index()) # polars is better for speed (rather than pandas)
            df = df.with_columns(pl.lit(symbol).alias("symbol"))

            cols_to_drop = ["Capital Gains", "Dividends", "Stock Splits"]
            df = df.drop([c for c in cols_to_drop if c in df.columns])
            # Focusing on core price data for simplicity, thus i'm excluding Dividends, Stock Splits, Capital Gains
            # for simplicity — can be added later if needed for total return analytics.

            data_frames.append(df)


            print(df.columns)
            print(df.shape)

        return pl.concat(data_frames)
    
    def fetch_fundamentals(self, symbols: list):
        '''Fetches the fundamentals (of a company), yahooFinance was too flimsly so I use AlphaVantage instead'''
        fd = FundamentalData(key=settings.ALPHA_VANTAGE_KEY, output_format='pandas')

        fundamentals = []

        for symbol in symbols:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            fundamentals.append({
                "symbol": symbol,
                "market_cap": info.get("marketCap"),
                "pe_ratio": info.get("trailingPE"),
                "dividend_yield": info.get("dividendYield"),
                "beta": info.get("beta"),
                "sector": info.get("sector"),
                "industry": info.get("industry")
            })
        
        return pl.DataFrame(fundamentals)
    
    def fetch_news_sentiment(self, symbols: list, api_key: str):
        """Fetch news for sentiment analysis from newsapi.org"""
        all_news = []
        
        for symbol in symbols:
            url = f"https://newsapi.org/v2/everything?q={symbol}&apiKey={api_key}&pageSize=10"
            response = requests.get(url)
            
            if response.status_code == 200:
                articles = response.json()["articles"]
                for article in articles:
                    all_news.append({
                        "symbol": symbol,
                        "title": article["title"],
                        "published_at": article["publishedAt"],
                        "source": article["source"]["name"]
                    })
        
        return pl.DataFrame(all_news)