import yfinance as yf
from config.settings import settings
from alpha_vantage.fundamentaldata import FundamentalData

import requests
from datetime import datetime, timedelta
import polars as pl

from fredapi import Fred

import time

class DataExtractor:

    def __init__(self):
        self.fred = Fred(api_key=settings.FRED_API_KEY)

    def fetch_polygon_minute_bars(self, symbols: list, start_date: str, end_date: str):
        '''
        Polygon.io: Up to 2 years of 1-minute bars apparently (free tier)
        Should be 390 bars/day * 252 days = 98,280 bars per symbol
        '''
        all_bars = []
        
        for symbol in symbols:
            url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}"
            params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": settings.POLYGON_KEY}
            
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                if "results" in data:
                    for bar in data["results"]:
                        all_bars.append({
                            "symbol": symbol,
                            "timestamp": datetime.fromtimestamp(bar["t"] / 1000),
                            "open": bar["o"],
                            "high": bar["h"],
                            "low": bar["l"],
                            "close": bar["c"],
                            "volume": bar["v"],
                            "vwap": bar["vw"],
                            "trades": bar["n"]  # Number of trades (liquidity measure)
                        })
            
            time.sleep(12)  # Free tier: 5 requests/min
        
        return pl.DataFrame(all_bars)

    def fetch_alpha_vantage_intraday(self, symbol: str, interval="1min"):
        '''
        Backup source: Alpha Vantage 1-min bars (last 30 days)
        Use for real-time updates after Polygon historical pull
        '''
        url = f"https://www.alphavantage.co/query"
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": interval,
            "apikey": settings.ALPHA_VANTAGE_KEY,
            "outputsize": "full"
        }
        
        response = requests.get(url, params=params)
        data = response.json()
        
        bars = []
        time_series = data.get(f"Time Series ({interval})", {})
        
        for timestamp, values in time_series.items():
            bars.append({
                "symbol": symbol,
                "timestamp": datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S"),
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"])
            })
        
        return pl.DataFrame(bars)

    def calculate_technical_indicators(self, df: pl.DataFrame):
        '''
        Add 20+ technical indicators that quants actually use
        This transforms raw prices into actionable features
        '''
        df = df.sort(["symbol", "timestamp"])
        
        # Group by symbol for rolling calculations
        df = df.with_columns([
            # Returns
            (pl.col("close") / pl.col("close").shift(1) - 1).alias("returns"),
            (pl.col("close") / pl.col("close").shift(5) - 1).alias("returns_5min"),
            
            # Moving averages (capture trend)
            pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
            pl.col("close").rolling_mean(window_size=50).over("symbol").alias("sma_50"),
            pl.col("close").ewm_mean(span=12).over("symbol").alias("ema_12"),
            
            # Volatility (key for regime detection)
            pl.col("returns").rolling_std(window_size=20).over("symbol").alias("volatility_20min"),
            
            # Volume indicators (institutional activity)
            (pl.col("volume") / pl.col("volume").rolling_mean(20).over("symbol")).alias("volume_ratio"),
            
            # Microstructure (spoofing detection)
            (pl.col("high") - pl.col("low")).alias("range"),
            ((pl.col("high") - pl.col("low")) / pl.col("close")).alias("range_pct"),
            
            # Order flow imbalance proxy
            ((pl.col("close") - pl.col("low")) - (pl.col("high") - pl.col("close"))).alias("buying_pressure")
        ])
        
        # RSI (Relative Strength Index)
        df = df.with_columns([
            pl.when(pl.col("returns") > 0)
              .then(pl.col("returns"))
              .otherwise(0)
              .rolling_mean(14)
              .over("symbol")
              .alias("gain_avg"),
            
            pl.when(pl.col("returns") < 0)
              .then(-pl.col("returns"))
              .otherwise(0)
              .rolling_mean(14)
              .over("symbol")
              .alias("loss_avg")
        ]).with_columns([
            (100 - (100 / (1 + pl.col("gain_avg") / pl.col("loss_avg")))).alias("rsi")
        ])
        
        return df

    def fetch_finra_short_interest(self, symbols: list):
        '''
        FINRA short interest: Institutional bearish positioning
        Updated twice monthly - critical for squeeze detection
        '''
        # FINRA provides this via their API or downloadable files
        # Implementation depends on current FINRA data format
        # TODO
        pass

    def fetch_sec_filings(self, symbols: list):
        '''
        SEC EDGAR: 13F filings show institutional holdings
        8-K: Material events
        This is the data Bloomberg terminals use
        '''
        filings = []
        
        for symbol in symbols:
            # SEC EDGAR API endpoint
            url = f"https://data.sec.gov/submissions/CIK{self._get_cik(symbol)}.json"
            headers = {"User-Agent": "YourName your@email.com"}  # SEC requires this
            
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                recent_filings = data.get("filings", {}).get("recent", {})
                
                # Extract 8-K (material events) and 13-F (holdings)
                for i, form_type in enumerate(recent_filings.get("form", [])):
                    if form_type in ["8-K", "13F-HR"]:
                        filings.append({
                            "symbol": symbol,
                            "filing_date": recent_filings["filingDate"][i],
                            "form_type": form_type,
                            "accession_number": recent_filings["accessionNumber"][i]
                        })
            
            time.sleep(0.1)  # Be nice to SEC servers
        # TODO FinBERT analysis
        return pl.DataFrame(filings)

    def _get_cik(self, symbol: str) -> str:
        '''Convert ticker to CIK (SEC identifier)'''
        # Use SEC's ticker-to-CIK mapping
        url = "https://www.sec.gov/files/company_tickers.json"
        response = requests.get(url)
        data = response.json()
        
        for item in data.values():
            if item["ticker"] == symbol:
                return str(item["cik_str"]).zfill(10)
        return None

    def fetch_macro_indicators(self):
            '''
            Federal Reserve Economic Data (FRED)
            These determine market regimes (bull/bear/crisis)
            '''
            indicators = {
                "vix": "VIXCLS",  # VIX Index
                "treasury_10y": "DGS10",  # 10-Year Treasury
                "treasury_2y": "DGS2",  # 2-Year Treasury
                "credit_spread": "BAMLH0A0HYM2",  # High Yield spread
                "unemployment": "UNRATE",
                "fed_funds": "FEDFUNDS"
            }
            
            macro_data = []
            
            for name, series_id in indicators.items():
                data = self.fred.get_series(series_id)
                
                for date, value in data.items():
                    macro_data.append({
                        "date": date,
                        "indicator": name,
                        "value": float(value) if value else None
                    })
            
            return pl.DataFrame(macro_data)
    
    def fetch_enhanced_fundamentals(self, symbols: list):
        '''
        Beyond basic metrics - get quarterly earnings, guidance, estimates
        '''
        fd = FundamentalData(key=settings.ALPHA_VANTAGE_KEY, output_format='pandas')
        fundamentals = []
        
        for symbol in symbols:
            try:
                # Company overview
                overview, _ = fd.get_company_overview(symbol)
                
                # Earnings data (quarterly)
                earnings, _ = fd.get_earnings(symbol)
                
                # Cash flow statement
                cashflow, _ = fd.get_cash_flow_annual(symbol)
                
                row = overview.iloc[0]
                
                fundamentals.append({
                    "symbol": symbol,
                    "market_cap": float(row["MarketCapitalization"]) if row["MarketCapitalization"] else None,
                    "pe_ratio": float(row["PERatio"]) if row["PERatio"] else None,
                    "peg_ratio": float(row["PEGRatio"]) if row["PEGRatio"] else None,
                    "price_to_book": float(row["PriceToBookRatio"]) if row["PriceToBookRatio"] else None,
                    "roe": float(row["ReturnOnEquityTTM"]) if row["ReturnOnEquityTTM"] else None,
                    "profit_margin": float(row["ProfitMargin"]) if row["ProfitMargin"] else None,
                    "operating_margin": float(row["OperatingMarginTTM"]) if row["OperatingMarginTTM"] else None,
                    "debt_to_equity": float(row["DebtToEquity"]) if row["DebtToEquity"] else None,
                    "beta": float(row["Beta"]) if row["Beta"] else None,
                    "dividend_yield": float(row["DividendYield"]) if row["DividendYield"] else None,
                    "sector": row["Sector"],
                    "industry": row["Industry"],
                    "52_week_high": float(row["52WeekHigh"]) if row["52WeekHigh"] else None,
                    "52_week_low": float(row["52WeekLow"]) if row["52WeekLow"] else None
                })
                
                time.sleep(12)
                
            except Exception as e:
                print(f"Failed to fetch {symbol}: {e}")
        
        return pl.DataFrame(fundamentals)

    # OLD FUNCTIONS. UNUSED

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
            try:
                # Fetch company overview
                data, _ = fd.get_company_overview(symbol)

                # data is a single-row DataFrame; get scalars with .iloc[0]
                row = data.iloc[0]

                fundamentals.append({
                    "symbol": symbol,
                    "market_cap": float(row["MarketCapitalization"]) if row["MarketCapitalization"] else None,
                    "pe_ratio": float(row["PERatio"]) if row["PERatio"] else None,
                    "dividend_yield": float(row["DividendYield"]) if row["DividendYield"] else None,
                    "beta": float(row["Beta"]) if row["Beta"] else None,
                    "sector": row["Sector"],
                    "industry": row["Industry"]
                })

                # Sleep 12s to avoid hitting free tier rate limit (5 requests/min)
                time.sleep(12)

            except Exception as e:
                print(f"Failed to fetch {symbol}: {e}")
                fundamentals.append({
                    "symbol": symbol,
                    "market_cap": None,
                    "pe_ratio": None,
                    "dividend_yield": None,
                    "beta": None,
                    "sector": None,
                    "industry": None
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