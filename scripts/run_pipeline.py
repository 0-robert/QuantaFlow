from etl import DataExtractor

extractor = DataExtractor()
prices = extractor.fetch_yahoo_prices(["AAPL", "MSFT", "TSLA"])
print(prices.head())