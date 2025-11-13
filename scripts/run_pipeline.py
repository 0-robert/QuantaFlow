# scripts/run_pipeline.py
from etl.extract import DataExtractor
from etl.validate import validate_dataframe, PriceRecord, FundamentalRecord, NewsRecord
from etl.transform import DataTransformer
from etl.load import DataLoader
from config.settings import settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_full_pipeline(symbols: list):
    '''Execute complete ETL pipeline'''
    
    logger.info("Starting ETL pipeline...")
    
    #============= 1) Extract
    logger.info("Extracting data...")
    extractor = DataExtractor()
    
    prices_raw = extractor.fetch_yahoo_prices(symbols)
    fundamentals_raw = extractor.fetch_fundamentals(symbols)
    news_raw = extractor.fetch_news_sentiment(symbols, settings.NEWS_API_KEY)
    
    print(prices_raw.shape)
    print(prices_raw.columns)

    print(fundamentals_raw.shape)
    print(fundamentals_raw.columns)

    print(news_raw.shape)
    print(news_raw.columns)


    logger.info(f"Extracted {len(prices_raw)} price records")
    logger.info(f"Extracted {len(fundamentals_raw)} fundamental records")
    logger.info(f"Extracted {len(news_raw)} news articles")
    
    #============= 2) Validate
    logger.info("Validating data...")
    
    prices_valid, price_errors = validate_dataframe(prices_raw, PriceRecord)

    fundamentals_valid, fund_errors = validate_dataframe(fundamentals_raw, FundamentalRecord)
    news_valid, news_errors = validate_dataframe(news_raw, NewsRecord)
    
    if price_errors:
        logger.warning(f"Found {len(price_errors)} price validation errors")
    if fund_errors:
        logger.warning(f"Found {len(fund_errors)} fundamental validation errors")
        for i in fund_errors:
            logger.error(i)
    
    #============= 3) Transform
    logger.info("Transforming data...")
    
    transformer = DataTransformer()
    prices_transformed = transformer.transform_pipeline(prices_valid)
    
    #============= 4) Load 
    logger.info("Loading to storage...")
    
    loader = DataLoader()
    
    # Save to Parquet (for archival/backup)
    loader.save_to_parquet(prices_transformed, "prices")
    logger.info("Loaded prices to storage +--")


    loader.save_to_parquet(fundamentals_valid, "fundamentals")
    logger.info("Loaded fundamentals to storage ++-")

    loader.save_to_parquet(news_valid, "news")
    logger.info("Loaded news to storage +++")

    
    # Load to DuckDB (for querying)
    loader.load_to_duckdb(prices_transformed, "prices")    
    logger.info("Loaded prices to DuckDB +--")

    loader.load_to_duckdb(fundamentals_valid, "fundamentals")    
    logger.info("Loaded fundamentals to DuckDB ++-")

    loader.load_to_duckdb(news_valid, "news")    
    logger.info("Loaded news to DuckDB +++")

    
    # Create indexes
    loader.create_indexes()
    
    # Log pipeline run
    loader.log_pipeline_run("prices", len(prices_transformed), "SUCCESS")
    
    logger.info("Pipeline completed successfully!")

if __name__ == "__main__":
    # Test with a few symbols
    SYMBOLS = ["AAPL"]
    run_full_pipeline(SYMBOLS)