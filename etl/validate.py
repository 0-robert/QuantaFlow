from pydantic import BaseModel, field_validator
from datetime import datetime
from typing import Optional
import polars as pl

class IntradayBar(BaseModel):
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: Optional[float] = None  # Volume-weighted average price
    trades: Optional[int] = None  # Number of trades
    
    # Derived fields
    returns: Optional[float] = None
    volatility_20min: Optional[float] = None
    rsi: Optional[float] = None
    volume_ratio: Optional[float] = None

class MacroIndicator(BaseModel):
    date: datetime
    indicator: str  # 'vix', 'treasury_10y', etc.
    value: float

class EnhancedFundamental(BaseModel):
    symbol: str
    market_cap: Optional[float]
    pe_ratio: Optional[float]
    peg_ratio: Optional[float]
    price_to_book: Optional[float]
    roe: Optional[float]  
    profit_margin: Optional[float]
    operating_margin: Optional[float]
    debt_to_equity: Optional[float]
    beta: Optional[float]
    dividend_yield: Optional[float]
    sector: Optional[str]
    industry: Optional[str]
    week_52_high: Optional[float]
    week_52_low: Optional[float]

class SECFiling(BaseModel):
    symbol: str
    filing_date: datetime
    form_type: str  # '8-K', '13F-HR', '10-Q', etc.
    accession_number: str
    url: Optional[str] = None

# OLD

class PriceRecord(BaseModel):
    Date: datetime
    Open: float
    High: float
    Low: float
    Close: float
    Volume: int
    symbol: str
    Dividends: Optional[float] = None
    Stock_Splits: Optional[float] = None
    Capital_Gains: Optional[float] = None

    @field_validator("High", "Low", "Open", "Close")
    def prices_positive(cls, val):
        if val <= 0:
            raise ValueError("Price MUST be positive")
        return val
    
    @field_validator("Volume")
    def volume_positive(cls, val):
        if val < 0:
            raise ValueError("Volume can't be negative")
        return val
    
class FundamentalRecord(BaseModel):
    '''For storing the fundamentals from YahooFinance'''
    symbol: str
    market_cap: Optional[float]
    pe_ratio: Optional[float]
    dividend_yield: Optional[float]
    beta: Optional[float]
    sector: Optional[str] = None
    industry: Optional[str] = None

class NewsRecord(BaseModel):
    symbol: str
    title: str
    published_at: str
    source: str

def validate_dataframe(df, schema):
    '''Validate entire dataframe against pydantic schema'''
    validated = []
    errors = []

    for row in df.to_dicts():
        try:
            validated_record = schema(**row) # attempt to convert row -> pydantic model
            validated.append(validated_record.model_dump()) # pydantic model -> (back to) dict and saved
        except Exception as e:
            errors.append({"row": row, "error": str(e)})

    return pl.DataFrame(validated), errors