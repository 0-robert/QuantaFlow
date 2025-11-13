from pydantic import BaseModel, field_validator
from datetime import datetime
from typing import Optional
import polars as pl

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