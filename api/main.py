from fastapi import FastAPI, HTTPException
import duckdb
from typing import Optional
from datetime import date

app = FastAPI(title="QuantaFlow API", description="Financial DataLake API")

DB_PATH = "data/lake/quantaflow.duckdb"


@app.get("/")
def root():
    return {
        "message": "QuantaFlow DataLake API",
        "endpoints": [
            "/prices/{symbol}",
            "/returns/{symbol}",
            "/fundamentals/{symbol}",
            "/sentiment/{symbol}",
            "/query",
            "/stats"
        ]
    }

@app.get("/prices/{symbol}")
def get_prices(symbol: str, start_date: Optional[date] = None, end_date: Optional[date]=None, limit: int = 1000):
    '''Get prices for a symbol'''
    con = duckdb.connect(DB_PATH, read_only=True)

    query = f"SELECT * FROM prices WHERE symbol = '{symbol}'"
    
    if start_date:
        query += f" AND Date >= '{start_date}'"
    if end_date:
        query += f" AND Date <= '{end_date}'"
    
    query += f" ORDER BY Date DESC LIMIT {limit}"
    
    try:
        result = con.execute(query).df()
        con.close()
        return result.to_dict(orient="records")
    except Exception as e:
        con.close()
        raise HTTPException(status_code=404, detail=str(e))
    

@app.get("/returns/{symbol}")
def get_returns(symbol: str, period: str = "1y"):
    '''Get returns data with technical indicators'''
    con = duckdb.connect(DB_PATH, read_only=True)
    
    query = f'''
        SELECT 
            Date,
            symbol,
            Close,
            daily_return,
            log_return,
            sma_20,
            sma_50,
            sma_200,
            volatility_20d
        FROM prices 
        WHERE symbol = '{symbol}'
        AND Date >= CURRENT_DATE - INTERVAL '{period}'
        ORDER BY Date DESC
    '''
    
    try:
        result = con.execute(query).df()
        con.close()
        return result.to_dict(orient="records")
    except Exception as e:
        con.close()
        raise HTTPException(status_code=404, detail=str(e))
    
@app.get("/fundamentals/{symbol}")
def get_fundamentals(symbol: str):
    '''Get company fundamentals'''
    con = duckdb.connect(DB_PATH, read_only=True)
    
    try:
        result = con.execute(f'''
            SELECT * FROM fundamentals 
            WHERE symbol = '{symbol}'
        ''').df()

        con.close()
        
        if len(result) == 0:
            raise HTTPException(status_code=404, detail="Symbol not found")
        
        return result.to_dict(orient="records")[0]
    except Exception as e:
        con.close()
        raise HTTPException(status_code=404, detail=str(e))
    

@app.get("/sentiment/{symbol}")
def get_sentiment(symbol: str, limit: int = 10):
    '''Get recent news for symbol'''
    con = duckdb.connect(DB_PATH, read_only=True)
    
    try:
        result = con.execute(f'''
            SELECT * FROM news 
            WHERE symbol = '{symbol}'
            ORDER BY published_at DESC
            LIMIT {limit}
        ''').df()
        con.close()
        return result.to_dict(orient="records")
    except Exception as e:
        con.close()
        raise HTTPException(status_code=404, detail=str(e))
    

@app.post("/query")
def custom_query(sql: str):
    """Execute custom SQL query (advanced users)"""
    con = duckdb.connect(DB_PATH, read_only=True)
    
    # Security: Only allow SELECT
    if not sql.strip().upper().startswith("SELECT"):
        raise HTTPException(status_code=400, detail="Only SELECT queries allowed")
    
    try:
        result = con.execute(sql).df()
        con.close()
        return result.to_dict(orient="records")
    except Exception as e:
        con.close()
        raise HTTPException(status_code=400, detail=str(e))
    



@app.get("/stats")
def get_stats():
    """Get database statistics"""
    con = duckdb.connect(DB_PATH, read_only=True)
    
    stats = {}
    
    # Count rows in each table
    for table in ["prices", "fundamentals", "news"]:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            stats[table] = count
        except:
            stats[table] = 0
    
    # Date range
    try:
        date_range = con.execute('''
            SELECT MIN(Date) as start, MAX(Date) as end 
            FROM prices
        ''').df()
        stats["date_range"] = date_range.to_dict(orient="records")[0]
    except:
        pass
    
    con.close()
    return stats