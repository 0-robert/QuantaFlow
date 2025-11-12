import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime

class DataLoader:
    def __init__(self, db_path="data/lake/quantaflow.duckdb"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    def save_to_parquet(self, df: pl.DataFrame, table_name: str):
        '''Save the dataframe as a partitioned Parquet'''
        output_dir = f'data/processed/{table_name}'
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Partition the data by symbol

        for symbol in df["symbol"].unique():
            symbol_df = df.filter(pl.col("symbol") == symbol)
            file_path = f"{output_dir}/{symbol}.parquet"
            symbol_df.write_parquet(file_path, compression="snappy") # Even though less aggressive compression, snappy is speedy

            return output_dir

        return output_dir


    def load_to_duckdb(self, df: pl.DataFrame, table_name: str):
        'Load the df into DuckDB'
        con = duckdb.connect(self.db_path)

        # Create if doesn't exist
        con.execute(f'''
                    CREATE TABLE IF NOT EXISTS {table_name} AS
                    SELECT * FROM df WHERE 1=0''')
        
        # Insert data
        con.execute(f"INSERT INTO {table_name} SELECT * FROM df")

        # Metadata log for info
        row_count = con.execute("SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Loaded {row_count} rows into {table_name}")
        # TODO Replace with LOGGER and also find out why fetchone()?

        con.close()

    def create_indexes(self):
        '''Create indexes for speedy queries'''
        con = duckdb.connect(self.db_path)

        con.execute("CREATE INDEX IF NOT EXISTS idx_prices_symbol ON prices(symbol)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(Date)")

        con.close()

    def log_pipeline_run(self, table_name: str, row_count: int, status: str):
        '''Track ETL runs'''
        con = duckdb.connect(self.db_path)
        
        con.execute('''
            CREATE TABLE IF NOT EXISTS pipeline_logs (
                id INTEGER PRIMARY KEY,
                table_name VARCHAR,
                run_time TIMESTAMP,
                row_count INTEGER,
                status VARCHAR
            )
        ''')
        
        con.execute(f'''
            INSERT INTO pipeline_logs (table_name, run_time, row_count, status)
            VALUES ('{table_name}', '{datetime.now()}', {row_count}, '{status}')
        ''')
        
        con.close()