# type: ignore
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

default_args = {
    "owner": "avishka",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="retail_etl_parquet_chunked_dag_postgres",
    default_args=default_args,
    description="ETL pipeline using Parquet chunks and exporting to Postgres",
    start_date=datetime(2025, 9, 12),
    schedule=None,
    catchup=False,
) as dag:

    
    # Extract Task
    
    @task
    def extract():
        """Simulate data extraction: return CSV file path"""
        return "/usr/local/airflow/include/data.csv"

    
    # Transform Task
  
    @task
    def transform(data_path: str):
        """Clean, filter, and store as Parquet with proper types"""
        df = pd.read_csv(data_path)

        # Convert InvoiceDate to datetime
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")

        # Convert numeric columns to appropriate types
        df["CustomerID"] = pd.to_numeric(df["CustomerID"], errors="coerce")  # float
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").astype("Int64")  # nullable int
        df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")  # float

        # Remove any rows with nulls in essential columns
        df.dropna(subset=["InvoiceDate", "CustomerID", "Quantity", "UnitPrice"], inplace=True)

        # Remove duplicates
        df.drop_duplicates(inplace=True)

        # Keep only valid quantities and unit prices
        df = df[df["Quantity"] > 0]
        df = df[df["UnitPrice"] >= 0]

        # Save transformed data to Parquet
        output_path = "/tmp/transformed_data.parquet"
        df.to_parquet(output_path, index=False)

        return output_path

    
    # Load Task
    
    @task
    def load(data_path: str):
        """Load Parquet file into Postgres in batches with correct types"""
        # Connect to Postgres
        hook = PostgresHook(postgres_conn_id="gx_retail_postgres")
        engine = hook.get_sqlalchemy_engine()

        # Open Parquet file
        parquet_file = pq.ParquetFile(data_path)

        # Define SQLAlchemy dtype mapping
        from sqlalchemy.types import Float, Integer, DateTime
        dtype_map = {
            "CustomerID": Float,
            "InvoiceDate": DateTime,
            "Quantity": Integer,
            "UnitPrice": Float,
        }

        # Load data in batches
        first_batch = True
        for batch in parquet_file.iter_batches(batch_size=10000):
            df_batch = pa.Table.from_batches([batch]).to_pandas()
            df_batch.to_sql(
                "validated_sales_data",
                engine,
                if_exists="replace" if first_batch else "append",
                index=False,
                dtype=dtype_map
            )
            first_batch = False

    
    # DAG Dependencies
   
    extracted_path = extract()
    transformed_data_path = transform(extracted_path)
    load(transformed_data_path)
