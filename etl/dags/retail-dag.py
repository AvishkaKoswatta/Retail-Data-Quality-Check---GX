# type: ignore
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os

default_args = {
    "owner": "avishka",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retail_etl_parquet_chunked_dag",
    default_args=default_args,
    description="ETL pipeline using Parquet chunks and exporting to CSV",
    start_date=datetime(2025, 9, 12),
    schedule=None,
    catchup=False,
) as dag:

    # ------------------------
    # Extract Task
    # ------------------------
    @task
    def extract():
        """Simulate data extraction"""
        return "/usr/local/airflow/include/data.csv"

    # ------------------------
    # Transform Task
    # ------------------------
    @task
    def transform(data_path):
        """Clean, filter, and store as Parquet"""
        df = pd.read_csv(data_path)
        
        # Explicit conversion
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
        # Drop rows where conversion failed
        df = df.dropna(subset=["InvoiceDate"])

        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)
        df = df[df["Quantity"] > 0]   # Keep only positive quantities (no returns)
        df = df[df["UnitPrice"] >= 0]  

        output_path = "/tmp/transformed_data.parquet"
        df.to_parquet(output_path, index=False)
        return output_path

    # ------------------------
    # Load Task
    # ------------------------
    @task
    def load(data_path):
        """Read Parquet in chunks and write to a single CSV file"""
        parquet_file = pq.ParquetFile(data_path)
        output_csv = "/usr/local/airflow/include/outputs/final_output.csv"

        # Remove if existing (avoid appending duplicates)
        if os.path.exists(output_csv):
            os.remove(output_csv)

        first_chunk = True
        for batch in parquet_file.iter_batches(batch_size=10000):
            df_batch = pa.Table.from_batches([batch]).to_pandas()
            df_batch.to_csv(
                output_csv,
                mode="w" if first_chunk else "a",  
                header=first_chunk,                 
                index=False
            )
            first_chunk = False

        return output_csv

    # ------------------------
    # DAG Dependencies
    # ------------------------
    extracted_path = extract()
    transformed_data_path = transform(extracted_path)
    load(transformed_data_path)
