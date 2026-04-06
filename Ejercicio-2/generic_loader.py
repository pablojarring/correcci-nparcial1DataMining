import pandas as pd
from sqlalchemy import create_engine
from tenacity import retry, stop_after_attempt, wait_exponential

CHUNK_SIZE = 10_000


# --- Retries con backoff exponencial ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))
def load_chunk(df_chunk, table_name, engine):
    df_chunk.to_sql(table_name, engine, schema="raw", if_exists="append", index=False)


def load_parquet_to_raw(parquet_url, table_name, db_url):
    df = pd.read_parquet(parquet_url)

    engine = create_engine(db_url)

    # --- Chunking ---
    for i in range(0, len(df), CHUNK_SIZE):
        chunk = df.iloc[i : i + CHUNK_SIZE]
        load_chunk(chunk, table_name, engine)
        print(f"Chunk {i // CHUNK_SIZE + 1} — {len(chunk)} filas insertadas")
