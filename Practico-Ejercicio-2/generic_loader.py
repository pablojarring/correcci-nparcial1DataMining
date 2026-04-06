"""
Data Loader Genérico con Reintentos y Chunking
===============================================
Carga datos desde tablas de una BD de forma genérica.
- Reintentos con backoff exponencial
- Chunking (procesamiento por bloques)

Asume que las variables de conexión ya están definidas.
"""

import logging
import time
from typing import Generator, List, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("GenericDataLoader")

# Variables de conexión (asumidas ya definidas)
db_url = "localhost"
db_port = 5432
db_name = "megaline_dwh"
db_user = "megaline_admin"
db_password = "megaline_secure_pwd"
db_target_schema = "raw"


class GenericDataLoader:
    """Data loader genérico con reintentos y chunking."""

    def __init__(self, table_name: str, chunk_size: int = 10_000, max_retries: int = 3, source_schema: str = "public"):
        self.table_name = table_name
        self.chunk_size = chunk_size
        self.max_retries = max_retries
        self.source_schema = source_schema

    def _get_connection(self):
        return psycopg2.connect(
            host=db_url, port=db_port,
            dbname=db_name, user=db_user, password=db_password,
        )

    def _get_total_rows(self, conn) -> int:
        query = f"SELECT COUNT(*) FROM {self.source_schema}.{self.table_name}"
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone()[0]

    def extract_chunks(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Genera chunks de datos desde la tabla usando LIMIT/OFFSET."""
        conn = self._get_connection()
        try:
            total_rows = self._get_total_rows(conn)
            total_chunks = (total_rows + self.chunk_size - 1) // self.chunk_size
            logger.info("Tabla '%s': %d filas, %d chunks", self.table_name, total_rows, total_chunks)

            offset = 0
            chunk_num = 0
            while offset < total_rows:
                chunk_num += 1
                query = (
                    f"SELECT * FROM {self.source_schema}.{self.table_name} "
                    f"ORDER BY 1 LIMIT {self.chunk_size} OFFSET {offset}"
                )
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query)
                    rows = cur.fetchall()
                logger.info("  Chunk %d/%d — %d filas (offset=%d)", chunk_num, total_chunks, len(rows), offset)
                yield [dict(row) for row in rows]
                offset += self.chunk_size
        finally:
            conn.close()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((psycopg2.OperationalError, psycopg2.InterfaceError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _insert_chunk(self, chunk: List[Dict[str, Any]]) -> int:
        """Inserta un chunk con reintentos y backoff exponencial."""
        if not chunk:
            return 0
        columns = chunk[0].keys()
        col_names = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])
        insert_query = (
            f"INSERT INTO {db_target_schema}.{self.table_name} ({col_names}) "
            f"VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        )
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                cur.executemany(insert_query, chunk)
            conn.commit()
            return len(chunk)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def load(self) -> Dict[str, Any]:
        """Ejecuta extracción por chunks + carga con reintentos."""
        logger.info("INICIO — Cargando tabla: %s", self.table_name)
        start_time = time.time()
        total_loaded = 0
        chunks_processed = 0

        for chunk in self.extract_chunks():
            rows_inserted = self._insert_chunk(chunk)
            total_loaded += rows_inserted
            chunks_processed += 1

        elapsed = time.time() - start_time
        logger.info("FIN — %s: %d filas en %.2fs", self.table_name, total_loaded, elapsed)
        return {"table": self.table_name, "rows": total_loaded, "chunks": chunks_processed, "seconds": round(elapsed, 2)}


# === Ejecutar carga de todas las tablas ===
if __name__ == "__main__":
    tables = ["megaline_users", "megaline_plans", "megaline_calls", "megaline_messages", "megaline_internet"]
    for table in tables:
        GenericDataLoader(table_name=table, chunk_size=10_000).load()
