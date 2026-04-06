"""
Data Loader Genérico con Reintentos y Chunking
===============================================
Carga datos desde tablas de una base de datos PostgreSQL
de forma genérica, con soporte para:
  - Reintentos con backoff exponencial
  - Chunking (procesamiento por bloques)
  - Logging de progreso

Asume que las siguientes variables ya están definidas:
  - db_url, db_user, db_password, db_name
"""

import logging
import time
from typing import Generator, List, Dict, Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

# ---- Configuración de logging ----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("GenericDataLoader")

# ---- Variables de conexión (asumidas ya definidas en el entorno) ----
db_url = "localhost"
db_port = 5432
db_name = "megaline_dwh"
db_user = "megaline_admin"
db_password = "megaline_secure_pwd"
db_target_schema = "raw"


class GenericDataLoader:
    """
    Data loader genérico que extrae datos de cualquier tabla
    con soporte de reintentos y chunking.
    """

    def __init__(
        self,
        table_name: str,
        chunk_size: int = 10_000,
        max_retries: int = 3,
        source_schema: str = "public",
    ):
        """
        Args:
            table_name:    Nombre de la tabla a cargar.
            chunk_size:    Número de filas por chunk.
            max_retries:   Número máximo de reintentos por operación.
            source_schema: Schema de origen de la tabla.
        """
        self.table_name = table_name
        self.chunk_size = chunk_size
        self.max_retries = max_retries
        self.source_schema = source_schema

    # ---- Conexión ----

    def _get_connection(self):
        """Crea y retorna una conexión a la base de datos."""
        return psycopg2.connect(
            host=db_url,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password,
        )

    # ---- Extracción con chunking ----

    def _get_total_rows(self, conn) -> int:
        """Obtiene el conteo total de filas de la tabla."""
        query = f"SELECT COUNT(*) FROM {self.source_schema}.{self.table_name}"
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone()[0]

    def extract_chunks(self) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Genera chunks de datos desde la tabla usando LIMIT/OFFSET.
        Cada chunk es una lista de diccionarios.
        """
        conn = self._get_connection()
        try:
            total_rows = self._get_total_rows(conn)
            total_chunks = (total_rows + self.chunk_size - 1) // self.chunk_size

            logger.info(
                "Tabla '%s': %d filas totales, %d chunks de %d filas",
                self.table_name,
                total_rows,
                total_chunks,
                self.chunk_size,
            )

            offset = 0
            chunk_num = 0

            while offset < total_rows:
                chunk_num += 1
                query = (
                    f"SELECT * FROM {self.source_schema}.{self.table_name} "
                    f"ORDER BY 1 "
                    f"LIMIT {self.chunk_size} OFFSET {offset}"
                )

                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query)
                    rows = cur.fetchall()

                logger.info(
                    "  Chunk %d/%d — %d filas extraídas (offset=%d)",
                    chunk_num,
                    total_chunks,
                    len(rows),
                    offset,
                )

                yield [dict(row) for row in rows]
                offset += self.chunk_size

        finally:
            conn.close()

    # ---- Carga con reintentos ----

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((psycopg2.OperationalError, psycopg2.InterfaceError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _insert_chunk(self, chunk: List[Dict[str, Any]]) -> int:
        """
        Inserta un chunk de datos en la tabla destino.
        Usa reintentos con backoff exponencial ante errores de conexión.

        Returns:
            Número de filas insertadas.
        """
        if not chunk:
            return 0

        columns = chunk[0].keys()
        col_names = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])

        insert_query = (
            f"INSERT INTO {db_target_schema}.{self.table_name} ({col_names}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT DO NOTHING"
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

    # ---- Orquestación principal ----

    def load(self) -> Dict[str, Any]:
        """
        Ejecuta el pipeline completo de extracción y carga:
        1. Extrae datos en chunks desde la tabla origen
        2. Inserta cada chunk en la tabla destino con reintentos

        Returns:
            Diccionario con métricas de la carga.
        """
        logger.info("=" * 60)
        logger.info("INICIO — Cargando tabla: %s", self.table_name)
        logger.info("=" * 60)

        start_time = time.time()
        total_loaded = 0
        chunks_processed = 0
        errors = 0

        for chunk in self.extract_chunks():
            try:
                rows_inserted = self._insert_chunk(chunk)
                total_loaded += rows_inserted
                chunks_processed += 1
            except Exception as e:
                errors += 1
                logger.error(
                    "Error al insertar chunk %d de tabla '%s': %s",
                    chunks_processed + 1,
                    self.table_name,
                    str(e),
                )
                raise

        elapsed = time.time() - start_time

        metrics = {
            "table": self.table_name,
            "total_rows_loaded": total_loaded,
            "chunks_processed": chunks_processed,
            "errors": errors,
            "elapsed_seconds": round(elapsed, 2),
        }

        logger.info("FIN — Tabla '%s': %d filas en %.2fs", self.table_name, total_loaded, elapsed)
        logger.info("Métricas: %s", metrics)

        return metrics


# =============================================================================
# Ejecutar carga de todas las tablas de Megaline
# =============================================================================
if __name__ == "__main__":
    tables = [
        "megaline_users",
        "megaline_plans",
        "megaline_calls",
        "megaline_messages",
        "megaline_internet",
    ]

    all_metrics = []

    for table in tables:
        loader = GenericDataLoader(
            table_name=table,
            chunk_size=10_000,
            max_retries=3,
        )
        result = loader.load()
        all_metrics.append(result)

    logger.info("=" * 60)
    logger.info("RESUMEN DE CARGA")
    logger.info("=" * 60)
    for m in all_metrics:
        logger.info(
            "  %s — %d filas, %d chunks, %.2fs",
            m["table"],
            m["total_rows_loaded"],
            m["chunks_processed"],
            m["elapsed_seconds"],
        )
