"""
PySpark — Ingreso promedio por plan en 2025
============================================
Tarea 5 del examen práctico:
  Al ser 10 TB de datos, necesitan usar Spark para resolver la pregunta de negocio.
  Usando PySpark, crear el código para calcular el ingreso promedio de cada plan en 2025.

  a. Usar la tabla de hechos y dimensiones para calcular
  b. La tabla de hechos tiene datos desde 2015 - actualidad

Pregunta de negocio: ¿Qué plan es más rentable y por qué?
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =============================================================================
# Configuración de conexión JDBC (código dado en el examen)
# =============================================================================
db_url = "jdbc:postgresql://localhost:5432/megaline_dwh"
db_user = "megaline_admin"
db_password = "megaline_secure_pwd"
jdbc_driver = "org.postgresql.Driver"

# =============================================================================
# Inicializar SparkSession
# =============================================================================
spark = SparkSession.builder \
    .appName("Megaline - Ingreso Promedio por Plan 2025") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.1.jar") \
    .getOrCreate()

# =============================================================================
# Leer datos desde la base de datos mediante conexión ODBC/JDBC
# (Código de conexión proporcionado en el examen)
# =============================================================================

# Tabla de hechos
df_fct = spark.read.format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "gold.fct_monthly_usage") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", jdbc_driver) \
    .load()

# Dimensión de planes
df_plans = spark.read.format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "gold.dim_plans") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", jdbc_driver) \
    .load()

# =============================================================================
# Filtrar datos del año 2025
# La tabla de hechos tiene datos desde 2015 hasta la actualidad
# =============================================================================
df_2025 = df_fct.filter(F.year(F.col("month_year")) == 2025)

# =============================================================================
# Calcular ingreso promedio por plan en 2025
# Usamos la tabla de hechos y la dimensión de planes
# =============================================================================
revenue_by_plan = df_2025 \
    .join(df_plans, df_2025["plan_name"] == df_plans["plan_name"], "inner") \
    .groupBy(df_plans["plan_name"]) \
    .agg(
        F.avg("total_revenue").alias("ingreso_promedio_mensual_por_usuario"),
        F.sum("total_revenue").alias("ingreso_total"),
        F.count("*").alias("total_registros"),
        F.avg("base_revenue").alias("ingreso_base_promedio"),
        F.avg("excess_call_revenue").alias("excedente_llamadas_promedio"),
        F.avg("excess_message_revenue").alias("excedente_mensajes_promedio"),
        F.avg("excess_internet_revenue").alias("excedente_internet_promedio"),
    ) \
    .orderBy(F.col("ingreso_promedio_mensual_por_usuario").desc())

# =============================================================================
# Mostrar resultados
# =============================================================================
print("=" * 80)
print("MEGALINE — INGRESO PROMEDIO POR PLAN EN 2025")
print("Pregunta de negocio: ¿Qué plan es más rentable y por qué?")
print("=" * 80)

revenue_by_plan.show(truncate=False)

# Detalle por componente de ingreso
print("\nDesglose de ingresos promedio por componente:")
revenue_by_plan.select(
    "plan_name",
    F.round("ingreso_base_promedio", 2).alias("base_usd"),
    F.round("excedente_llamadas_promedio", 2).alias("exc_calls_usd"),
    F.round("excedente_mensajes_promedio", 2).alias("exc_msgs_usd"),
    F.round("excedente_internet_promedio", 2).alias("exc_inet_usd"),
    F.round("ingreso_promedio_mensual_por_usuario", 2).alias("total_prom_usd"),
).show(truncate=False)

# =============================================================================
# Finalizar sesión
# =============================================================================
spark.stop()
