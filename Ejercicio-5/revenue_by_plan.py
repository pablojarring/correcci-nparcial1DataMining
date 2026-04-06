from pyspark.sql import SparkSession
from pyspark.sql import functions as F

db_url = "jdbc:postgresql://localhost:5432/megaline_dwh"
db_user = "megaline_admin"
db_password = "megaline_secure_pwd"
jdbc_driver = "org.postgresql.Driver"

spark = SparkSession.builder \
    .appName("Megaline - Ingreso Promedio por Plan 2025") \
    .getOrCreate()

# Leer tabla de hechos
df_fct = spark.read.format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "gold.fct_monthly_usage") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", jdbc_driver) \
    .load()

# Leer dimensión de planes
df_plans = spark.read.format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "gold.dim_plans") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", jdbc_driver) \
    .load()

# Filtrar solo datos del año 2025
df_2025 = df_fct.filter(F.year(F.col("month_year")) == 2025)

# Calcular ingreso promedio por plan
revenue_by_plan = df_2025 \
    .join(df_plans, df_2025["plan_name"] == df_plans["plan_name"], "inner") \
    .groupBy(df_plans["plan_name"]) \
    .agg(
        F.avg("total_revenue").alias("ingreso_promedio"),
        F.sum("total_revenue").alias("ingreso_total"),
        F.count("*").alias("total_registros"),
    ) \
    .orderBy(F.col("ingreso_promedio").desc())

revenue_by_plan.show(truncate=False)

spark.stop()
