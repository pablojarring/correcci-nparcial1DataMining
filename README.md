# Corrección Examen Parcial 1 — Data Mining

**Universidad San Francisco de Quito**
Ingeniería en Ciencias de la Computación
Materia: Data Mining
Profesor: MSc. Erick Ñauñay Cantos

---

## Contexto

**Megaline** es una empresa de telecomunicaciones que ofrece planes mensuales con una combinación de minutos, mensajes y datos móviles.

- **25 millones** de clientes en todo USA
- **~10 TB** de datos
- **Pregunta de negocio:** ¿Qué plan es más rentable y por qué?

## Estructura del Proyecto

```
├── README.md                          # Este archivo
├── respuestas_teorico.md              # Respuestas del examen teórico (25 preguntas)
├── docker-compose.yml                 # Tarea 1: Infraestructura
├── data_loader/
│   └── generic_loader.py              # Tarea 2: Data loader genérico (retries + chunking)
├── erd/
│   └── erd_gold.md                    # Tarea 3: Diagrama ERD esquema estrella (Gold)
├── dbt_megaline/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── sources.yml
│   │   ├── staging/
│   │   │   ├── stg_calls.sql
│   │   │   ├── stg_messages.sql
│   │   │   └── stg_internet.sql
│   │   └── gold/
│   │       └── fct_monthly_usage.sql  # Tarea 4: Tabla de hechos (FROM + JOINs)
├── pyspark/
│   └── revenue_by_plan.py             # Tarea 5: Ingreso promedio por plan (2025)
└── sql/
    └── create_fact_table.sql          # Extra: DDL + particionamiento Ene-Feb 2025
```

## Tablas de la Base de Datos

| Tabla | Descripción |
|-------|-------------|
| `megaline_users` | Catálogo de usuarios (cliente, plan, fechas) |
| `megaline_plans` | Catálogo de planes (cuota, límites, tarifas) |
| `megaline_calls` | Eventos de llamadas |
| `megaline_messages` | Eventos de SMS |
| `megaline_internet` | Sesiones de internet (MB consumidos) |

## Cómo Ejecutar

### 1. Levantar infraestructura
```bash
docker-compose up -d
```

### 2. Ejecutar data loader
```bash
pip install psycopg2-binary tenacity
python data_loader/generic_loader.py
```

### 3. Ejecutar dbt
```bash
cd dbt_megaline
dbt run
```

### 4. Ejecutar PySpark
```bash
spark-submit pyspark/revenue_by_plan.py
```
