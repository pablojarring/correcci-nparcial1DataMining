# Diagrama ERD — Capa Gold (Esquema Estrella)

## Modelo Dimensional — Megaline Telecom

Este diagrama representa el modelado dimensional en esquema estrella para la capa Gold del Data Warehouse de Megaline. La **tabla de hechos unifica los tres servicios** (llamadas, mensajes e internet) a nivel de usuario por mes.

```mermaid
erDiagram

    %% ===== DIMENSIONES =====

    dim_users {
        int user_id PK
        varchar first_name
        varchar last_name
        int age
        varchar city
        date reg_date
        varchar plan
        date churn_date
    }

    dim_plans {
        varchar plan_name PK
        int messages_included
        int mb_per_month_included
        int minutes_included
        decimal usd_monthly_pay
        decimal usd_per_gb
        decimal usd_per_message
        decimal usd_per_minute
    }

    dim_date {
        int date_key PK
        date full_date
        int year
        int month
        int day
        int quarter
        varchar month_name
        varchar day_of_week
    }

    %% ===== TABLA DE HECHOS (unifica llamadas, mensajes e internet) =====

    fct_monthly_usage {
        bigint fact_id PK
        int user_id FK
        varchar plan_name FK
        int date_key FK
        date month_year
        int total_calls
        decimal total_call_minutes
        int total_messages
        decimal total_mb_used
        decimal excess_minutes
        int excess_messages
        decimal excess_mb
        decimal base_revenue
        decimal excess_call_revenue
        decimal excess_message_revenue
        decimal excess_internet_revenue
        decimal total_revenue
    }

    %% ===== RELACIONES =====

    dim_users ||--o{ fct_monthly_usage : "user_id"
    dim_plans ||--o{ fct_monthly_usage : "plan_name"
    dim_date  ||--o{ fct_monthly_usage : "date_key"
```

## Descripción de las Tablas

### Tabla de Hechos: `fct_monthly_usage`

Tabla de hechos central que **unifica los tres servicios** (llamadas, mensajes e internet) a nivel de **usuario por mes**. Contiene métricas de uso y cálculos de ingresos.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `fact_id` | BIGINT | Clave surrogate de la tabla de hechos |
| `user_id` | INT | FK → dim_users |
| `plan_name` | VARCHAR | FK → dim_plans |
| `date_key` | INT | FK → dim_date (primer día del mes) |
| `month_year` | DATE | Mes/año del período |
| `total_calls` | INT | Total de llamadas realizadas en el mes |
| `total_call_minutes` | DECIMAL | Total de minutos de llamadas |
| `total_messages` | INT | Total de mensajes SMS enviados |
| `total_mb_used` | DECIMAL | Total de MB consumidos de internet |
| `excess_minutes` | DECIMAL | Minutos que exceden el plan |
| `excess_messages` | INT | Mensajes que exceden el plan |
| `excess_mb` | DECIMAL | MB que exceden el plan |
| `base_revenue` | DECIMAL | Ingreso base (cuota mensual del plan) |
| `excess_call_revenue` | DECIMAL | Ingreso por minutos excedentes |
| `excess_message_revenue` | DECIMAL | Ingreso por mensajes excedentes |
| `excess_internet_revenue` | DECIMAL | Ingreso por GB excedentes |
| `total_revenue` | DECIMAL | Ingreso total = base + excedentes |

### Dimensión: `dim_users`

Catálogo de usuarios/clientes de Megaline.

### Dimensión: `dim_plans`

Catálogo de planes con cuotas mensuales, límites incluidos y tarifas por excedente.

### Dimensión: `dim_date`

Dimensión de fecha para facilitar análisis temporales (filtrar por año, mes, trimestre, etc.).

## Granularidad

- **Grano:** Un registro por **usuario por mes**
- **Unificación:** Los tres servicios (calls, messages, internet) se agregan en una sola fila por usuario/mes
- **Cálculo de excedentes:** Se calculan comparando uso real vs. límites del plan
