-- =============================================================================
-- fct_monthly_usage: Tabla de hechos — Uso mensual unificado por usuario
-- =============================================================================
-- La tabla de hechos unifica los tres servicios (calls, messages, internet)
-- a nivel de usuario por mes.
--
-- Tarea 4 del examen: Escribir solo el FROM y JOINs con su clave de unión
-- Asunciones:
--   a. Se tiene un archivo sources.yml con el source para planes y usuarios
--   b. Ya existen modelos en silver: stg_calls, stg_messages, stg_internet
-- =============================================================================

with calls_monthly as (
    select
        user_id,
        month_year,
        count(*)                        as total_calls,
        sum(duration_minutes)           as total_call_minutes
    from {{ ref('stg_calls') }}
    group by user_id, month_year
),

messages_monthly as (
    select
        user_id,
        month_year,
        count(*)                        as total_messages
    from {{ ref('stg_messages') }}
    group by user_id, month_year
),

internet_monthly as (
    select
        user_id,
        month_year,
        sum(mb_used)                    as total_mb_used
    from {{ ref('stg_internet') }}
    group by user_id, month_year
),

-- =========================================================================
-- FROM + JOINs con claves de unión (lo que pide el examen)
-- =========================================================================
-- Se usa FULL OUTER JOIN entre los 3 staging para capturar usuarios
-- que usen cualquier combinación de servicios (puede haber usuarios
-- que solo llamen, solo envíen SMS, o solo usen internet en un mes).
-- Se une con LEFT JOIN a users (para obtener el plan del usuario)
-- y con INNER JOIN a plans (para obtener las tarifas y límites).
-- =========================================================================

joined as (
    select
        coalesce(c.user_id, m.user_id, i.user_id)          as user_id,
        coalesce(c.month_year, m.month_year, i.month_year)  as month_year,
        coalesce(c.total_calls, 0)                          as total_calls,
        coalesce(c.total_call_minutes, 0)                   as total_call_minutes,
        coalesce(m.total_messages, 0)                       as total_messages,
        coalesce(i.total_mb_used, 0)                        as total_mb_used,
        u.plan,
        p.minutes_included,
        p.messages_included,
        p.mb_per_month_included,
        p.usd_monthly_pay,
        p.usd_per_minute,
        p.usd_per_message,
        p.usd_per_gb

    from calls_monthly as c

    full outer join messages_monthly as m
        on c.user_id = m.user_id
        and c.month_year = m.month_year

    full outer join internet_monthly as i
        on coalesce(c.user_id, m.user_id) = i.user_id
        and coalesce(c.month_year, m.month_year) = i.month_year

    left join {{ source('megaline', 'megaline_users') }} as u
        on coalesce(c.user_id, m.user_id, i.user_id) = u.user_id

    inner join {{ source('megaline', 'megaline_plans') }} as p
        on u.plan = p.plan_name
),

-- =========================================================================
-- Cálculo de excedentes e ingresos
-- =========================================================================

calculated as (
    select
        user_id,
        month_year,
        plan                                                            as plan_name,
        total_calls,
        total_call_minutes,
        total_messages,
        total_mb_used,

        -- Excedentes
        greatest(total_call_minutes - minutes_included, 0)              as excess_minutes,
        greatest(total_messages - messages_included, 0)                 as excess_messages,
        greatest(total_mb_used - mb_per_month_included, 0)              as excess_mb,

        -- Ingresos
        usd_monthly_pay                                                 as base_revenue,

        greatest(total_call_minutes - minutes_included, 0)
            * usd_per_minute                                            as excess_call_revenue,

        greatest(total_messages - messages_included, 0)
            * usd_per_message                                           as excess_message_revenue,

        greatest(total_mb_used - mb_per_month_included, 0)
            / 1024.0 * usd_per_gb                                      as excess_internet_revenue

    from joined
)

select
    row_number() over (order by user_id, month_year)                    as fact_id,
    user_id,
    plan_name,
    month_year,
    total_calls,
    total_call_minutes,
    total_messages,
    total_mb_used,
    excess_minutes,
    excess_messages,
    excess_mb,
    base_revenue,
    excess_call_revenue,
    excess_message_revenue,
    excess_internet_revenue,
    base_revenue
        + excess_call_revenue
        + excess_message_revenue
        + excess_internet_revenue                                       as total_revenue

from calculated
