-- =====================================================================
-- Tarea 4: Tabla de hechos con dbt
-- Escribe solo el FROM y JOINs con su clave de unión
-- =====================================================================
-- Asunciones:
--   a. Se tiene sources.yml con el source para planes y usuarios
--   b. Modelos en silver: stg_calls, stg_internet, stg_messages
-- =====================================================================

SELECT
    *

FROM {{ ref('stg_calls') }} AS c

FULL OUTER JOIN {{ ref('stg_messages') }} AS m
    ON c.user_id = m.user_id
    AND c.month_year = m.month_year

FULL OUTER JOIN {{ ref('stg_internet') }} AS i
    ON COALESCE(c.user_id, m.user_id) = i.user_id
    AND COALESCE(c.month_year, m.month_year) = i.month_year

LEFT JOIN {{ source('megaline', 'megaline_users') }} AS u
    ON COALESCE(c.user_id, m.user_id, i.user_id) = u.user_id

INNER JOIN {{ source('megaline', 'megaline_plans') }} AS p
    ON u.plan = p.plan_name
