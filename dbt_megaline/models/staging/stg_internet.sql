-- stg_internet: Modelo staging para sesiones de internet
-- Limpia y estandariza los datos de megaline_internet

with source as (
    select * from {{ source('megaline', 'megaline_internet') }}
),

staged as (
    select
        id,
        user_id,
        cast(session_date as date)                          as session_date,
        cast(mb_used as decimal(12,2))                      as mb_used,
        date_trunc('month', cast(session_date as date))     as month_year
    from source
)

select * from staged
