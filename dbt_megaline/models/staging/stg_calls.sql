-- stg_calls: Modelo staging para eventos de llamadas
-- Limpia y estandariza los datos de megaline_calls

with source as (
    select * from {{ source('megaline', 'megaline_calls') }}
),

staged as (
    select
        id,
        user_id,
        cast(call_date as date)                         as call_date,
        cast(duration as decimal(10,2))                 as duration_minutes,
        date_trunc('month', cast(call_date as date))    as month_year
    from source
)

select * from staged
