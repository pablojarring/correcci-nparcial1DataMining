-- stg_messages: Modelo staging para eventos de SMS
-- Limpia y estandariza los datos de megaline_messages

with source as (
    select * from {{ source('megaline', 'megaline_messages') }}
),

staged as (
    select
        id,
        user_id,
        cast(message_date as date)                          as message_date,
        date_trunc('month', cast(message_date as date))     as month_year
    from source
)

select * from staged
