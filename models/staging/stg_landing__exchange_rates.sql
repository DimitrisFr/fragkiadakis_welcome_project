with source as (

    select *
    from {{ source('landing', 'exchange_rates') }}

),

renamed as (

    select
        cast(date as date) as exchange_rate_date,
        cast(rates as string) as rates_json,
        upper(trim(cast(base as string))) as base_currency_code

    from source

)

select *
from renamed