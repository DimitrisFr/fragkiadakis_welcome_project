with exchange_rates as (

    select *
    from {{ ref('stg_landing__exchange_rates') }}

),

parsed as (

    select
        exchange_rate_date,
        base_currency_code,
        from_json(rates_json, 'map<string,double>') as rates_map
    from exchange_rates

),

exploded as (

    select
        exchange_rate_date,
        upper(trim(fx.key)) as currency_code,
        upper(trim(base_currency_code)) as base_currency_code,
        cast(fx.value as decimal(18,6)) as exchange_rate_to_eur
    from parsed
    lateral view explode(rates_map) fx as key, value

)

select *
from exploded