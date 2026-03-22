with transfers as (

    select 
    
    *
    from {{ ref('stg_landing__transfers') }}

),

drivers as (

    select *
    from {{ ref('stg_landing__drivers') }}

),

cities as (

    select *
    from {{ ref('stg_landing__welcome_cities') }}

),

coupons as (

    select *
    from {{ ref('stg_landing__coupons') }}

),

event_flags as (

    select *
    from {{ ref('int_transfer_event_flags') }}

),

fx as (

    select *
    from {{ ref('int_exchange_rates_daily') }}

),

joined as (

    select
        t.transfer_id,
        t.created_at,
        t.created_date,
        t.operated_at,
        t.operated_date,
        t.welcome_city_id,
        c.hailing_type,

        t.driver_id,
        d.driver_company_id,

        case
            when d.driver_company_id is null then 'individual'
            else 'company'
        end as driver_type,

        t.coupon_id,
        t.transfer_status,
        t.traveler_price,
        t.currency_code,

        fx_transfer.exchange_rate_to_eur as transfer_fx_rate,

        cp.cost_kind,
        cp.cost_amount,
        cp.currency_code as coupon_currency_code,

        fx_coupon.exchange_rate_to_eur as coupon_fx_rate,

        coalesce(ef.is_perfect_app_usage, 0) as is_perfect_app_usage

    from transfers t

    left join drivers d
        on t.driver_id = d.driver_id

    left join cities c
        on t.welcome_city_id = c.welcome_city_id

    left join coupons cp
        on t.coupon_id = cp.coupon_id

    left join event_flags ef
        on t.transfer_id = ef.transfer_id

    -- FX for transfer currency
    left join fx fx_transfer
        on t.operated_date = fx_transfer.exchange_rate_date
       and t.currency_code = fx_transfer.currency_code

    -- FX for coupon currency
    left join fx fx_coupon
        on t.operated_date = fx_coupon.exchange_rate_date
       and cp.currency_code = fx_coupon.currency_code

),

derived as (

    select
        *,
        cast(traveler_price as decimal(18,6)) as traveler_price_dec,
        cast(cost_amount as decimal(18,6)) as cost_amount_dec,
        -- STATUS FLAGS
        case when transfer_status = 'finished' then 1 else 0 end as is_operated,
        case when transfer_status = 'canceled' then 1 else 0 end as is_cancelled,
        1 as is_booked,

        -- FX CONVERSION
        coalesce(
            traveler_price_dec / nullif(transfer_fx_rate, 0)
            ,0 ) as traveler_price_eur,

        case
            when coupon_id is null then 0

            when cost_kind = 'fixed'
                then 
                    coalesce(
                        cost_amount / nullif(coupon_fx_rate, 0)
                        ,0)

            when cost_kind = 'percentage'
                then 
                    coalesce(                       
                        (traveler_price_dec * cost_amount_dec / 100.0)
                            / nullif(transfer_fx_rate, 0)
                        ,0)
            else 0
        end as coupon_discount_eur

    from joined

),

final as (

    select
        transfer_id,

        created_at,
        created_date,
        operated_at,
        operated_date,

        welcome_city_id,
        hailing_type,

        driver_id,
        driver_company_id,
        driver_type,

        coupon_id,
        transfer_status,

        traveler_price,
        currency_code,

        cast(traveler_price_eur as decimal (18,6)) as traveler_price_eur,
        cast(coupon_discount_eur as decimal (18,6)) as coupon_discount_eur,

        is_booked,
        is_operated,
        is_cancelled,
        is_perfect_app_usage

    from derived

)

select 
{{ dbt_utils.generate_surrogate_key(['transfer_id']) }} as transfer_sk,

*
from final