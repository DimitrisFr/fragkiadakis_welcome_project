with transfers as (

    select *
    from {{ ref('int_transfers_enriched') }}

),

aggregated as (

    select
        operated_date as metric_date,
        welcome_city_id,
        hailing_type,
        driver_type,

        sum(is_booked) as transfers_booked,
        sum(is_operated) as transfers_operated,

        case
            when sum(is_booked) = 0 then 0
            else
                cast(sum(is_cancelled) as decimal(18,6))
                / sum(is_booked)
        end as cancellation_rate,

        sum(case when is_operated = 1 then traveler_price_eur else 0 end) as gmv,
        sum(case when is_operated = 1 then coupon_discount_eur else 0 end) as coupon_discount,

        case
            when sum(is_operated) = 0 then 0
            else
                cast(
                    sum(
                        case
                            when is_operated = 1 and is_perfect_app_usage = 1 then 1
                            else 0
                        end
                    ) as decimal(18,6)
                )
                /
                cast(sum(is_operated) as decimal(18,6))
        end as driver_perfect_app_usage_rate

    from transfers
    group by 1, 2, 3, 4

),

final as (

    select


        metric_date,
        welcome_city_id,
        hailing_type,
        driver_type,
        transfers_booked,
        transfers_operated,
        cast(cancellation_rate as decimal(18,6)) as cancellation_rate,
        cast(gmv as decimal(18,6) ) as gmv,
        cast(coupon_discount as decimal(18,6) ) as coupon_discount,
        cast(driver_perfect_app_usage_rate as decimal(18,6)) as driver_perfect_app_usage_rate

    from aggregated

)

select *
from final