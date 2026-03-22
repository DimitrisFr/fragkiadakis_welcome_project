with source as (

    select *
    from {{ source('landing', 'transfers') }}

),

renamed as (

    select
        cast(id as int) as transfer_id,
        cast(welcome_city_id as int) as welcome_city_id,
        cast(driver_id as int) as driver_id,
        cast(coupon_id as int) as coupon_id,
        lower(trim(cast(status as string))) as transfer_status,
        cast(traveler_price as double) as traveler_price,
        upper(trim(cast(currency_code as string))) as currency_code,
        cast(operated_at as timestamp) as operated_at,
        cast(created_at as timestamp) as created_at,
        cast(operated_at as date) as operated_date,
        cast(created_at as date) as created_date

    from source

)

select *
from renamed