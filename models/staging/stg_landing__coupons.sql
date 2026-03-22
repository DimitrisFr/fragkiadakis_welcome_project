with source as (

    select *
    from {{ source('landing', 'coupons') }}

),

renamed as (

    select
        cast(id as int) as coupon_id,
        cast(cost_kind as string) as cost_kind,
        cast(cost_amount as double) as cost_amount,
        cast(currency_code as string) as currency_code,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at

    from source

),

cleaned as (

    select
        coupon_id,
        lower(trim(cost_kind)) as cost_kind,
        cost_amount,
        upper(trim(currency_code)) as currency_code, -- This avoids subtle bugs later when joining FX rates
        created_at,
        updated_at

    from renamed

)

select *
from cleaned
