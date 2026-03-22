with source as (

    select * 
    from {{ source('landing', 'drivers') }}

),

renamed as (

    select
        cast(id as int) as driver_id,
        cast(driver_company_id as int) as driver_company_id

    from source

),

final as (

    select *
    from renamed

)

select *
from final