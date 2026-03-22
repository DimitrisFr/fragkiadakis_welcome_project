with source as (

    select *
    from {{ source('landing', 'welcome_cities') }}

),

renamed as (

    select
        cast(id as int) as welcome_city_id,
        cast(hailing_kind as string) as hailing_type -- I standardized the naming to match the business language

    from source

)

select *
from renamed