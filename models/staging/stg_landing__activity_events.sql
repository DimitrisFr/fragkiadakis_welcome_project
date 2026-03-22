with source as (

    select *
    from {{ source('landing', 'activity_events') }}

),

renamed as (

    select
        cast(id as int) as activity_event_id,
        cast(transfer_id as int) as transfer_id,
        cast(event_name as string) as event_name,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at

    from source

)

select *
from renamed