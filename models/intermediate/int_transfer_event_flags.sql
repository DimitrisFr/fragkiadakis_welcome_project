with activity_events as (

    select *
    from {{ ref('stg_landing__activity_events') }}

),

aggregated as (

    select
        transfer_id,

        max(case when event_name = 'driver_confirmed_operation' then 1 else 0 end) as has_driver_confirmed_operation,
        max(case when event_name = 'driver_at_pickup_location' then 1 else 0 end) as has_driver_at_pickup_location,
        max(case when event_name = 'driver_met_traveler' then 1 else 0 end) as has_driver_met_traveler,
        max(case when event_name = 'transfer_finished' then 1 else 0 end) as has_transfer_finished,

        -- optional helper flags
        max(case when event_name = 'driver_accepted' then 1 else 0 end) as has_driver_accepted,
        max(case when event_name = 'driver_begin_ride' then 1 else 0 end) as has_driver_begin_ride,
        max(case when event_name = 'driver_review_request' then 1 else 0 end) as has_driver_review_request,
        max(case when event_name = 'traveler_no_show_up' then 1 else 0 end) as has_traveler_no_show_up,
        max(case when event_name = 'transfer_removed_from_schedule' then 1 else 0 end) as has_transfer_removed_from_schedule,

        min(created_at) as first_event_at,
        max(created_at) as last_event_at,
        count(*) as total_activity_events

    from activity_events
    group by 1

),

final as (

    select
        transfer_id,
        has_driver_confirmed_operation,
        has_driver_at_pickup_location,
        has_driver_met_traveler,
        has_transfer_finished,
        has_driver_accepted,
        has_driver_begin_ride,
        has_driver_review_request,
        has_traveler_no_show_up,
        has_transfer_removed_from_schedule,
        first_event_at,
        last_event_at,
        total_activity_events,
        case
            when has_driver_confirmed_operation = 1
             and has_driver_at_pickup_location = 1
             and has_driver_met_traveler = 1
             and has_transfer_finished = 1
            then 1
            else 0
        end as is_perfect_app_usage
    from aggregated

)

select 
{{ dbt_utils.generate_surrogate_key(['transfer_id']) }} as transfer_sk,

*
from final