with source as (

    select * from {{ source('bookings', 'flights') }}

),

renamed as (

    select
        flight_id,
        route_no,
        status,
        scheduled_departure,
        scheduled_arrival,
        actual_departure,
        actual_arrival

    from source

)

select * from renamed
