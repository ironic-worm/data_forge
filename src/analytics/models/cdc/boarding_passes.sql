with source as (

    select * from {{ source('bookings', 'boarding_passes') }}

),

renamed as (

    select
        ticket_no,
        flight_id,
        seat_no,
        boarding_no,
        boarding_time

    from source

)

select * from renamed
