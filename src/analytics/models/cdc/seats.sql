with source as (

    select * from {{ source('bookings', 'seats') }}

),

renamed as (

    select
        airplane_code,
        seat_no,
        fare_conditions

    from source

)

select * from renamed