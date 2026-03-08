with source as (

    select * from {{ source('bookings', 'tickets') }}

),

renamed as (

    select
        ticket_no,
        book_ref,
        passenger_id,
        passenger_name,
        outbound

    from source

)

select * from renamed