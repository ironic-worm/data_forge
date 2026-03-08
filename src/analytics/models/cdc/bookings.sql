with source as (

    select * from {{ source('bookings', 'bookings') }}

),

renamed as (

    select
        book_ref,
        book_date,
        total_amount

    from source

)

select * from renamed
