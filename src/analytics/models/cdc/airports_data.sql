with source as (

    select * from {{ source('bookings', 'airports_data') }}

),

renamed as (

    select
        airport_code,
        airport_name,
        city,
        country,
        coordinates,
        timezone

    from source

)

select * from renamed
