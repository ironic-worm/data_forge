with source as (

    select * from {{ source('bookings', 'airplanes_data') }}

),

renamed as (

    select
        airplane_code,
        model,
        range,
        speed

    from source

)

select * from renamed