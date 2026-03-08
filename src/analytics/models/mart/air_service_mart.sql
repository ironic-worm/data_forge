SELECT b.book_ref,
       b.book_date,
       b.total_amount,
       t.ticket_no,
       t.passenger_id,
       t.passenger_name,
       t.outbound,
       seg.flight_id,
       seg.price,
       bp.boarding_no,
       bp.boarding_time,
       f.route_no,
       f.status,
       f.scheduled_departure,
       f.scheduled_arrival,
       f.actual_departure,
       f.actual_arrival,
       r.validity,
       r.departure_airport,
       r.arrival_airport,
       r.airplane_code,
       r.days_of_week,
       r.scheduled_time,
       r.duration,
       da.city          AS da_city,
       da.country       AS da_country,
       da.coordinates   AS da_coordinates,
       da.timezone      AS da_timezone,
       aa.airport_code  AS aa_airport_code,
       aa.airport_name  AS aa_airport_name,
       aa.city          AS aa_city,
       aa.country       AS aa_country,
       aa.coordinates   AS aa_coordinates,
       aa.timezone      AS aa_timezone,
       ap.airplane_code AS ap_airplane_code,
       ap.model,
       ap.range,
       ap.speed,
       s.seat_no,
       s.fare_conditions
FROM {{ ref('bookings') }} b
         JOIN {{ ref('tickets') }} t ON t.book_ref = b.book_ref
         JOIN {{ ref('segments') }} seg ON seg.ticket_no = t.ticket_no
         JOIN {{ ref('boarding_passes') }} bp ON bp.ticket_no = seg.ticket_no AND bp.flight_id = seg.flight_id
         JOIN {{ ref('flights') }} f ON seg.flight_id = f.flight_id
         JOIN LATERAL ( SELECT r_1.route_no,
                               r_1.validity,
                               r_1.departure_airport,
                               r_1.arrival_airport,
                               r_1.airplane_code,
                               r_1.days_of_week,
                               r_1.scheduled_time,
                               r_1.duration
                        FROM {{ ref('routes') }} r_1
                        WHERE r_1.route_no = f.route_no
                        ORDER BY r_1.validity DESC
                        LIMIT 1) r ON true
         JOIN {{ ref('airports_data') }} da ON da.airport_code = r.departure_airport
         JOIN {{ ref('airports_data') }} aa ON aa.airport_code = r.arrival_airport
         JOIN {{ ref('airplanes_data') }} ap ON ap.airplane_code = r.airplane_code
         JOIN {{ ref('seats') }} s ON s.airplane_code = ap.airplane_code AND bp.seat_no = s.seat_no