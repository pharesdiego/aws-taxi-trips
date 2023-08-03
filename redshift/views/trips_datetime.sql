CREATE OR REPLACE VIEW trips_datetime AS
SELECT
    trip_id,
    trip_distance,
    pickup_date.date as pickup_date,
    pickup_time.hour as pickup_hour,
    pickup_time.minute as pickup_minute,
    pickup_time.second as pickup_second,
    dropoff_date.date as dropoff_date,
    dropoff_time.hour as dropoff_hour,
    dropoff_time.minute as dropoff_minute,
    dropoff_time.second as dropoff_second,
    CAST(passenger_count AS INT),
    payment_type_id,
    total_amount
    FROM fact_trips
LEFT JOIN dim_dates AS pickup_date ON pickup_date.date_id = pickup_date_id
LEFT JOIN dim_dates AS dropoff_date ON dropoff_date.date_id = dropoff_date_id
LEFT JOIN dim_times AS pickup_time ON pickup_time.time_id = pickup_time_id
LEFT JOIN dim_times AS dropoff_time ON dropoff_time.time_id = dropoff_time_id
LEFT JOIN dim_locations AS pickup_loc ON pickup_loc.location_id = pickup_location_id
LEFT JOIN dim_locations AS dropoff_loc ON dropoff_loc.location_id = dropoff_location_id;
