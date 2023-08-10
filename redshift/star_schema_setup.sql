CREATE TABLE dim_vendors (
    vendor_id INT NOT NULL PRIMARY KEY,
    vendor_name VARCHAR
)
    DISTSTYLE ALL;

CREATE TABLE dim_dates (
    date_id INT NOT NULL PRIMARY KEY,
    date DATE,
    year INT,
    month INT,
    day INT,
    weekday INT,
    is_holiday BOOLEAN
)
    DISTSTYLE ALL
    SORTKEY(date);

CREATE TABLE dim_times (
    time_id INT NOT NULL PRIMARY KEY,
    hour INT,
    minute INT,
    second INT
)
    DISTSTYLE ALL
    SORTKEY(hour, minute, second);

CREATE TABLE dim_locations (
    location_id INT NOT NULL PRIMARY KEY,
    borough VARCHAR,
    zone VARCHAR
)
    DISTSTYLE ALL
    SORTKEY(borough);

CREATE TABLE dim_rate_codes (
    rate_code_id INT NOT NULL PRIMARY KEY,
    rate_code_name VARCHAR
)
    DISTSTYLE ALL;

CREATE TABLE dim_payment_types (
    payment_type_id INT NOT NULL PRIMARY KEY,
    payment_type_name VARCHAR
)
    DISTSTYLE ALL;

CREATE TABLE fact_trips (
    trip_id VARCHAR NOT NULL PRIMARY KEY,
    vendor_id INT REFERENCES dim_vendors(vendor_id),
    pickup_date_id INT REFERENCES dim_dates(date_id),
    pickup_time_id INT REFERENCES dim_times(time_id),
    dropoff_date_id INT REFERENCES dim_dates(date_id),
    dropoff_time_id INT REFERENCES dim_times(time_id),
    passenger_count INT,
    trip_distance FLOAT,
    pickup_location_id INT REFERENCES dim_locations(location_id),
    dropoff_location_id INT REFERENCES dim_locations(location_id),
    rate_code_id INT REFERENCES dim_rate_codes(rate_code_id),
    store_and_forward BOOLEAN,
    payment_type_id INT REFERENCES dim_payment_types(payment_type_id),
    fare_amount FLOAT,
    extra_amount FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    congestion_surcharge FLOAT,
    airport_fee_amount FLOAT,
    total_amount FLOAT
)
    DISTKEY(pickup_date_id)
    SORTKEY(pickup_date_id, dropoff_date_id);
