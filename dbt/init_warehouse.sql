CREATE schema IF NOT EXISTS bronze;
CREATE schema IF NOT EXISTS silver;
CREATE schema IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.weather_raw (
    ingested_at TIMESTAMP NOT NULL,
    city_name VARCHAR(50),
    latitude NUMERIC,
    longitude NUMERIC,
    day DATE NOT NULL,
    temp_max NUMERIC,
    temp_min NUMERIC,
    precipitation_sum NUMERIC
);