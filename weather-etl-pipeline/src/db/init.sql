CREATE DATABASE IF NOT EXISTS weather_db;

CREATE TABLE IF NOT EXISTS weather_db.weather_hourly
(
    city String,
    date Date,
    hour UInt8,
    temperature Float32,
    precipitation Float32,
    wind_speed Float32,
    wind_direction UInt16,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (city, date, hour);

CREATE TABLE IF NOT EXISTS weather_db.weather_daily
(
    city String,
    date Date,
    temp_min Float32,
    temp_max Float32,
    temp_avg Float32,
    precipitation_total Float32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (city, date);