-- 1. Basic shape: row count, station count, parameter count, time range
SELECT
    COUNT(*)                       AS n_rows,
    COUNT(DISTINCT location_id)    AS n_stations,
    COUNT(DISTINCT parameter)      AS n_parameters,
    COUNT(DISTINCT sensor_id)      AS n_sensors,
    MIN(datetime_utc)              AS earliest_ts,
    MAX(datetime_utc)              AS latest_ts
FROM silver_data;


-- 2. sensor_id cardinality per (location_id, parameter) - must be 1 for a given (location_id, parameter) pair
SELECT
    location_id,
    parameter,
    COUNT(DISTINCT sensor_id) AS n_sensors
FROM silver_data
GROUP BY location_id, parameter
ORDER BY n_sensors DESC, location_id, parameter;


-- 3. What exact parameter names/casing exist?
SELECT DISTINCT parameter, unit
FROM silver_data
ORDER BY parameter;


-- 4. Per-station-parameter coverage: row count and null rate
SELECT
    location_id,
    parameter,
    COUNT(*)                                       AS n_rows,
    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS n_nulls,
    MIN(datetime_utc)                              AS first_ts,
    MAX(datetime_utc)                              AS last_ts
FROM silver_data
GROUP BY location_id, parameter
ORDER BY n_rows ASC;  


-- 5. Expected vs actual row count per station-parameter
SELECT
    location_id,
    parameter,
    COUNT(*) AS actual_rows,
    DATE_DIFF('hour', MIN(datetime_utc), MAX(datetime_utc)) + 1 AS expected_rows_if_hourly,
    COUNT(*) * 1.0 / (DATE_DIFF('hour', MIN(datetime_utc), MAX(datetime_utc)) + 1) AS coverage_ratio
FROM silver_data
GROUP BY location_id, parameter
ORDER BY coverage_ratio ASC;


-- 6. Value distribution per parameter (global)
SELECT
    parameter,
    unit,
    COUNT(*)                                    AS n,
    MIN(value)                                  AS min_val,
    APPROX_PERCENTILE(value, 0.01)              AS p01,
    APPROX_PERCENTILE(value, 0.50)              AS median,
    APPROX_PERCENTILE(value, 0.99)              AS p99,
    MAX(value)                                  AS max_val,
    AVG(value)                                  AS mean_val,
    STDDEV(value)                               AS stddev_val
FROM silver_data
GROUP BY parameter, unit
ORDER BY parameter;


-- 7. Same distribution, but per station
SELECT
    location_id,
    parameter,
    COUNT(*)                       AS n,
    MIN(value)                     AS min_val,
    APPROX_PERCENTILE(value, 0.50) AS median,
    MAX(value)                     AS max_val,
    AVG(value)                     AS mean_val,
    STDDEV(value)                  AS stddev_val
FROM silver_data
GROUP BY location_id, parameter
ORDER BY parameter, location_id;


-- 8. Station coordinates — one row per station
SELECT DISTINCT
    location_id,
    location_name,
    lat,
    lon
FROM silver_data
ORDER BY location_id;


-- 9. Rough nearest-neighbor distance 
WITH stations AS (
    SELECT DISTINCT location_id, lat, lon
    FROM silver_data
),
pairs AS (
    SELECT
        a.location_id       AS location_id,
        b.location_id       AS neighbor_id,
        2 * 6371 * ASIN(SQRT(
            POWER(SIN(RADIANS(b.lat - a.lat) / 2), 2) +
            COS(RADIANS(a.lat)) * COS(RADIANS(b.lat)) *
            POWER(SIN(RADIANS(b.lon - a.lon) / 2), 2)
        )) AS distance_km
    FROM stations a
    CROSS JOIN stations b
    WHERE a.location_id <> b.location_id
)
SELECT
    location_id,
    COUNT(*) FILTER (WHERE distance_km <= 15)  AS n_neighbors_15km,
    COUNT(*) FILTER (WHERE distance_km <= 25)  AS n_neighbors_25km,
    MIN(distance_km)                           AS nearest_km
FROM pairs
GROUP BY location_id
ORDER BY n_neighbors_15km ASC;