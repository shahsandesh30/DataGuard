-- 1. Basic shape: row count, station count, parameter count, time range
SELECT
    COUNT(*)                       AS n_rows,
    COUNT(DISTINCT locationid)    AS n_stations,
    COUNT(DISTINCT parameter)      AS n_parameters,
    COUNT(DISTINCT sensor_id)      AS n_sensors,
    MIN(datetime)              AS earliest_ts,
    MAX(datetime)              AS latest_ts
FROM silver_data;


-- 2. sensor_id cardinality per (locationid, parameter) - must be 1 for a given (locationid, parameter) pair
SELECT
    locationid,
    parameter,
    COUNT(DISTINCT sensor_id) AS n_sensors
FROM silver_data
GROUP BY locationid, parameter
ORDER BY n_sensors DESC, locationid, parameter;


-- 3. What exact parameter names/casing exist?
SELECT DISTINCT parameter, unit
FROM silver_data
ORDER BY parameter;


-- 4. Per-station-parameter coverage: row count and null rate
SELECT
    locationid,
    parameter,
    COUNT(*)                                       AS n_rows,
    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS n_nulls,
    MIN(datetime)                              AS first_ts,
    MAX(datetime)                              AS last_ts
FROM silver_data
GROUP BY locationid, parameter
ORDER BY n_rows ASC;  


-- 5. Expected vs actual row count per station-parameter
SELECT
    locationid,
    parameter,
    COUNT(*) AS actual_rows,
    DATE_DIFF('hour', MIN(datetime), MAX(datetime)) + 1 AS expected_rows_if_hourly,
    COUNT(*) * 1.0 / (DATE_DIFF('hour', MIN(datetime), MAX(datetime)) + 1) AS coverage_ratio
FROM silver_data
GROUP BY locationid, parameter
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
    locationid,
    parameter,
    COUNT(*)                       AS n,
    MIN(value)                     AS min_val,
    APPROX_PERCENTILE(value, 0.50) AS median,
    MAX(value)                     AS max_val,
    AVG(value)                     AS mean_val,
    STDDEV(value)                  AS stddev_val
FROM silver_data
GROUP BY locationid, parameter
ORDER BY parameter, locationid;


-- 8. Station coordinates — one row per station
SELECT DISTINCT
    locationid,
    location_name,
    latitude,
    longitude
FROM silver_data
ORDER BY locationid;


-- 9. Rough nearest-neighbor distance 
WITH stations AS (
    SELECT DISTINCT locationid, latitude, longitude
    FROM silver_data
),
pairs AS (
    SELECT
        a.locationid       AS locationid,
        b.locationid       AS neighbor_id,
        2 * 6371 * ASIN(SQRT(
            POWER(SIN(RADIANS(b.latitude - a.latitude) / 2), 2) +
            COS(RADIANS(a.latitude)) * COS(RADIANS(b.latitude)) *
            POWER(SIN(RADIANS(b.longitude - a.longitude) / 2), 2)
        )) AS distance_km
    FROM stations a
    CROSS JOIN stations b
    WHERE a.locationid <> b.locationid
)
SELECT
    locationid,
    COUNT(*) FILTER (WHERE distance_km <= 15)  AS n_neighbors_15km,
    COUNT(*) FILTER (WHERE distance_km <= 25)  AS n_neighbors_25km,
    MIN(distance_km)                           AS nearest_km
FROM pairs
GROUP BY locationid
ORDER BY n_neighbors_15km ASC;