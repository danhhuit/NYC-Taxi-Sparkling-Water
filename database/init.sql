-- NYC Taxi final-project JDBC source.
-- Run with:
-- psql -U postgres -d nyc_taxi_db -f database/init.sql

CREATE TABLE IF NOT EXISTS public.zone_statistics (
    zone_id INTEGER PRIMARY KEY,
    zone_type VARCHAR(32) NOT NULL,
    congestion_level VARCHAR(16) NOT NULL,
    priority_score INTEGER NOT NULL CHECK (priority_score BETWEEN 0 AND 5)
);

INSERT INTO public.zone_statistics (
    zone_id,
    zone_type,
    congestion_level,
    priority_score
) VALUES
    (4,   'RESIDENTIAL', 'MEDIUM', 2),
    (13,  'BUSINESS',    'HIGH',   4),
    (79,  'COMMERCIAL',  'HIGH',   4),
    (90,  'BUSINESS',    'HIGH',   4),
    (132, 'AIRPORT',     'HIGH',   5),
    (138, 'AIRPORT',     'HIGH',   5),
    (161, 'COMMERCIAL',  'HIGH',   4),
    (237, 'RESIDENTIAL', 'MEDIUM', 3)
ON CONFLICT (zone_id) DO UPDATE SET
    zone_type = EXCLUDED.zone_type,
    congestion_level = EXCLUDED.congestion_level,
    priority_score = EXCLUDED.priority_score;

SELECT COUNT(*) AS zone_statistics_rows
FROM public.zone_statistics;
