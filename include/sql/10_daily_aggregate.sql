-- include/sql/10_daily_aggregate.sql
BEGIN;

DELETE FROM agg_vehicle_day
WHERE partition_date = DATE :ds;

INSERT INTO agg_vehicle_day (
  partition_date, vehicle_id, num_events, trip_count,
  avg_speed, max_speed,
  avg_rpm, max_rpm,
  max_engine_temp,
  max_dtc_count, dtc_events,
  updated_at
)
SELECT
  partition_date,
  COALESCE(vehicle_id, device_id) AS vehicle_id,
  COUNT(*)::INT,
  COUNT(DISTINCT trip_id)::INT,
  AVG(speed),
  MAX(speed),
  AVG(rpm),
  MAX(rpm),
  MAX(coolant_temp_c),
  MAX(dtc_count),
  SUM(CASE WHEN COALESCE(dtc_count, 0) > 0 THEN 1 ELSE 0 END)::INT,
  NOW()
FROM raw_telematics
WHERE partition_date = DATE :ds
GROUP BY partition_date, COALESCE(vehicle_id, device_id);

COMMIT;