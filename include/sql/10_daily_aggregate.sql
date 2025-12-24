-- include/sql/01_build_daily_aggregate.sql
BEGIN;

DELETE FROM agg_device_day
WHERE partition_date = DATE :ds;

INSERT INTO agg_device_day (
  partition_date, device_id, num_events, trip_count,
  avg_speed, max_speed, max_dtc_count, dtc_events, updated_at
)
SELECT
  partition_date,
  device_id,
  COUNT(*)::INT,
  COUNT(DISTINCT trip_id)::INT,
  AVG(speed),
  MAX(speed),
  MAX(dtc_count),
  SUM(CASE WHEN COALESCE(dtc_count, 0) > 0 THEN 1 ELSE 0 END)::INT,
  NOW()
FROM raw_telematics
WHERE partition_date = DATE :ds
GROUP BY partition_date, device_id;

COMMIT;
