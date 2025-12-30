-- =========================================================
-- 00_create_tables.sql
-- Postgres schema for Levin Vehicle Telematics 
-- Columns in CSV:
--   timeStamp, tripID, accData, gps_speed, battery, cTemp, dtc, eLoad, iat
-- =========================================================

BEGIN;

-- Raw event table (near-source, backfill-safe)
CREATE TABLE IF NOT EXISTS raw_telematics (
  id                BIGSERIAL PRIMARY KEY,

  -- Core partitioning fields
  event_time        TIMESTAMPTZ NOT NULL,
  partition_date    DATE NOT NULL,

  -- Identifiers
  trip_id           TEXT NOT NULL,
  device_id         TEXT NOT NULL,

  -- Signals
  acc_data          TEXT,
  gps_speed         DOUBLE PRECISION,
  battery           DOUBLE PRECISION,
  coolant_temp_c    DOUBLE PRECISION,  -- cTemp
  dtc_count         INTEGER,           -- dtc
  engine_load       DOUBLE PRECISION,  -- eLoad
  intake_air_temp   DOUBLE PRECISION,  -- iat

  -- Optional metadata
  source_file       TEXT,
  ingested_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Idempotency: prevents duplicates on reruns/backfills
-- If you later discover a vehicle/device identifier, add it here too.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'raw_telematics_trip_time_uniq'
    ) THEN
        ALTER TABLE raw_telematics
            ADD CONSTRAINT raw_telematics_trip_time_uniq UNIQUE (trip_id, event_time);
    END IF;
END $$;

-- Speed up per-day processing
CREATE INDEX IF NOT EXISTS ix_raw_telematics_partition_date
  ON raw_telematics (partition_date);

-- Curated daily aggregates (per trip/day)
CREATE TABLE IF NOT EXISTS agg_trip_day (
  partition_date         DATE NOT NULL,
  trip_id                TEXT NOT NULL,

  num_events             INTEGER NOT NULL,

  avg_gps_speed          DOUBLE PRECISION,
  max_gps_speed          DOUBLE PRECISION,

  avg_battery            DOUBLE PRECISION,
  min_battery            DOUBLE PRECISION,
  max_battery            DOUBLE PRECISION,

  avg_coolant_temp_c     DOUBLE PRECISION,
  max_coolant_temp_c     DOUBLE PRECISION,

  avg_engine_load        DOUBLE PRECISION,
  max_engine_load        DOUBLE PRECISION,

  avg_intake_air_temp    DOUBLE PRECISION,
  max_intake_air_temp    DOUBLE PRECISION,

  dtc_events             INTEGER,      -- count of rows where dtc_count > 0
  max_dtc_count          INTEGER,

  updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (partition_date, trip_id)
);

CREATE INDEX IF NOT EXISTS ix_agg_trip_day_partition_date
  ON agg_trip_day (partition_date);

COMMIT;
