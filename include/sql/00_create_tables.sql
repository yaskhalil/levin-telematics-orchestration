-- =========================================================
-- 00_create_tables.sql
-- Postgres schema for Levin Vehicle Telematics 
-- Source columns (v2.csv):
--   tripID,deviceID,timeStamp,accData,gps_speed,battery,cTemp,dtc,eLoad,iat,imap,kpl,maf,rpm,speed,tAdv,tPos
-- =========================================================

BEGIN;

-- Raw event table (near-source, backfill-safe)
CREATE TABLE IF NOT EXISTS raw_telematics (
  id                BIGSERIAL PRIMARY KEY,

  -- Core partitioning fields
  event_time        TIMESTAMPTZ NOT NULL,
  partition_date    DATE NOT NULL,

  -- Identifiers
  -- NOTE: For this dataset, deviceID is treated as the vehicle identifier.
  vehicle_id        TEXT,
  device_id         TEXT,
  trip_id           TEXT,

  -- Key implementing the README uniqueness rule:
  -- (vehicle_id, event_time, COALESCE(trip_id,''))
  trip_id_key       TEXT GENERATED ALWAYS AS (COALESCE(trip_id, '')) STORED,

  -- Signals
  acc_data          TEXT,
  gps_speed         DOUBLE PRECISION,
  battery           DOUBLE PRECISION,
  coolant_temp_c    DOUBLE PRECISION,  -- cTemp
  dtc_count         INTEGER,           -- dtc
  engine_load       DOUBLE PRECISION,  -- eLoad
  intake_air_temp   DOUBLE PRECISION,  -- iat

  -- Additional signals present in v2.csv
  imap              DOUBLE PRECISION,
  kpl               DOUBLE PRECISION,
  maf               DOUBLE PRECISION,
  rpm               DOUBLE PRECISION,
  speed             DOUBLE PRECISION,
  t_adv             DOUBLE PRECISION,
  t_pos             DOUBLE PRECISION,

  -- Optional metadata
  source_file       TEXT,
  payload           JSONB,
  ingested_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Backward/forward compatibility for DBs created by earlier iterations
ALTER TABLE raw_telematics ADD COLUMN IF NOT EXISTS vehicle_id TEXT;
ALTER TABLE raw_telematics ADD COLUMN IF NOT EXISTS device_id TEXT;
ALTER TABLE raw_telematics ADD COLUMN IF NOT EXISTS trip_id TEXT;
-- Generated column used by uniqueness rule. For existing DBs, add it if missing.
ALTER TABLE raw_telematics
  ADD COLUMN IF NOT EXISTS trip_id_key TEXT GENERATED ALWAYS AS (COALESCE(trip_id, '')) STORED;
ALTER TABLE raw_telematics ADD COLUMN IF NOT EXISTS imap DOUBLE PRECISION;
ALTER TABLE raw_telematics ADD COLUMN IF NOT EXISTS kpl DOUBLE PRECISION;
ALTER TABLE raw_telematics ADD COLUMN IF NOT EXISTS maf DOUBLE PRECISION;
ALTER TABLE raw_telematics ADD COLUMN IF NOT EXISTS rpm DOUBLE PRECISION;
ALTER TABLE raw_telematics ADD COLUMN IF NOT EXISTS speed DOUBLE PRECISION;
ALTER TABLE raw_telematics ADD COLUMN IF NOT EXISTS t_adv DOUBLE PRECISION;
ALTER TABLE raw_telematics ADD COLUMN IF NOT EXISTS t_pos DOUBLE PRECISION;
ALTER TABLE raw_telematics ADD COLUMN IF NOT EXISTS payload JSONB;

-- Treat historical device_id values as vehicle_id if vehicle_id is missing
UPDATE raw_telematics
SET vehicle_id = device_id
WHERE vehicle_id IS NULL AND device_id IS NOT NULL;

-- Idempotency: enforce the README uniqueness rule
ALTER TABLE raw_telematics DROP CONSTRAINT IF EXISTS raw_telematics_trip_time_uniq;
ALTER TABLE raw_telematics DROP CONSTRAINT IF EXISTS raw_telematics_device_trip_time_uniq;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'raw_telematics_vehicle_time_tripkey_uniq'
    ) THEN
        ALTER TABLE raw_telematics
            ADD CONSTRAINT raw_telematics_vehicle_time_tripkey_uniq UNIQUE (vehicle_id, event_time, trip_id_key);
    END IF;
END $$;

-- Speed up per-day processing
CREATE INDEX IF NOT EXISTS ix_raw_telematics_partition_date
  ON raw_telematics (partition_date);

-- Curated daily aggregates (per vehicle/day) - README target table
CREATE TABLE IF NOT EXISTS agg_vehicle_day (
  partition_date         DATE NOT NULL,
  vehicle_id             TEXT NOT NULL,

  num_events             INTEGER NOT NULL,
  trip_count             INTEGER NOT NULL,

  avg_speed              DOUBLE PRECISION,
  max_speed              DOUBLE PRECISION,

  avg_rpm                DOUBLE PRECISION,
  max_rpm                DOUBLE PRECISION,

  -- Mapped from coolant_temp_c if no true engine temp exists
  max_engine_temp        DOUBLE PRECISION,

  max_dtc_count          INTEGER,
  dtc_events             INTEGER,

  updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (partition_date, vehicle_id)
);

CREATE INDEX IF NOT EXISTS ix_agg_vehicle_day_partition_date
  ON agg_vehicle_day (partition_date);

COMMIT;
