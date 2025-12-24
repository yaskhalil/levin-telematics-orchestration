-- include/sql/00_create_tables.sql
BEGIN;

CREATE TABLE IF NOT EXISTS raw_telematics (
  id              BIGSERIAL PRIMARY KEY,
  event_time      TIMESTAMPTZ NOT NULL,
  partition_date  DATE NOT NULL,
  device_id       TEXT NOT NULL,
  trip_id         TEXT NOT NULL,

  acc_data        TEXT,
  gps_speed       DOUBLE PRECISION,
  battery         DOUBLE PRECISION,
  coolant_temp_c  DOUBLE PRECISION,
  dtc_count       INTEGER,
  engine_load     DOUBLE PRECISION,
  intake_air_temp DOUBLE PRECISION,
  imap            DOUBLE PRECISION,
  kpl             DOUBLE PRECISION,
  maf             DOUBLE PRECISION,
  rpm             DOUBLE PRECISION,
  speed           DOUBLE PRECISION,
  t_adv           DOUBLE PRECISION,
  t_pos           DOUBLE PRECISION
);

-- prevent duplicates on reruns
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ux_raw_telematics_event') THEN
    ALTER TABLE raw_telematics
      ADD CONSTRAINT ux_raw_telematics_event UNIQUE (device_id, trip_id, event_time);
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS agg_device_day (
  partition_date DATE NOT NULL,
  device_id      TEXT NOT NULL,
  num_events     INTEGER NOT NULL,
  trip_count     INTEGER,
  avg_speed      DOUBLE PRECISION,
  max_speed      DOUBLE PRECISION,
  max_dtc_count  INTEGER,
  dtc_events     INTEGER,
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (partition_date, device_id)
);

COMMIT;
