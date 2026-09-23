-- =====================================================================
-- GridCare: Flink SQL for Confluent Cloud
-- Run each statement separately in a Flink SQL workspace, in order.
-- Start the simulator first so the topics (tables) and schemas exist.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1) Use the simulated event timestamps as event time
-- ---------------------------------------------------------------------
ALTER TABLE meter_readings MODIFY WATERMARK FOR reading_ts AS reading_ts - INTERVAL '2' MINUTE;
ALTER TABLE weather_obs    MODIFY WATERMARK FOR obs_ts     AS obs_ts     - INTERVAL '2' MINUTE;
ALTER TABLE grid_status    MODIFY WATERMARK FOR status_ts  AS status_ts  - INTERVAL '2' MINUTE;

-- ---------------------------------------------------------------------
-- 2) Per-meter usage in 10-minute tumbling windows
--    (millions of raw readings -> one row per home per window)
-- ---------------------------------------------------------------------
CREATE TABLE meter_usage_10m AS
SELECT
  meter_id,
  zip,
  window_start,
  window_end,
  AVG(kw)  AS avg_kw,
  MIN(kw)  AS min_kw,
  COUNT(*) AS readings
FROM TABLE(
  TUMBLE(TABLE meter_readings, DESCRIPTOR(reading_ts), INTERVAL '10' MINUTES))
GROUP BY meter_id, zip, window_start, window_end;

-- ---------------------------------------------------------------------
-- 3) Neighborhood conditions: window join of weather and grid status
-- ---------------------------------------------------------------------
CREATE TABLE zip_conditions_10m AS
SELECT
  w.zip,
  w.window_start,
  w.window_end,
  w.max_heat_index_f,
  g.outage_minutes
FROM (
  SELECT zip, window_start, window_end, MAX(heat_index_f) AS max_heat_index_f
  FROM TABLE(
    TUMBLE(TABLE weather_obs, DESCRIPTOR(obs_ts), INTERVAL '10' MINUTES))
  GROUP BY zip, window_start, window_end
) w
JOIN (
  SELECT zip, window_start, window_end,
         SUM(CASE WHEN status = 'OUTAGE' THEN 1 ELSE 0 END) AS outage_minutes
  FROM TABLE(
    TUMBLE(TABLE grid_status, DESCRIPTOR(status_ts), INTERVAL '10' MINUTES))
  GROUP BY zip, window_start, window_end
) g
ON  w.zip = g.zip
AND w.window_start = g.window_start
AND w.window_end   = g.window_end;

-- ---------------------------------------------------------------------
-- 4) GridCare alerts: vulnerable home + dangerous heat + no cooling
--
--    Tip: set the statement property sql.state-ttl (e.g. '2 h') in the
--    workspace settings to keep join state bounded.
--    If Flink treats `households` as an upsert table and complains, run:
--      ALTER TABLE households SET ('changelog.mode' = 'append');
-- ---------------------------------------------------------------------
CREATE TABLE gridcare_alerts AS
SELECT
  h.household_id,
  u.meter_id,
  u.zip,
  u.window_start,
  u.window_end,
  ROUND(u.avg_kw, 3)              AS avg_kw,
  ROUND(z.max_heat_index_f, 1)    AS heat_index_f,
  h.medical_device,
  h.age_65_plus,
  h.contact_name,
  h.contact_phone,
  CASE WHEN z.outage_minutes > 0
       THEN 'AREA_OUTAGE'               -- utility: prioritize restoration / cooling center
       ELSE 'INDIVIDUAL_COOLING_LOSS'   -- power is on but AC stopped: welfare check
  END AS alert_type,
  CASE WHEN h.medical_device <> 'NONE' THEN 'CRITICAL' ELSE 'HIGH' END AS priority,
  CONCAT(
    CASE WHEN h.medical_device <> 'NONE' THEN 'CRITICAL' ELSE 'HIGH' END,
    ': household ', h.household_id, ' in ', u.zip,
    ' using ', CAST(ROUND(u.avg_kw, 2) AS STRING), ' kW at heat index ',
    CAST(ROUND(z.max_heat_index_f, 0) AS STRING), 'F',
    CASE WHEN z.outage_minutes > 0 THEN ' (area outage)' ELSE ' (possible AC failure)' END,
    '. Contact ', h.contact_name, ' ', h.contact_phone
  ) AS message
FROM meter_usage_10m u
JOIN zip_conditions_10m z
  ON  u.zip = z.zip
  AND u.window_start = z.window_start
JOIN households h
  ON  u.meter_id = h.meter_id
WHERE h.medical_baseline = TRUE
  AND z.max_heat_index_f >= 100
  AND u.avg_kw < 0.3;

-- ---------------------------------------------------------------------
-- 5) Demo queries (run interactively, no need to persist)
-- ---------------------------------------------------------------------

-- Live alert feed
SELECT window_start, priority, alert_type, message
FROM gridcare_alerts;

-- Scale proof: how much raw data is flowing
SELECT window_start, COUNT(*) AS readings, COUNT(DISTINCT meter_id) AS meters
FROM TABLE(
  TUMBLE(TABLE meter_readings, DESCRIPTOR(reading_ts), INTERVAL '10' MINUTES))
GROUP BY window_start, window_end;

-- Neighborhood risk board: which ZIPs have the most at-risk homes right now
SELECT zip, alert_type, COUNT(DISTINCT household_id) AS at_risk_homes
FROM gridcare_alerts
GROUP BY zip, alert_type;
