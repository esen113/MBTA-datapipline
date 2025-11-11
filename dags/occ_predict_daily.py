from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT = "ba882-f25-class-project-team9"
DS_RT = "mbta_rt"
DS_ML = "mbta_ml"
BQ_LOCATION = "US"

SQL_PRED_1D = f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DS_ML}.occ_predictions_1d` AS
SELECT snapshot_id,
       observed_at_utc AS ts,
       vehicle_id,
       route_id,
       current_status,
       predicted_label
FROM ML.PREDICT(
  MODEL `{PROJECT}.{DS_ML}.occ_lr_min`,
  (
    SELECT
      route_id,
      current_status,
      EXTRACT(HOUR FROM observed_at_utc) AS hour_of_day,
      EXTRACT(DAYOFWEEK FROM observed_at_utc) AS day_of_week,
      IF(EXTRACT(DAYOFWEEK FROM observed_at_utc) IN (1,7), 1, 0) AS is_weekend,
      ROUND(latitude, 3)  AS lat_bin,
      ROUND(longitude, 3) AS lon_bin,
      snapshot_id,
      vehicle_id,
      observed_at_utc
    FROM `{PROJECT}.{DS_RT}.rt_vehicle_positions`
    WHERE observed_at_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
      AND occupancy_status IS NULL
      AND vehicle_id IS NOT NULL
      AND latitude BETWEEN 41.5 AND 42.9
      AND longitude BETWEEN -71.9 AND -70.5
  )
);
"""

SQL_VIEW = f"""
CREATE OR REPLACE VIEW `{PROJECT}.{DS_ML}.v_vehicle_positions_with_occ` AS
WITH pred AS (
  SELECT snapshot_id, ts, predicted_label
  FROM `{PROJECT}.{DS_ML}.occ_predictions_1d`
)
SELECT
  v.*,
  COALESCE(v.occupancy_status, p.predicted_label) AS occupancy_filled
FROM `{PROJECT}.{DS_RT}.rt_vehicle_positions` v
LEFT JOIN pred p
  ON p.snapshot_id = v.snapshot_id
 AND p.ts = v.observed_at_utc;
"""

with DAG(
    dag_id="occ_predict_daily",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="15 5 * * *",
    catchup=False,
    tags=["mbta", "occ", "predict"],
) as dag_occ_predict:
    predict = BigQueryInsertJobOperator(
        task_id="predict_occ_1d",
        configuration={"query": {"query": SQL_PRED_1D, "useLegacySql": False}},
        gcp_conn_id="google_cloud_default",
        location=BQ_LOCATION,
    )
    publish = BigQueryInsertJobOperator(
        task_id="publish_view",
        configuration={"query": {"query": SQL_VIEW, "useLegacySql": False}},
        gcp_conn_id="google_cloud_default",
        location=BQ_LOCATION,
    )

    predict >> publish
