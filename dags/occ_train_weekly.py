from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyDatasetOperator,
)

PROJECT = "ba882-f25-class-project-team9"
DS_RT = "mbta_rt"
DS_ML = "mbta_ml"
BQ_LOCATION = "US"

SQL_FEATURES = f"""
CREATE SCHEMA IF NOT EXISTS `{PROJECT}.{DS_ML}`;
CREATE OR REPLACE TABLE `{PROJECT}.{DS_ML}.occ_features_min`
PARTITION BY DATE(ts) CLUSTER BY route_id AS
SELECT
  UPPER(TRIM(occupancy_status)) AS label,
  route_id, current_status,
  EXTRACT(HOUR FROM observed_at_utc) AS hour_of_day,
  EXTRACT(DAYOFWEEK FROM observed_at_utc) AS day_of_week,
  IF(EXTRACT(DAYOFWEEK FROM observed_at_utc) IN (1,7), 1, 0) AS is_weekend,
  ROUND(latitude, 3)  AS lat_bin,
  ROUND(longitude, 3) AS lon_bin,
  observed_at_utc AS ts
FROM `{PROJECT}.{DS_RT}.rt_vehicle_positions`
WHERE occupancy_status IS NOT NULL
  AND latitude BETWEEN 41.5 AND 42.9
  AND longitude BETWEEN -71.9 AND -70.5;
"""

SQL_TRAIN = f"""
CREATE OR REPLACE MODEL `{PROJECT}.{DS_ML}.occ_lr_min`
OPTIONS (MODEL_TYPE='logistic_reg', INPUT_LABEL_COLS=['label'], AUTO_CLASS_WEIGHTS=TRUE, MAX_ITERATIONS=50) AS
SELECT * EXCEPT(ts)
FROM `{PROJECT}.{DS_ML}.occ_features_min`
WHERE ts <  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 67 DAY);
"""

SQL_EVAL = f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DS_ML}.occ_eval_last` AS
SELECT * FROM ML.EVALUATE(
  MODEL `{PROJECT}.{DS_ML}.occ_lr_min`,
  (SELECT * EXCEPT(ts) FROM `{PROJECT}.{DS_ML}.occ_features_min`
   WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))
);
"""

with DAG(
    dag_id="occ_train_weekly",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="0 5 * * 1",
    catchup=False,
    tags=["mbta", "occ", "train"],
) as dag_occ_train:
    create_ml_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_ml_dataset",
        project_id=PROJECT,
        dataset_id=DS_ML,
        location=BQ_LOCATION,
        exists_ok=True,
        gcp_conn_id="google_cloud_default",
    )

    features = BigQueryInsertJobOperator(
        task_id="build_features",
        configuration={"query": {"query": SQL_FEATURES, "useLegacySql": False}},
        gcp_conn_id="google_cloud_default",
        location=BQ_LOCATION,
    )
    train = BigQueryInsertJobOperator(
        task_id="train_model",
        configuration={"query": {"query": SQL_TRAIN, "useLegacySql": False}},
        gcp_conn_id="google_cloud_default",
        location=BQ_LOCATION,
    )
    evaluate = BigQueryInsertJobOperator(
        task_id="evaluate_model",
        configuration={"query": {"query": SQL_EVAL, "useLegacySql": False}},
        gcp_conn_id="google_cloud_default",
        location=BQ_LOCATION,
    )

    create_ml_dataset >> features >> train >> evaluate
