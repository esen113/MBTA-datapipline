"""
OCC baseline CLI utilities.

Usage examples:
  python mbta_api_to_bq.py --occ all
  python mbta_api_to_bq.py --occ predict7d
"""

from __future__ import annotations

import argparse
import sys
from google.cloud import bigquery

PROJECT = "ba882-f25-class-project-team9"
DS_RT = "mbta_rt"
DS_ML = "mbta_ml"


def _bq():
    return bigquery.Client(project=PROJECT)


def _run(sql: str):
    return _bq().query(sql).result()


def occ_build_features():
    _run(
        f"""
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
    )
    print("[occ] features table ready")


def occ_train_model():
    _run(
        f"""
    CREATE OR REPLACE MODEL `{PROJECT}.{DS_ML}.occ_lr_min`
    OPTIONS (MODEL_TYPE='logistic_reg', INPUT_LABEL_COLS=['label'], AUTO_CLASS_WEIGHTS=TRUE, MAX_ITERATIONS=50) AS
    SELECT * EXCEPT(ts)
    FROM `{PROJECT}.{DS_ML}.occ_features_min`
    WHERE ts <  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
      AND ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 67 DAY);
    """
    )
    print("[occ] model trained")


def occ_eval():
    _run(
        f"""
    CREATE OR REPLACE TABLE `{PROJECT}.{DS_ML}.occ_eval_last` AS
    SELECT * FROM ML.EVALUATE(
      MODEL `{PROJECT}.{DS_ML}.occ_lr_min`,
      (SELECT * EXCEPT(ts) FROM `{PROJECT}.{DS_ML}.occ_features_min`
       WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))
    );
    """
    )
    print("[occ] evaluation written (check Macro-F1)")


def occ_predict(days: int):
    assert days in (1, 7), "days must be 1 or 7"
    _run(
        f"""
    CREATE OR REPLACE TABLE `{PROJECT}.{DS_ML}.occ_predictions_{days}d` AS
    SELECT
      snapshot_id,
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
        WHERE observed_at_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
          AND occupancy_status IS NULL
          AND vehicle_id IS NOT NULL
          AND latitude BETWEEN 41.5 AND 42.9
          AND longitude BETWEEN -71.9 AND -70.5
      )
    );
    """
    )
    print(f"[occ] predictions_{days}d written")


def occ_publish_view():
    _run(
        f"""
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
    )
    print("[occ] publish view updated")


def occ_entry_from_cli(argv: list[str] | None = None) -> bool:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--occ",
        choices=["features", "train", "eval", "predict1d", "predict7d", "publish", "all"],
    )
    args, _ = parser.parse_known_args(argv)
    if not args.occ:
        return False

    if args.occ == "features":
        occ_build_features()
    elif args.occ == "train":
        occ_train_model()
    elif args.occ == "eval":
        occ_eval()
    elif args.occ == "predict1d":
        occ_predict(1)
    elif args.occ == "predict7d":
        occ_predict(7)
    elif args.occ == "publish":
        occ_publish_view()
    elif args.occ == "all":
        occ_build_features()
        occ_train_model()
        occ_eval()
        occ_predict(1)
        occ_publish_view()
    return True


if __name__ == "__main__":
    if occ_entry_from_cli(sys.argv[1:]):
        sys.exit(0)
