import datetime as dt
import hashlib
import json
import os
import time
from typing import Any, Dict, Iterable, List, Optional

import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException

# -------- Runtime configuration --------
PROJECT_ID = os.environ.get("GCP_PROJECT") or os.environ.get("GCLOUD_PROJECT")
MBTA_BASE_URL = "https://api-v3.mbta.com"

SERVICE_DAY_SHIFT_HOURS = 3  # MBTA treats the service day as ending at 3 a.m. local time


def _get_airflow_var(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Prefer environment overrides AIRFLOW_VAR_<KEY>, otherwise fall back to Airflow Variables.
    """
    env_key = f"AIRFLOW_VAR_{key.upper()}"
    if env_key in os.environ:
        return os.environ[env_key]
    from airflow.models import Variable  # Lazy import so parsing works even before metadata DB is ready

    return Variable.get(key, default_var=default)


BQ_DATASET = _get_airflow_var("BQ_DATASET")
GCS_BUCKET = _get_airflow_var("GCS_BUCKET")
MBTA_API_KEY = _get_airflow_var("MBTA_API_KEY")
BQ_LOCATION = _get_airflow_var("BQ_LOCATION", "US") or "US"
BQ_PROJECT = _get_airflow_var("BQ_PROJECT_ID") or PROJECT_ID
log = logging.getLogger(__name__)

PRED_TABLE = f"{BQ_DATASET}.rt_trip_stop_predictions"
PRED_STAGING = f"{BQ_DATASET}.rt_trip_stop_predictions_staging"
VEH_TABLE = f"{BQ_DATASET}.rt_vehicle_positions"
VEH_STAGING = f"{BQ_DATASET}.rt_vehicle_positions_staging"

PRED_SCHEMA = [
    {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "observed_at_utc", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "feed_timestamp_utc", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "route_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "direction_id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "trip_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "stop_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "parent_station", "type": "STRING", "mode": "NULLABLE"},
    {"name": "service_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "sched_arrival_utc", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "sched_departure_utc", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "pred_arrival_utc", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "pred_departure_utc", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "delay_sec", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "current_status", "type": "STRING", "mode": "NULLABLE"},
    {"name": "headway_branch_sec", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "headway_trunk_sec", "type": "INTEGER", "mode": "NULLABLE"},
]

VEH_SCHEMA = [
    {"name": "snapshot_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "observed_at_utc", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "vehicle_timestamp_utc", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "vehicle_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "vehicle_label", "type": "STRING", "mode": "NULLABLE"},
    {"name": "route_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "direction_id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "trip_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "stop_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "bearing", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "speed_mps", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "current_status", "type": "STRING", "mode": "NULLABLE"},
    {"name": "occupancy_status", "type": "STRING", "mode": "NULLABLE"},
    {"name": "occupancy_percent", "type": "INTEGER", "mode": "NULLABLE"},
]


def _parse_ts(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    try:
        # MBTA returns ISO-8601 strings with a trailing Z.
        return (
            dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
            .astimezone(dt.timezone.utc)
            .isoformat()
        )
    except ValueError:
        return None


def _service_date(obs_iso: str) -> str:
    obs = dt.datetime.fromisoformat(obs_iso.replace("Z", "+00:00"))
    shifted = obs - dt.timedelta(hours=SERVICE_DAY_SHIFT_HOURS)
    return shifted.date().isoformat()


def _sha1(*parts: Any) -> str:
    digest = hashlib.sha1()
    digest.update("|".join("" if p is None else str(p) for p in parts).encode("utf-8"))
    return digest.hexdigest()


def _headers() -> Dict[str, str]:
    head = {"Accept": "application/json"}
    if MBTA_API_KEY:
        head["x-api-key"] = MBTA_API_KEY
    return head


def _collect(url: str, params: Optional[Dict[str, Any]] = None, max_pages: int = 10) -> List[Dict[str, Any]]:
    pages: List[Dict[str, Any]] = []
    page = 0
    while url and page < max_pages:
        response = requests.get(url, headers=_headers(), params=params, timeout=20)
        response.raise_for_status()
        payload = response.json()
        pages.append(payload)
        url = payload.get("links", {}).get("next")
        params = None  # The next link already contains pagination params.
        page += 1
        time.sleep(0.2)
    return pages


def _chunks(seq: List[str], size: int) -> Iterable[List[str]]:
    """Yield successive chunks from a list."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _norm_rel(rel: Dict[str, Any], key: str) -> Optional[str]:
    """Safely extract relationships.<key>.data.id"""
    try:
        data = ((rel or {}).get(key) or {}).get("data") or {}
        return data.get("id")
    except Exception:
        return None


def _route_ids(route_types: str = "0,1,2,3") -> List[str]:
    pages = _collect(
        f"{MBTA_BASE_URL}/routes",
        params={"filter[type]": route_types, "page[limit]": "1000"},
        max_pages=3,
    )
    ids: List[str] = []
    for page in pages:
        ids.extend(
            r.get("id")
            for r in page.get("data", [])
            if isinstance(r, dict) and r.get("id")
        )
    return ids


def _now_iso_utc() -> str:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()


def extract_predictions_to_gcs(**_context) -> str:
    gcs = GCSHook()
    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    gcs_path = f"raw/predictions/dt={ts}/pred_{ts}.jsonl"

    route_ids = _route_ids()
    if not route_ids:
        log.warning("No route ids fetched from MBTA; skipping predictions pull.")
        raise AirflowSkipException("No route ids available")

    now_iso = _now_iso_utc()
    lines: List[str] = []
    seen: set[str] = set()
    BATCH = 50

    for batch in _chunks(route_ids, BATCH):
        params = {
            "include": "stop,route,trip",
            "page[limit]": "1000",
            "filter[route]": ",".join(batch),
        }
        pages = _collect(f"{MBTA_BASE_URL}/predictions", params=params, max_pages=5)
        got = 0
        for page in pages:
            for item in page.get("data", []):
                attributes = item.get("attributes", {})
                relationships = item.get("relationships") or {}
                trip_id = _norm_rel(relationships, "trip")
                stop_id = _norm_rel(relationships, "stop")
                route_id = _norm_rel(relationships, "route")
                direction_id = attributes.get("direction_id")
                pred_arrival = _parse_ts(attributes.get("arrival_time"))
                pred_departure = _parse_ts(attributes.get("departure_time"))
                feed_ts = _parse_ts(attributes.get("updated_at"))

                event_id = _sha1(trip_id, stop_id, pred_arrival, pred_departure, feed_ts)
                if event_id in seen:
                    continue
                seen.add(event_id)

                record = {
                    "event_id": event_id,
                    "observed_at_utc": now_iso,
                    "feed_timestamp_utc": feed_ts,
                    "route_id": route_id,
                    "direction_id": direction_id,
                    "trip_id": trip_id,
                    "stop_id": stop_id,
                    "parent_station": None,
                    "service_date": _service_date(now_iso),
                    "sched_arrival_utc": None,
                    "sched_departure_utc": None,
                    "pred_arrival_utc": pred_arrival,
                    "pred_departure_utc": pred_departure,
                    "delay_sec": None,
                    "current_status": attributes.get("status"),
                    "headway_branch_sec": None,
                    "headway_trunk_sec": None,
                }
                lines.append(json.dumps(record))
                got += 1
        log.info("predictions batch size=%d -> records=%d", len(batch), got)

    if not lines:
        log.warning("Route-chunked predictions 0 records; retry with route_type fallback.")
        pages = _collect(
            f"{MBTA_BASE_URL}/predictions",
            params={
                "include": "stop,route,trip",
                "page[limit]": "1000",
                "filter[route_type]": "0,1,2,3",
            },
            max_pages=5,
        )
        for page in pages:
            for item in page.get("data", []):
                attributes = item.get("attributes", {})
                relationships = item.get("relationships") or {}
                trip_id = _norm_rel(relationships, "trip")
                stop_id = _norm_rel(relationships, "stop")
                route_id = _norm_rel(relationships, "route")
                pred_arrival = _parse_ts(attributes.get("arrival_time"))
                pred_departure = _parse_ts(attributes.get("departure_time"))
                feed_ts = _parse_ts(attributes.get("updated_at"))
                event_id = _sha1(trip_id, stop_id, pred_arrival, pred_departure, feed_ts)
                if event_id in seen:
                    continue
                seen.add(event_id)
                lines.append(
                    json.dumps(
                        {
                            "event_id": event_id,
                            "observed_at_utc": now_iso,
                            "feed_timestamp_utc": feed_ts,
                            "route_id": route_id,
                            "direction_id": attributes.get("direction_id"),
                            "trip_id": trip_id,
                            "stop_id": stop_id,
                            "parent_station": None,
                            "service_date": _service_date(now_iso),
                            "sched_arrival_utc": None,
                            "sched_departure_utc": None,
                            "pred_arrival_utc": pred_arrival,
                            "pred_departure_utc": pred_departure,
                            "delay_sec": None,
                            "current_status": attributes.get("status"),
                            "headway_branch_sec": None,
                            "headway_trunk_sec": None,
                        }
                    )
                )

    log.info("predictions total records=%d (unique event_id)", len(lines))
    if not lines:
        raise AirflowSkipException("No predictions returned from MBTA API")

    gcs.upload(
        bucket_name=GCS_BUCKET,
        object_name=gcs_path,
        data="\n".join(lines),
        mime_type="application/json",
    )
    return gcs_path


def extract_vehicles_to_gcs(**_context) -> str:
    gcs = GCSHook()
    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    gcs_path = f"raw/vehicles/dt={ts}/veh_{ts}.jsonl"

    pages = _collect(
        f"{MBTA_BASE_URL}/vehicles",
        params={
            "include": "route,trip,stop",
            "page[limit]": "1000",
            "filter[direction_id]": "0,1",
        },
        max_pages=5,
    )

    now_iso = _now_iso_utc()
    lines: List[str] = []
    for page in pages:
        for item in page.get("data", []):
            attributes = item.get("attributes", {})
            rel_trip = ((item.get("relationships") or {}).get("trip") or {}).get("data") or {}
            rel_stop = ((item.get("relationships") or {}).get("stop") or {}).get("data") or {}
            rel_route = ((item.get("relationships") or {}).get("route") or {}).get("data") or {}
            vehicle_id = item.get("id")

            trip_id = rel_trip.get("id")
            stop_id = rel_stop.get("id")
            route_id = rel_route.get("id")

            vehicle_ts = _parse_ts(attributes.get("updated_at"))
            snapshot_id = _sha1(
                vehicle_id,
                attributes.get("latitude"),
                attributes.get("longitude"),
                vehicle_ts,
            )

            record = {
                "snapshot_id": snapshot_id,
                "observed_at_utc": now_iso,
                "vehicle_timestamp_utc": vehicle_ts,
                "vehicle_id": vehicle_id,
                "vehicle_label": attributes.get("label"),
                "route_id": route_id,
                "direction_id": attributes.get("direction_id"),
                "trip_id": trip_id,
                "stop_id": stop_id,
                "latitude": attributes.get("latitude"),
                "longitude": attributes.get("longitude"),
                "bearing": attributes.get("bearing"),
                "speed_mps": attributes.get("speed"),
                "current_status": attributes.get("current_status"),
                "occupancy_status": attributes.get("occupancy_status"),
                "occupancy_percent": attributes.get("occupancy_percentage"),
            }
            lines.append(json.dumps(record))

    gcs.upload(
        bucket_name=GCS_BUCKET,
        object_name=gcs_path,
        data="\n".join(lines),
        mime_type="application/json",
    )
    return gcs_path


def _build_create_table_sql(
    table_fqn: str,
    schema: List[Dict[str, str]],
    partition_field: Optional[str] = None,
) -> str:
    columns = []
    type_overrides = {
        "FLOAT": "FLOAT64",
        "INTEGER": "INT64",
    }
    for field in schema:
        sql_type = type_overrides.get(field["type"], field["type"])
        col = f"`{field['name']}` {sql_type}"
        if field.get("mode") == "REQUIRED":
            col += " NOT NULL"
        columns.append(col)
    ddl = f"CREATE TABLE IF NOT EXISTS `{table_fqn}` (\n  " + ",\n  ".join(columns) + "\n)"
    if partition_field:
        ddl += f"\nPARTITION BY DATE({partition_field});"
    else:
        ddl += ";"
    return ddl


with DAG(
    dag_id="mbta_realtime_to_bigquery",
    start_date=days_ago(1),
    schedule_interval="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 0},
    is_paused_upon_creation=True,
    tags=["mbta", "realtime", "bigquery"],
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        project_id=BQ_PROJECT,
        dataset_id=BQ_DATASET,
        location=BQ_LOCATION,
        exists_ok=True,
    )

    create_pred_base = BigQueryInsertJobOperator(
        task_id="create_pred_base",
        configuration={
            "query": {
                "query": _build_create_table_sql(
                    PRED_TABLE, PRED_SCHEMA, partition_field="observed_at_utc"
                ),
                "useLegacySql": False,
            }
        },
        project_id=BQ_PROJECT,
        location=BQ_LOCATION,
    )

    create_pred_staging = BigQueryInsertJobOperator(
        task_id="create_pred_staging",
        configuration={
            "query": {
                "query": _build_create_table_sql(PRED_STAGING, PRED_SCHEMA),
                "useLegacySql": False,
            }
        },
        project_id=BQ_PROJECT,
        location=BQ_LOCATION,
    )

    create_veh_base = BigQueryInsertJobOperator(
        task_id="create_veh_base",
        configuration={
            "query": {
                "query": _build_create_table_sql(
                    VEH_TABLE, VEH_SCHEMA, partition_field="observed_at_utc"
                ),
                "useLegacySql": False,
            }
        },
        project_id=BQ_PROJECT,
        location=BQ_LOCATION,
    )

    create_veh_staging = BigQueryInsertJobOperator(
        task_id="create_veh_staging",
        configuration={
            "query": {
                "query": _build_create_table_sql(VEH_STAGING, VEH_SCHEMA),
                "useLegacySql": False,
            }
        },
        project_id=BQ_PROJECT,
        location=BQ_LOCATION,
    )

    pred_to_gcs = PythonOperator(
        task_id="extract_predictions_to_gcs",
        python_callable=extract_predictions_to_gcs,
    )

    veh_to_gcs = PythonOperator(
        task_id="extract_vehicles_to_gcs",
        python_callable=extract_vehicles_to_gcs,
    )

    load_pred_stage = GCSToBigQueryOperator(
        task_id="load_pred_stage",
        bucket=GCS_BUCKET,
        source_objects=["{{ ti.xcom_pull(task_ids='extract_predictions_to_gcs') }}"],
        destination_project_dataset_table=PRED_STAGING,
        schema_fields=PRED_SCHEMA,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        source_format="NEWLINE_DELIMITED_JSON",
        project_id=BQ_PROJECT,
        location=BQ_LOCATION,
    )

    load_veh_stage = GCSToBigQueryOperator(
        task_id="load_veh_stage",
        bucket=GCS_BUCKET,
        source_objects=["{{ ti.xcom_pull(task_ids='extract_vehicles_to_gcs') }}"],
        destination_project_dataset_table=VEH_STAGING,
        schema_fields=VEH_SCHEMA,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        source_format="NEWLINE_DELIMITED_JSON",
        project_id=BQ_PROJECT,
        location=BQ_LOCATION,
    )

    merge_pred = BigQueryInsertJobOperator(
        task_id="merge_pred",
        configuration={
            "query": {
                "useLegacySql": False,
                "query": f"""
                MERGE `{PRED_TABLE}` T
                USING `{PRED_STAGING}` S
                ON T.event_id = S.event_id
                WHEN NOT MATCHED AND S.stop_id IS NOT NULL THEN
                  INSERT (
                    event_id, observed_at_utc, feed_timestamp_utc,
                    route_id, direction_id, trip_id, stop_id, parent_station,
                    service_date, sched_arrival_utc, sched_departure_utc,
                    pred_arrival_utc, pred_departure_utc, delay_sec,
                    current_status, headway_branch_sec, headway_trunk_sec
                  )
                  VALUES (
                    S.event_id, S.observed_at_utc, S.feed_timestamp_utc,
                    S.route_id, S.direction_id, S.trip_id, S.stop_id, S.parent_station,
                    S.service_date, S.sched_arrival_utc, S.sched_departure_utc,
                    S.pred_arrival_utc, S.pred_departure_utc, S.delay_sec,
                    S.current_status, S.headway_branch_sec, S.headway_trunk_sec
                  );
                """,
            }
        },
        project_id=BQ_PROJECT,
        location=BQ_LOCATION,
    )

    truncate_pred_stage = BigQueryInsertJobOperator(
        task_id="truncate_pred_stage",
        configuration={
            "query": {
                "useLegacySql": False,
                "query": f"TRUNCATE TABLE `{PRED_STAGING}`",
            }
        },
        project_id=BQ_PROJECT,
        location=BQ_LOCATION,
    )

    merge_veh = BigQueryInsertJobOperator(
        task_id="merge_veh",
        configuration={
            "query": {
                "useLegacySql": False,
                "query": f"""
                MERGE `{VEH_TABLE}` T
                USING `{VEH_STAGING}` S
                ON T.snapshot_id = S.snapshot_id
                WHEN NOT MATCHED THEN
                  INSERT (
                    snapshot_id, observed_at_utc, vehicle_timestamp_utc,
                    vehicle_id, vehicle_label, route_id, direction_id,
                    trip_id, stop_id, latitude, longitude, bearing, speed_mps,
                    current_status, occupancy_status, occupancy_percent
                  )
                  VALUES (
                    S.snapshot_id, S.observed_at_utc, S.vehicle_timestamp_utc,
                    S.vehicle_id, S.vehicle_label, S.route_id, S.direction_id,
                    S.trip_id, S.stop_id, S.latitude, S.longitude, S.bearing, S.speed_mps,
                    S.current_status, S.occupancy_status, S.occupancy_percent
                  );
                """,
            }
        },
        project_id=BQ_PROJECT,
        location=BQ_LOCATION,
    )

    truncate_veh_stage = BigQueryInsertJobOperator(
        task_id="truncate_veh_stage",
        configuration={
            "query": {
                "useLegacySql": False,
                "query": f"TRUNCATE TABLE `{VEH_STAGING}`",
            }
        },
        project_id=BQ_PROJECT,
        location=BQ_LOCATION,
    )

    create_dataset >> [
        create_pred_base,
        create_pred_staging,
        create_veh_base,
        create_veh_staging,
    ]
    create_pred_base >> pred_to_gcs >> load_pred_stage >> merge_pred >> truncate_pred_stage
    create_veh_base >> veh_to_gcs >> load_veh_stage >> merge_veh >> truncate_veh_stage
