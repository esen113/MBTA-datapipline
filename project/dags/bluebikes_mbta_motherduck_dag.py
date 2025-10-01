import os
import tempfile
import datetime as dt
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

from plugins.md_etl_utils import (
    _env,
    list_candidate_yyyymm,
    bluebikes_download_zip,
    bluebikes_zip_to_parquet,
    mbta_gtfs_download,
    mbta_gtfs_to_parquet,
    gcs_upload,
    bq_load_parquet,
    bq_create_dataset_if_not_exists,
)

# ---- DAG 基本参数 ----
DAG_ID = "bluebikes_mbta_motherduck_weekly"
SCHEDULE = "0 3 * * 1"   # 每周一 03:00 UTC
DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    start_date=days_ago(1),
    schedule_interval=SCHEDULE,
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    tags=["bluebikes", "mbta", "motherduck", "gcp"],
) as dag:

    @task
    def ensure_bq_dataset():
        ds = _env("BQ_DATASET", "transport")
        return bq_create_dataset_if_not_exists(ds)

    @task
    def plan_bluebikes_months() -> List[str]:
        start_yyyymm = _env("BLUEBIKES_START_YYYYMM", "202301")
        max_m = int(_env("BLUEBIKES_MAX_MONTHS_PER_RUN", "2"))
        return list_candidate_yyyymm(start_yyyymm, max_months=max_m)

    @task
    def process_one_bluebikes_month(yyyymm: str) -> str:
        """
        下载 -> MD 清洗 -> 本地 Parquet -> 上传 GCS -> 返回 GCS URI
        """
        gcs_bucket = _env("GCS_BUCKET")
        assert gcs_bucket, "GCS_BUCKET 未设置"
        with tempfile.TemporaryDirectory() as td:
            # 1) 下载 zip
            fname, content = bluebikes_download_zip(yyyymm)
            # 2) 转换 parquet
            parquet_path = bluebikes_zip_to_parquet(content, yyyymm, td)
            # 3) 上传 GCS
            gcs_uri = f"gs://{gcs_bucket}/bluebikes/yyyymm={yyyymm}/{os.path.basename(parquet_path)}"
            gcs_upload(parquet_path, gcs_uri)
            return gcs_uri

    @task
    def load_bluebikes_to_bq(gcs_uri: str) -> str:
        dataset = _env("BQ_DATASET", "transport")
        table = f"{dataset}.bluebikes_trips"
        return bq_load_parquet(gcs_uri, table, write_disposition="WRITE_APPEND")

    @task
    def process_mbta_gtfs() -> List[str]:
        """
        下载 MBTA GTFS -> 解析为 stops/routes Parquet -> 上传 GCS -> 返回 GCS URI 列表
        """
        gcs_bucket = _env("GCS_BUCKET")
        assert gcs_bucket, "GCS_BUCKET 未设置"
        with tempfile.TemporaryDirectory() as td:
            fname, content = mbta_gtfs_download()
            out_files = mbta_gtfs_to_parquet(content, td)
            uris = []
            snap = dt.date.today().isoformat()
            for p in out_files:
                base = os.path.basename(p)
                gcs_uri = f"gs://{gcs_bucket}/mbta/gtfs_snapshot_date={snap}/{base}"
                gcs_upload(p, gcs_uri)
                uris.append(gcs_uri)
            return uris

    @task
    def load_mbta_to_bq(gcs_uris: List[str]) -> List[str]:
        dataset = _env("BQ_DATASET", "transport")
        results = []
        for uri in gcs_uris:
            base = os.path.basename(uri)
            if base.startswith("mbta_stops"):
                table = f"{dataset}.mbta_stops"
            elif base.startswith("mbta_routes"):
                table = f"{dataset}.mbta_routes"
            else:
                # 可扩展 trips/stop_times 时加这里
                table = f"{dataset}.mbta_misc"
            results.append(bq_load_parquet(uri, table, write_disposition="WRITE_TRUNCATE" if "mbta_misc" in table else "WRITE_APPEND"))
        return results

    # --- 依赖编排 ---
    _ = ensure_bq_dataset()
    months = plan_bluebikes_months()
    # 动态映射：每个待处理月份一个分支
    gcs_uris = process_one_bluebikes_month.expand(yyyymm=months)
    _ = load_bluebikes_to_bq.expand(gcs_uri=gcs_uris)

    mbta_uris = process_mbta_gtfs()
    _ = load_mbta_to_bq(mbta_uris)
