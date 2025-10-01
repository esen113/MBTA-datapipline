import os
import io
import re
import zipfile
import datetime as dt
from typing import List, Tuple, Optional

import duckdb
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from google.cloud import storage
from google.cloud import bigquery

# ---------- 通用 ----------
def _env(k: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(k, default)

def _http_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1.2,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=frozenset(["GET", "HEAD"]))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": "bb-mbta-pipeline/1.0"})
    return s

def _md_connect():
    """
    连接 MotherDuck:
    - 优先使用环境变量 motherduck_token（官方建议小写键名）
    - 兼容 MOTHERDUCK_TOKEN
    - 若都没有，退化为本地 DuckDB（开发友好，但不建议生产）
    """
    token = os.getenv("motherduck_token") or os.getenv("MOTHERDUCK_TOKEN")
    db = _env("MD_DATABASE", "bb_mbta")
    if token:
        # 官方推荐：md: 前缀 + 环境变量令牌（或在 URL 拼接）
        # 见 MotherDuck 文档：连接与认证。:contentReference[oaicite:3]{index=3}
        conn = duckdb.connect(f"md:{db}?motherduck_token={token}")
    else:
        # 开发兜底：本地内存库（生产请务必配置 token）
        conn = duckdb.connect("")  # :memory:
        print("[WARN] motherduck_token 未设置，使用本地 DuckDB 内存库。")
    # 更稳一些的参数
    conn.sql("PRAGMA threads=4")
    conn.sql("PRAGMA enable_progress_bar")
    return conn

# ---------- GCS / BigQuery ----------
def gcs_upload(local_path: str, gcs_uri: str):
    assert gcs_uri.startswith("gs://")
    _, _, rest = gcs_uri.partition("gs://")
    bucket_name, _, blob_name = rest.partition("/")
    client = storage.Client(project=_env("GCP_PROJECT"))
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    return gcs_uri

def bq_load_parquet(gcs_uri: str, table: str, write_disposition="WRITE_APPEND"):
    """
    从 Parquet 加载到 BigQuery（自动建表、自动推断 schema）。
    table 形如：project.dataset.table  或 dataset.table（会补 project）
    """
    client = bigquery.Client(project=_env("GCP_PROJECT"))
    if table.count(".") == 1:
        table = f"{client.project}.{table}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        autodetect=True,
    )
    job = client.load_table_from_uri(gcs_uri, table, job_config=job_config, location=_env("GCP_LOCATION", "US"))
    res = job.result()
    return f"{table} rows={res.output_rows}"

def bq_create_dataset_if_not_exists(dataset: str):
    client = bigquery.Client(project=_env("GCP_PROJECT"))
    ds_id = f"{client.project}.{dataset}"
    try:
        client.get_dataset(ds_id)
    except Exception:
        ds = bigquery.Dataset(ds_id)
        ds.location = _env("GCP_LOCATION", "US")
        client.create_dataset(ds, exists_ok=True)
    return ds_id

# ---------- Bluebikes ----------
def list_candidate_yyyymm(start_yyyymm: str, upto: Optional[dt.date] = None, max_months: int = 2) -> List[str]:
    """
    根据起始 YYYYMM 生成到当前上月的月份列表，最多返回 max_months 个“未处理”的候选。
    不做“是否已处理”的外部查询，简单裁剪，避免一次性扫全量。
    """
    y = int(start_yyyymm[:4]); m = int(start_yyyymm[4:])
    start = dt.date(y, m, 1)
    end = (upto or dt.date.today()).replace(day=1) - dt.timedelta(days=1)  # 上月最后一天
    months = []
    cur = start
    while cur <= end:
        months.append(f"{cur.year}{cur.month:02d}")
        # 下一个月
        if cur.month == 12:
            cur = dt.date(cur.year+1, 1, 1)
        else:
            cur = dt.date(cur.year, cur.month+1, 1)
    # 最近的在后面；我们取靠后的最多 max_months 个
    return months[-max_months:] if months else []

def bluebikes_download_zip(yyyymm: str, base: Optional[str] = None) -> Tuple[str, bytes]:
    """
    下载指定月份的 Bluebikes zip。
    先尝试 YYYYMM-bluebikes-tripdata.zip；
    较早年份也可能是 *-hubway-tripdata.zip（向后兼容）。
    """
    base = base or _env("BLUEBIKES_S3_BASE", "https://s3.amazonaws.com/hubway-data")
    sess = _http_session()

    for pattern in [f"{yyyymm}-bluebikes-tripdata.zip", f"{yyyymm}-hubway-tripdata.zip"]:
        url = f"{base.rstrip('/')}/{pattern}"
        head = sess.head(url)
        if head.status_code == 200:
            resp = sess.get(url)
            resp.raise_for_status()
            return (pattern, resp.content)
    raise FileNotFoundError(f"Bluebikes zip not found for {yyyymm}")

def _normalize_cols(cols: List[str]) -> List[str]:
    # 统一列名：小写、空格/特殊符号 -> 下划线
    norm = []
    for c in cols:
        x = re.sub(r"[^\w]+", "_", c.strip().lower()).strip("_")
        norm.append(x)
    return norm

def bluebikes_zip_to_parquet(zip_bytes: bytes, yyyymm: str, out_dir: str) -> str:
    """
    用 MotherDuck 统一不同年度的列名，导出规范 Parquet。
    统一后的字段（尽力映射老新模式）：
      ride_id, rideable_type, start_time, end_time,
      start_station_id, start_station_name, end_station_id, end_station_name,
      start_lat, start_lng, end_lat, end_lng, member_type
    """
    os.makedirs(out_dir, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        # 找出里面的 CSV（历史上有时一个月多个文件，全部 union）
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError("zip 内未发现 CSV")

        # 解压到内存 -> MotherDuck 读文件路径也行，但这里用内存临时盘
        tmp_csvs = []
        for n in csv_names:
            data = zf.read(n)
            p = os.path.join(out_dir, f"_{yyyymm}_{os.path.basename(n)}")
            with open(p, "wb") as f:
                f.write(data)
            tmp_csvs.append(p)

    con = _md_connect()
    # 把所有 csv union 到 raw 表
    raw_views = []
    for i, p in enumerate(tmp_csvs):
        v = f"v_{i}"
        # read_csv_auto 能自动推断类型并处理头部差异
        con.sql(f"CREATE OR REPLACE VIEW {v} AS SELECT * FROM read_csv_auto('{p}', ignore_errors=true)")
        raw_views.append(v)
    con.sql("CREATE OR REPLACE VIEW trips_raw AS " + " UNION ALL ".join([f"SELECT * FROM {v}" for v in raw_views]))
    # 获取列名并做柔性映射
    cols = [r[0] for r in con.sql("PRAGMA table_info('trips_raw')").fetchall()]
    ncols = _normalize_cols(cols)
    colmap = dict(zip(cols, ncols))

    def has_any(*cands):  # 返回第一个存在的标准化列名
        for c in cands:
            if c in ncols:
                return c
        return None

    # 老新列名候选（覆盖 Bluebikes 新模型 & 旧 Hubway 样式）
    c_start  = has_any("started_at", "start_time", "starttime")
    c_end    = has_any("ended_at", "end_time", "stoptime", "stop_time")
    c_sid    = has_any("start_station_id", "start_station_number", "start_station_code", "start_station_id_id", "start_station_id_")
    c_sname  = has_any("start_station_name", "start_station")
    c_eid    = has_any("end_station_id", "end_station_number", "end_station_code", "end_station_id_id", "end_station_id_")
    c_ename  = has_any("end_station_name", "end_station")
    c_slat   = has_any("start_lat", "start_latitude", "start_station_latitude")
    c_slng   = has_any("start_lng", "start_longitude", "start_station_longitude")
    c_elat   = has_any("end_lat", "end_latitude", "end_station_latitude")
    c_elng   = has_any("end_lng", "end_longitude", "end_station_longitude")
    c_rid    = has_any("ride_id", "trip_id")
    c_rtype  = has_any("rideable_type", "bike_type")
    c_member = has_any("member_casual", "usertype", "member_type")

    # 生成 SELECT 表达式（缺失就 NULL）
    def col_expr(c, cast=None):
        if c is None:
            return "NULL"
        expr = f'"{c}"'
        if cast:
            expr = f"CAST({expr} AS {cast})"
        return expr

    select_sql = f"""
    CREATE OR REPLACE TABLE trips_clean AS
    SELECT
      {col_expr(c_rid)}        AS ride_id,
      {col_expr(c_rtype)}      AS rideable_type,
      {col_expr(c_start, 'TIMESTAMP')} AS start_time,
      {col_expr(c_end,   'TIMESTAMP')} AS end_time,
      {col_expr(c_sid)}        AS start_station_id,
      {col_expr(c_sname)}      AS start_station_name,
      {col_expr(c_eid)}        AS end_station_id,
      {col_expr(c_ename)}      AS end_station_name,
      CAST({col_expr(c_slat)} AS DOUBLE) AS start_lat,
      CAST({col_expr(c_slng)} AS DOUBLE) AS start_lng,
      CAST({col_expr(c_elat)} AS DOUBLE) AS end_lat,
      CAST({col_expr(c_elng)} AS DOUBLE) AS end_lng,
      {col_expr(c_member)}     AS member_type,
      '{yyyymm}'               AS yyyymm
    FROM trips_raw
    WHERE 1=1
      AND (end_time IS NULL OR end_time >= '2011-01-01') -- 粗过滤异常
    """
    con.sql(select_sql)

    out_parquet = os.path.join(out_dir, f"bluebikes_trips_{yyyymm}.parquet")
    con.sql(f"COPY (SELECT * FROM trips_clean) TO '{out_parquet}' (FORMAT PARQUET)")
    return out_parquet

# ---------- MBTA GTFS ----------
def mbta_gtfs_download(url: Optional[str] = None) -> Tuple[str, bytes]:
    url = url or _env("MBTA_GTFS_URL", "https://cdn.mbta.com/MBTA_GTFS.zip")
    sess = _http_session()
    resp = sess.get(url)
    resp.raise_for_status()
    fname = f"MBTA_GTFS_{dt.date.today().isoformat()}.zip"
    return (fname, resp.content)

def mbta_gtfs_to_parquet(gtfs_zip: bytes, out_dir: str) -> List[str]:
    """
    从 GTFS zip 中解析出 stops.txt, routes.txt（可以按需扩展 trips/stop_times 等），导出 Parquet。
    GTFS 结构参考：Google/Transit 以及各机构的 GTFS 静态规范。:contentReference[oaicite:4]{index=4}
    """
    os.makedirs(out_dir, exist_ok=True)
    zf = zipfile.ZipFile(io.BytesIO(gtfs_zip))
    members = {n.lower(): n for n in zf.namelist()}
    required = ["stops.txt", "routes.txt"]
    out_files = []

    con = _md_connect()
    for fname in required:
        if fname not in members:
            print(f"[WARN] GTFS 内无 {fname}, 跳过")
            continue
        real = members[fname]
        with zf.open(real) as f:
            raw = f.read()
        tmp = os.path.join(out_dir, f"_{fname}")
        with open(tmp, "wb") as fw:
            fw.write(raw)
        tbl = fname.replace(".txt", "")
        con.sql(f"CREATE OR REPLACE VIEW v_{tbl} AS SELECT * FROM read_csv_auto('{tmp}', header=True, ignore_errors=true)")
        # 轻度规范化
        if tbl == "stops":
            con.sql("""
                CREATE OR REPLACE TABLE stops_clean AS
                SELECT
                  CAST(stop_id AS STRING) AS stop_id,
                  stop_name,
                  CAST(stop_lat AS DOUBLE) AS stop_lat,
                  CAST(stop_lon AS DOUBLE) AS stop_lon,
                  location_type,
                  parent_station
                FROM v_stops
            """)
            outp = os.path.join(out_dir, "mbta_stops.parquet")
            con.sql(f"COPY (SELECT * FROM stops_clean) TO '{outp}' (FORMAT PARQUET)")
            out_files.append(outp)
        elif tbl == "routes":
            con.sql("""
                CREATE OR REPLACE TABLE routes_clean AS
                SELECT
                  CAST(route_id AS STRING) AS route_id,
                  agency_id,
                  route_short_name,
                  route_long_name,
                  route_type
                FROM v_routes
            """)
            outp = os.path.join(out_dir, "mbta_routes.parquet")
            con.sql(f"COPY (SELECT * FROM routes_clean) TO '{outp}' (FORMAT PARQUET)")
            out_files.append(outp)
    return out_files
