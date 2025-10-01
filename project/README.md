# Bluebikes + MBTA 周更管道（MotherDuck + Airflow + GCP）

## 快速开始

### 0) 选择运行方式（两选一）

- **Astronomer**：用此仓库 + `Dockerfile` + `astro deploy`。
- **GCP Composer**：创建环境 -> 在 Composer “Python 依赖”里加 `requirements.txt` 内容 -> 把 `dags/`、`plugins/`、`include/` 上传到环境的 `dags`/`plugins` 目录（或用 Git 同步）。

### 1) 准备 GCP
- 创建 **GCS** 存储桶（`GCS_BUCKET`）。
- BigQuery 权限：部署环境的默认服务账号要有 `BigQuery Admin`（至少 Dataset Create + Load 权限）与 `Storage Object Admin`。
- 若用 Composer，环境自动继承 GCP 凭据；若自托 Airflow / Astronomer，挂载 **Service Account JSON** 并设置 `GOOGLE_APPLICATION_CREDENTIALS`。

### 2) MotherDuck
- 在 MotherDuck 后台生成 **服务 Token**，设置环境变量 `motherduck_token`（推荐小写键名）。  
  连接字符串形如 `duckdb.connect("md:bb_mbta")`，Token 通过 env 注入。详见官方文档。  

### 3) 配置环境变量
- 复制 `.env.example` 为 `.env`，根据实际填写。
- Astronomer：在 Deployment 的 Env Vars 中逐项填入；Composer：在环境变量/连接中设置。

### 4) 部署与运行
- 启用 DAG `bluebikes_mbta_motherduck_weekly`。
- 手动 **Trigger** 一次验证；之后每周一 03:00（UTC）自动跑。

## 数据去向
- GCS：
  - `gs://$GCS_BUCKET/bluebikes/yyyymm=YYYYMM/bluebikes_trips_YYYYMM.parquet`
  - `gs://$GCS_BUCKET/mbta/gtfs_snapshot_date=YYYY-MM-DD/mbta_{stops|routes}.parquet`
- BigQuery（dataset=`$BQ_DATASET`）：
  - `bluebikes_trips`（追加）
  - `mbta_stops`（追加）
  - `mbta_routes`（追加）

## 常见问题
- **Bluebikes 命名**：优先 `YYYYMM-bluebikes-tripdata.zip`，老月分可能是 `*-hubway-tripdata.zip`（代码已兼容）。来源：Bluebikes System Data 页与公开案例。  
- **MBTA 静态数据**：`https://cdn.mbta.com/MBTA_GTFS.zip`，每周抓一份快照即可；API 基址 `https://api-v3.mbta.com/`（本方案默认不抓实时）。  
- **MotherDuck 连接失败**：确认已配置 `motherduck_token`；或临时用本地 DuckDB（日志会有 WARN）。
- **分区与建模**：当前直接 `WRITE_APPEND`；后续你可以在 BigQuery 做分区/聚簇/物化视图。


在 README 的“常见问题”里提到的来源：Bluebikes System Data（S3 桶直链）、命名模式示例与 MBTA GTFS/API 入口，分别见：Bluebikes 官方页面与第三方示例（命名）、MBTA 官方 API/仓库设置变量（GTFS URL）。

关键设计点（实话实说）

Airflow/Astronomer/Composer：只要跑得起来都行，别两套一起上。在 GCP 已经有 Composer 的话，用 Composer 就好；跨云或你团队已用 Astronomer，就用 Astronomer。

MotherDuck：这里是 轻量 ETL 刀片，把杂乱 CSV/ZIP 快速打平成统一 Parquet（DuckDB 的强项）。等数据积累太大（多年累加），把重 ETL 往 BigQuery 迁移。

Bluebikes 列名差异：不同年份字段名不一（Hubway/Bluebikes、旧/新 schema），我在 md_etl_utils.py 做了柔性映射（COALESCE 风格、缺就 NULL），保留统一字段。

幂等：默认每次只处理最近 N 个月，避免一次性扫爆。需要全量回填，把 BLUEBIKES_START_YYYYMM 往前调、BLUEBIKES_MAX_MONTHS_PER_RUN 临时调大即可。

安全：motherduck_token 别硬编码进代码库；GCP 凭据用服务账号或工作负载身份绑定，不要裸 JSON 放仓库。

你能立刻做的事

在 MotherDuck 后台拿 token，填到部署环境（motherduck_token）。

在 GCP 创建 GCS_BUCKET 和 BQ_DATASET（或交给 DAG 自动建 Dataset）。

用 Astronomer 或 Composer 部署以上文件。

手动触发 DAG，观察 GCS 和 BigQuery 表是否生成。

参考（只给关键信息来源）

Bluebikes System Data：季度发布历史行程数据，官方入口页含 S3 桶链接；S3 命名示例 YYYYMM-bluebikes-tripdata.zip。

Bluebikes 实时 / GBFS：官方建议新项目使用 GBFS 实时数据（本方案聚焦批量历史）。

MBTA 开发者：V3 API 门户；GTFS 静态直链 https://cdn.mbta.com/MBTA_GTFS.zip。 

GTFS 静态规范简介（多 txt 装进 zip）。

MotherDuck 连接方法：md: 前缀、使用 motherduck_token 环境变量。
