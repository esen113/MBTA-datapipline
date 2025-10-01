# Astronomer 官方运行时镜像（包含 Airflow）
FROM quay.io/astronomer/astro-runtime:11.6.0

USER root
# 常用系统工具（unzip 等）
RUN apt-get update && apt-get install -y --no-install-recommends \
    unzip curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

USER astro
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 将项目代码复制进镜像
COPY dags/ /usr/local/airflow/dags/
COPY plugins/ /usr/local/airflow/plugins/
COPY include/ /usr/local/airflow/include/
