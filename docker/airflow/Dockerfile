FROM apache/airflow:2.9.3-python3.11

# oracledb runs in thin mode (pure Python) and clickhouse-connect ships
# prebuilt wheels, so no system build toolchain or Oracle client is needed.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
