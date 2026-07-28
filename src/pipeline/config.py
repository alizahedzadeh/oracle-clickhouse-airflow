import os
from dataclasses import dataclass


@dataclass(frozen=True)
class OracleConfig:
    host: str
    port: int
    service_name: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        return f"{self.host}:{self.port}/{self.service_name}"


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str
    http_port: int
    user: str
    password: str
    database: str


def oracle_config() -> OracleConfig:
    return OracleConfig(
        host=os.environ["ORACLE_HOST"],
        port=int(os.environ.get("ORACLE_PORT", 1521)),
        service_name=os.environ["ORACLE_SERVICE_NAME"],
        user=os.environ["ORACLE_APP_USER"],
        password=os.environ["ORACLE_APP_USER_PASSWORD"],
    )


def clickhouse_config() -> ClickHouseConfig:
    return ClickHouseConfig(
        host=os.environ["CLICKHOUSE_HOST"],
        http_port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", 8123)),
        user=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ.get("CLICKHOUSE_DB", "analytics"),
    )
