import gzip
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import requests
from dagster import AssetExecutionContext, ConfigurableResource, EnvVar, asset, AssetKey
from dagster_dbt import DbtCliResource, dbt_assets

DBT_PROJECT_DIR = "/Users/yakov_shepelev/Учеба/Институт/Диплом/dagster_air_service/src/analytics"
DBT_PROFILES_DIR = "/Users/yakov_shepelev/.dbt"


@dataclass
class DemoDbLoadConfig:
    dump_url: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str


class LoadResource(ConfigurableResource):
    dump_url: str = EnvVar("URL")
    db_host: str = EnvVar("PG_HOST")
    db_port: int = EnvVar("PG_PORT")
    db_name: str = EnvVar("PG_NAME")
    db_user: str = EnvVar("PG_USER")
    db_password: str = EnvVar("PG_PASS")

    def get_config(self) -> DemoDbLoadConfig:
        return DemoDbLoadConfig(
            dump_url=self.dump_url,
            db_host=self.db_host,
            db_port=self.db_port,
            db_name=self.db_name,
            db_user=self.db_user,
            db_password=self.db_password,
        )


_DB_LEVEL_PATTERNS = (
    re.compile(r"^\s*DROP\s+DATABASE\b", re.IGNORECASE),
    re.compile(r"^\s*CREATE\s+DATABASE\b", re.IGNORECASE),
    re.compile(r"^\s*ALTER\s+DATABASE\b", re.IGNORECASE),
    re.compile(r"^\s*\\connect\b", re.IGNORECASE),
    re.compile(r"^\s*\\c\b", re.IGNORECASE),
)


def _should_skip_line(line: str) -> bool:
    return any(pattern.search(line) for pattern in _DB_LEVEL_PATTERNS)


def download_dump(context: AssetExecutionContext, dump_url: str, target_path: str) -> None:
    context.log.info(f"Загрузка: {dump_url}")

    with requests.get(dump_url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with open(target_path, "wb") as file:
            shutil.copyfileobj(response.raw, file)


def filter_dump(
        context: AssetExecutionContext,
        source_gz_path: str,
        target_sql_path: str,
) -> None:
    context.log.info(
        "Начата фильтрация"
    )

    skipped = 0
    total = 0

    with gzip.open(source_gz_path, "rt", encoding="utf-8", errors="replace") as fin, open(
            target_sql_path, "wt", encoding="utf-8"
    ) as fout:
        for line in fin:
            total += 1
            if _should_skip_line(line):
                skipped += 1
                continue
            fout.write(line)

    context.log.info(
        f"Фильтрация завершена"
    )


def _build_psql_command(cfg: DemoDbLoadConfig, sql_file_path: str) -> list[str]:
    return [
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        cfg.db_host,
        "-p",
        str(cfg.db_port),
        "-U",
        cfg.db_user,
        "-d",
        cfg.db_name,
        "-f",
        sql_file_path,
    ]


def run_psql(
        context: AssetExecutionContext,
        cfg: DemoDbLoadConfig,
        sql_file_path: str,
) -> None:
    psql_cmd = _build_psql_command(cfg, sql_file_path)

    env = os.environ.copy()
    env["PGPASSWORD"] = cfg.db_password

    context.log.info(
        f"Идет запись данных в БД"
    )

    proc = subprocess.run(
        psql_cmd,
        env=env,
        capture_output=True,
        text=True,
    )

    if proc.returncode != 0:
        context.log.error(proc.stdout[-8000:] if proc.stdout else "")
        context.log.error(proc.stderr[-8000:] if proc.stderr else "")
        raise RuntimeError(f"psql failed with code {proc.returncode}")

    context.log.info("Данные загружены успешно")


@contextmanager
def _temporary_dump_paths() -> Generator[tuple[str, str], None, None]:
    with tempfile.TemporaryDirectory(prefix="pg_demo_load_") as tmpdir:
        gz_path = os.path.join(tmpdir, "demo.sql.gz")
        filtered_sql_path = os.path.join(tmpdir, "demo.filtered.sql")
        yield gz_path, filtered_sql_path


@asset(name="load_data_air_service", group_name="LOAD_DATA")
def load_data_air_service(
        context: AssetExecutionContext,
        demo_db_loader: LoadResource,
) -> None:
    cfg = demo_db_loader.get_config()

    with _temporary_dump_paths() as (gz_path, filtered_sql_path):
        download_dump(context, cfg.dump_url, gz_path)
        filter_dump(context, gz_path, filtered_sql_path)
        run_psql(context, cfg, filtered_sql_path)


@dbt_assets(
    manifest=DBT_PROJECT_DIR + "/target" + "/manifest.json",
    name="air_service_dbt_assets"
)
def air_service_dbt_assets(
        context: AssetExecutionContext,
        dbt: DbtCliResource,
):
    yield from dbt.cli(["build"],
        context=context
    ).stream()
