#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gp_to_parquet.py — выгрузка таблицы Greenplum в parquet-файлы.

На каждое значение ключевого поля создаётся отдельный parquet-файл
в каталоге скрипта (или в --output-dir):

    exp_<schema>_<table>_<key_value>.parquet

Примеры
-------
Выгрузить всё из exp_schema.tbl в разрезе rrr_id:

    ./gp_to_parquet.py exp_schema tbl rrr_id all

По 1000 строк на каждый rrr_id, в 4 потока:

    ./gp_to_parquet.py exp_schema tbl rrr_id 1000 --workers 4

Только посмотреть план выгрузки (без записи файлов):

    ./gp_to_parquet.py exp_schema tbl rrr_id all --dry-run

Как это работает
----------------
1. Одним запросом получаем список значений ключа и количество строк по каждому
   (GROUP BY), чтобы заранее показать план и распределить нагрузку.
2. По каждому ключу выполняется серверный COPY ... TO STDOUT WITH CSV:
   Greenplum сам сериализует данные, Python их не разбирает построчно —
   это самый быстрый штатный способ забрать данные.
3. Локально DuckDB (нативный C++/векторный движок) читает CSV и пишет parquet
   с zstd-сжатием, не загружая всё в память.
4. Ключи обрабатываются параллельно (--workers), каждый поток — своё
   соединение с БД и свой экземпляр DuckDB.

Параметры подключения берутся из .env.secret_db рядом со скриптом
(GP_OUT_HOST, GP_OUT_PORT, GP_OUT_DB, GP_OUT_USER, GP_OUT_PASSWORD,
опционально GP_OUT_SSLMODE, GP_OUT_GSSENCMODE).

После каждой выгрузки в каталоге скрипта дописывается отчёт parq_resume.txt:
когда запускали, по какой таблице/ключу, сколько строк выгрузили, какие файлы
получились — или текст ошибки, если что-то пошло не так.

Зависимости: psycopg2-binary, duckdb (pyarrow не требуется).
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import getpass
import logging
import os
import re
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import duckdb
import psycopg2

LOG = logging.getLogger("gp_to_parquet")

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_ENV_FILE = SCRIPT_DIR / ".env.secret_db"
DEFAULT_RESUME_FILE = SCRIPT_DIR / "parq_resume.txt"

# Размер батча для COPY задаётся на стороне сервера, поэтому здесь важен только
# размер row group в parquet (в строках).
DEFAULT_ROW_GROUP_SIZE = 250_000
DEFAULT_COMPRESSION = "zstd"
DEFAULT_COMPRESSION_LEVEL = 3
DEFAULT_WORKERS = 4
DEFAULT_MAX_FILES = 10_000


# --------------------------------------------------------------------------- #
# Конфигурация подключения
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class GpConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    sslmode: str = "prefer"
    gssencmode: str = "disable"
    connect_timeout: int = 30

    def connect_kwargs(self, application_name: str = "gp_to_parquet") -> Dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "sslmode": self.sslmode,
            "gssencmode": self.gssencmode,
            "connect_timeout": self.connect_timeout,
            "application_name": application_name,
        }

    def describe(self) -> str:
        return f"{self.user}@{self.host}:{self.port}/{self.dbname}"


def load_env_file(path: Path) -> Dict[str, str]:
    """Читает KEY=VALUE из .env-файла (без shell-подстановок)."""
    if not path.is_file():
        raise SystemExit(f"Файл с параметрами подключения не найден: {path}")

    values: Dict[str, str] = {}
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        if "=" not in line:
            LOG.warning("%s:%d: строка без '=' пропущена", path, lineno)
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        values[key] = value
    return values


def build_config(env_file: Path) -> GpConfig:
    file_values = load_env_file(env_file)

    def get(name: str, default: Optional[str] = None, required: bool = False) -> str:
        value = file_values.get(name) or os.environ.get(name) or default
        if required and not value:
            raise SystemExit(f"Не задан параметр {name} (в {env_file} или в окружении)")
        return value or ""

    return GpConfig(
        host=get("GP_OUT_HOST", required=True),
        port=int(get("GP_OUT_PORT", "5432")),
        dbname=get("GP_OUT_DB", required=True),
        user=get("GP_OUT_USER", required=True),
        password=get("GP_OUT_PASSWORD", required=True),
        sslmode=get("GP_OUT_SSLMODE", "prefer"),
        gssencmode=get("GP_OUT_GSSENCMODE", "disable"),
        connect_timeout=int(get("GP_OUT_CONNECT_TIMEOUT", "30")),
    )


_ACTIVE_LOCK = threading.Lock()
_ACTIVE_CONNS: set = set()
_CANCELLED = threading.Event()


def connect(cfg: GpConfig, application_name: str = "gp_to_parquet"):
    conn = psycopg2.connect(**cfg.connect_kwargs(application_name))
    conn.autocommit = True
    with _ACTIVE_LOCK:
        _ACTIVE_CONNS.add(conn)
    return conn


def close_conn(conn) -> None:
    with _ACTIVE_LOCK:
        _ACTIVE_CONNS.discard(conn)
    try:
        conn.close()
    except Exception:  # noqa: BLE001 - закрытие не должно ронять выгрузку
        pass


_THREAD_LOCAL = threading.local()


def get_thread_conn(cfg: GpConfig):
    """Соединение, переиспользуемое потоком-воркером для всех его ключей."""
    conn = getattr(_THREAD_LOCAL, "conn", None)
    if conn is not None and not conn.closed:
        return conn
    conn = connect(cfg)
    _THREAD_LOCAL.conn = conn
    return conn


def drop_thread_conn() -> None:
    conn = getattr(_THREAD_LOCAL, "conn", None)
    _THREAD_LOCAL.conn = None
    if conn is not None:
        close_conn(conn)


def close_all_conns() -> None:
    with _ACTIVE_LOCK:
        conns = list(_ACTIVE_CONNS)
    for conn in conns:
        close_conn(conn)


def cancel_all() -> None:
    """Прерывает активные COPY при Ctrl+C (conn.cancel потокобезопасен)."""
    _CANCELLED.set()
    with _ACTIVE_LOCK:
        conns = list(_ACTIVE_CONNS)
    for conn in conns:
        try:
            conn.cancel()
        except Exception:  # noqa: BLE001
            pass


def connection_headroom(conn) -> Optional[int]:
    """Сколько ещё соединений можно открыть роли (None — без ограничений)."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.rolconnlimit,
                       (SELECT count(*) FROM pg_stat_activity WHERE usename = current_user)
                FROM pg_roles r
                WHERE r.rolname = current_user
                """
            )
            row = cur.fetchone()
    except Exception:  # noqa: BLE001 - на старых GP каталог может отличаться
        return None
    if not row or row[0] is None or int(row[0]) < 0:
        return None
    limit, active = int(row[0]), int(row[1])
    # текущее (discovery) соединение будет закрыто, ещё одно оставляем про запас
    return max(0, limit - active)


# --------------------------------------------------------------------------- #
# SQL-хелперы
# --------------------------------------------------------------------------- #

def qi(identifier: str) -> str:
    """Экранирует идентификатор (schema/table/column) двойными кавычками."""
    name = identifier.strip()
    if len(name) >= 2 and name[0] == '"' and name[-1] == '"':
        name = name[1:-1]
    if not name:
        raise SystemExit("Пустой идентификатор")
    if "\x00" in name:
        raise SystemExit(f"Недопустимый идентификатор: {identifier!r}")
    return '"' + name.replace('"', '""') + '"'


def sql_str(value: str) -> str:
    """Строковый литерал для DuckDB."""
    return "'" + value.replace("'", "''") + "'"


_PG_NUMERIC_RE = re.compile(r"^(?:numeric|decimal)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$")


def pg_type_to_duckdb(pg_type: str) -> str:
    """Отображение типа PostgreSQL/Greenplum в тип DuckDB для read_csv."""
    t = (pg_type or "").strip().lower()

    # составные/редкие типы, которые безопаснее читать как текст
    if t.endswith("[]") or t.startswith("interval") or t in (
        "bytea", "money", "inet", "cidr", "macaddr", "macaddr8", "xml",
        "bit", "bit varying", "varbit", "tsvector", "tsquery", "point",
        "line", "lseg", "box", "path", "polygon", "circle",
    ):
        return "VARCHAR"

    if t.startswith("character varying") or t.startswith("varchar"):
        return "VARCHAR"
    if t.startswith("character") or t.startswith("char"):
        return "VARCHAR"
    if t in ("text", "citext", "name", "oid"):
        return "VARCHAR"

    m = _PG_NUMERIC_RE.match(t)
    if m:
        precision, scale = int(m.group(1)), int(m.group(2))
        if precision <= 38 and scale <= precision:
            return f"DECIMAL({precision},{scale})"
        return "DOUBLE"
    if t in ("numeric", "decimal"):
        return "DOUBLE"

    if t in ("smallint", "int2"):
        return "SMALLINT"
    if t in ("integer", "int", "int4", "serial"):
        return "INTEGER"
    if t in ("bigint", "int8", "bigserial"):
        return "BIGINT"
    if t in ("real", "float4"):
        return "FLOAT"
    if t in ("double precision", "float8", "float"):
        return "DOUBLE"
    if t in ("boolean", "bool"):
        return "BOOLEAN"
    if t == "date":
        return "DATE"
    if t.startswith("timestamp"):
        return "TIMESTAMPTZ" if "with time zone" in t else "TIMESTAMP"
    if t.startswith("time"):
        # timetz в CSV выглядит как '10:00:00+03', надёжнее оставить текстом
        return "VARCHAR" if "with time zone" in t else "TIME"
    if t == "uuid":
        return "UUID"
    if t in ("json", "jsonb"):
        return "JSON"
    return "VARCHAR"


def fetch_columns(conn, schema: str, table: str) -> List[Tuple[str, str]]:
    """Возвращает [(имя колонки, тип DuckDB)] в порядке следования в таблице."""
    sql = """
        SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname = %s
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        rows = cur.fetchall()
    if not rows:
        raise SystemExit(f"Таблица {schema}.{table} не найдена или недоступна")
    return [(name, pg_type_to_duckdb(pg_type)) for name, pg_type in rows]


def fetch_key_groups(
    conn, schema: str, table: str, key: str, where: Optional[str]
) -> List[Tuple[Any, int]]:
    """[(значение ключа, количество строк)] по убыванию количества."""
    sql = f"SELECT {qi(key)} AS k, count(*) AS cnt FROM {qi(schema)}.{qi(table)}"
    if where:
        sql += f" WHERE ({where})"
    sql += " GROUP BY 1 ORDER BY 2 DESC"
    with conn.cursor() as cur:
        cur.execute(sql)
        return [(row[0], int(row[1])) for row in cur.fetchall()]


# --------------------------------------------------------------------------- #
# Имена файлов
# --------------------------------------------------------------------------- #

_UNSAFE_FILENAME_RE = re.compile(r"[^0-9A-Za-z._-]+")


def key_to_filename_part(value: Any, max_len: int = 80) -> str:
    if value is None:
        return "NULL"
    text = str(value).strip()
    if not text:
        return "EMPTY"
    safe = _UNSAFE_FILENAME_RE.sub("_", text).strip("._-")
    return (safe[:max_len] or "EMPTY")


def build_output_names(
    schema: str, table: str, key_values: Sequence[Any]
) -> List[str]:
    """Уникальные имена файлов (на случай коллизий после санитизации)."""
    prefix = f"exp_{key_to_filename_part(schema)}_{key_to_filename_part(table)}"
    used: Dict[str, int] = {}
    names: List[str] = []
    for value in key_values:
        base = f"{prefix}_{key_to_filename_part(value)}"
        count = used.get(base, 0) + 1
        used[base] = count
        names.append(f"{base}.parquet" if count == 1 else f"{base}_{count}.parquet")
    return names


# --------------------------------------------------------------------------- #
# Выгрузка одного ключа
# --------------------------------------------------------------------------- #

@dataclass
class TaskResult:
    key_value: Any
    parquet_path: Path
    rows: int = 0
    size: int = 0
    seconds: float = 0.0
    skipped: bool = False
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None


def default_temp_dir() -> Path:
    """Каталог для временных CSV (не .sql — правила проекта разрешают /tmp)."""
    candidates = [
        os.environ.get("GP_PARQUET_TMP"),
        f"/tmp/.private/{getpass.getuser()}/opencode",
        tempfile.gettempdir(),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate)
        try:
            path.mkdir(parents=True, exist_ok=True)
            if os.access(path, os.W_OK):
                return path
        except OSError:
            continue
    raise SystemExit("Не удалось найти каталог для временных файлов")


def copy_key_to_csv(
    conn,
    schema: str,
    table: str,
    key: str,
    key_value: Any,
    columns: Sequence[str],
    where: Optional[str],
    order_by: Optional[str],
    limit: Optional[int],
    csv_path: Path,
) -> None:
    """COPY (SELECT ...) TO STDOUT WITH CSV -> csv_path."""
    with conn.cursor() as cur:
        if key_value is None:
            key_cond = f"{qi(key)} IS NULL"
        else:
            literal = cur.mogrify("%s", (key_value,)).decode("utf-8")
            key_cond = f"{qi(key)} = {literal}"

        conditions = key_cond
        if where:
            conditions += f" AND ({where})"

        select_list = ", ".join(qi(c) for c in columns) if columns else "*"
        sql = f"SELECT {select_list} FROM {qi(schema)}.{qi(table)} WHERE {conditions}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"

        copy_sql = f"COPY ({sql}) TO STDOUT WITH CSV NULL AS '\\N'"
        with open(csv_path, "wb") as fh:
            cur.copy_expert(copy_sql, fh)


def csv_to_parquet(
    csv_path: Path,
    parquet_path: Path,
    column_types: Sequence[Tuple[str, str]],
    compression: str,
    compression_level: Optional[int],
    row_group_size: int,
    threads: int,
) -> int:
    """CSV -> parquet через DuckDB. Возвращает число строк в файле."""
    con = duckdb.connect()
    try:
        if threads > 0:
            con.execute(f"SET threads = {int(threads)}")

        columns_sql = ", ".join(
            f"{sql_str(name)}: {sql_str(duck_type)}" for name, duck_type in column_types
        )
        options = ["FORMAT parquet", f"COMPRESSION {compression}"]
        if compression_level is not None:
            options.append(f"COMPRESSION_LEVEL {int(compression_level)}")
        options.append(f"ROW_GROUP_SIZE {int(row_group_size)}")

        con.execute(
            f"""
            COPY (
                SELECT *
                FROM read_csv(
                    {sql_str(str(csv_path))},
                    header = false,
                    delim = ',',
                    quote = '"',
                    escape = '"',
                    nullstr = '\\N',
                    columns = {{{columns_sql}}},
                    auto_detect = false
                )
            ) TO {sql_str(str(parquet_path))} ({", ".join(options)})
            """
        )
        return int(
            con.execute(
                f"SELECT count(*) FROM read_parquet({sql_str(str(parquet_path))})"
            ).fetchone()[0]
        )
    finally:
        con.close()


def parquet_row_count(path: Path) -> int:
    con = duckdb.connect()
    try:
        return int(
            con.execute(
                f"SELECT count(*) FROM read_parquet({sql_str(str(path))})"
            ).fetchone()[0]
        )
    finally:
        con.close()


def export_group(
    cfg: GpConfig,
    schema: str,
    table: str,
    key: str,
    key_value: Any,
    columns: Sequence[Tuple[str, str]],
    where: Optional[str],
    order_by: Optional[str],
    limit: Optional[int],
    expected_rows: int,
    output_path: Path,
    temp_dir: Path,
    compression: str,
    compression_level: Optional[int],
    row_group_size: int,
    threads: int,
    keep_temp: bool,
    overwrite: bool,
    retries: int,
) -> TaskResult:
    result = TaskResult(key_value=key_value, parquet_path=output_path)
    started = time.time()

    if output_path.exists() and not overwrite:
        # Файл уже есть: пропускаем, только если в нём столько строк, сколько
        # ожидается сейчас (защита от старого файла, сделанного с другим --limit).
        try:
            existing_rows = parquet_row_count(output_path)
        except Exception:  # noqa: BLE001 - битый/недописанный файл перезапишем
            existing_rows = -1
        if existing_rows == expected_rows:
            result.skipped = True
            result.rows = existing_rows
            result.size = output_path.stat().st_size
            result.seconds = 0.0
            return result
        LOG.warning(
            "  %s: файл %s уже есть, но строк в нём %s вместо %s — перезаписываю",
            key_value, output_path.name, existing_rows, expected_rows,
        )

    csv_path = temp_dir / f"{output_path.stem}_{uuid.uuid4().hex[:8]}.csv"
    tmp_parquet = output_path.with_name(output_path.name + ".tmp")

    stop_monitor = threading.Event()

    def monitor_progress() -> None:
        while not stop_monitor.wait(20):
            try:
                transferred = csv_path.stat().st_size
            except OSError:
                transferred = 0
            LOG.info("  ... %s: получено %s, %.0f c", key_value, human_size(transferred),
                     time.time() - started)

    monitor = threading.Thread(target=monitor_progress, daemon=True)
    monitor.start()

    try:
        for attempt in range(1, retries + 2):
            if _CANCELLED.is_set():
                result.error = "отменено пользователем"
                result.seconds = time.time() - started
                return result
            try:
                copy_key_to_csv(
                    conn=get_thread_conn(cfg),
                    schema=schema,
                    table=table,
                    key=key,
                    key_value=key_value,
                    columns=[name for name, _ in columns],
                    where=where,
                    order_by=order_by,
                    limit=limit,
                    csv_path=csv_path,
                )
                if tmp_parquet.exists():
                    tmp_parquet.unlink()
                rows = csv_to_parquet(
                    csv_path=csv_path,
                    parquet_path=tmp_parquet,
                    column_types=columns,
                    compression=compression,
                    compression_level=compression_level,
                    row_group_size=row_group_size,
                    threads=threads,
                )
                os.replace(tmp_parquet, output_path)
                result.rows = rows
                result.size = output_path.stat().st_size
                result.seconds = time.time() - started
                return result
            except Exception as exc:  # noqa: BLE001 - нужен ретрай на сетевых сбоях
                drop_thread_conn()  # следующая попытка — с новым соединением
                if tmp_parquet.exists():
                    tmp_parquet.unlink()
                if attempt <= retries and not _CANCELLED.is_set():
                    LOG.warning(
                        "  %s: попытка %d/%d не удалась (%s), повтор...",
                        key_value, attempt, retries + 1, exc,
                    )
                    time.sleep(min(2 * attempt, 10))
                    continue
                result.error = f"{type(exc).__name__}: {exc}"
                result.seconds = time.time() - started
                return result
    finally:
        stop_monitor.set()
        if csv_path.exists():
            if keep_temp:
                LOG.info("  временный CSV сохранён: %s", csv_path)
            else:
                try:
                    csv_path.unlink()
                except OSError:
                    pass

    return result


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="gp_to_parquet.py",
        description=(
            "Выгрузка таблицы Greenplum в parquet-файлы: по одному файлу "
            "на каждое значение ключевого поля."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Примеры:\n"
            "  %(prog)s exp_schema tbl rrr_id all\n"
            "  %(prog)s exp_schema tbl rrr_id 1000 --workers 4\n"
            "  %(prog)s exp_schema tbl rrr_id all --dry-run\n"
        ),
    )
    parser.add_argument("schema", help="имя схемы, например exp_schema")
    parser.add_argument("table", help="имя таблицы, например tbl")
    parser.add_argument("key", help="ключевое поле, по которому режем на файлы")
    parser.add_argument(
        "limit",
        nargs="?",
        default="all",
        help="лимит строк на один файл: целое число или 'all' (по умолчанию all)",
    )

    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE,
                        help=f"файл с параметрами подключения (по умолчанию {DEFAULT_ENV_FILE})")
    parser.add_argument("--output-dir", type=Path, default=SCRIPT_DIR,
                        help="каталог для parquet (по умолчанию каталог скрипта)")
    parser.add_argument("--temp-dir", type=Path, default=None,
                        help="каталог для временных CSV (по умолчанию /tmp/.private/<user>/opencode)")
    parser.add_argument("--where", default=None,
                        help="дополнительное условие SQL (AND), например \"deleted_ind = 0\"")
    parser.add_argument("--order-by", default=None,
                        help="ORDER BY внутри файла, например \"ctrr_key\"")
    parser.add_argument("--columns", default=None,
                        help="список колонок через запятую (по умолчанию все)")
    parser.add_argument("--workers", type=int, default=0,
                        help=f"число параллельных потоков (0 = авто, максимум {DEFAULT_WORKERS})")
    parser.add_argument("--compression", default=DEFAULT_COMPRESSION,
                        choices=["zstd", "snappy", "gzip", "brotli", "lz4", "uncompressed"],
                        help=f"сжатие parquet (по умолчанию {DEFAULT_COMPRESSION})")
    parser.add_argument("--compression-level", type=int, default=None,
                        help=f"уровень сжатия (по умолчанию {DEFAULT_COMPRESSION_LEVEL} для zstd)")
    parser.add_argument("--row-group-size", type=int, default=DEFAULT_ROW_GROUP_SIZE,
                        help=f"размер row group в строках (по умолчанию {DEFAULT_ROW_GROUP_SIZE})")
    parser.add_argument("--threads-per-worker", type=int, default=0,
                        help="потоков DuckDB на воркер (0 = авто)")
    parser.add_argument("--retries", type=int, default=1,
                        help="повторов при ошибке выгрузки файла (по умолчанию 1)")
    parser.add_argument("--max-files", type=int, default=DEFAULT_MAX_FILES,
                        help=f"предохранитель от слишком большого числа файлов (по умолчанию {DEFAULT_MAX_FILES})")
    parser.add_argument("--overwrite", action="store_true",
                        help="перезаписывать существующие parquet-файлы")
    parser.add_argument("--keep-temp", action="store_true",
                        help="не удалять временные CSV")
    parser.add_argument("--dry-run", action="store_true",
                        help="показать план и выйти, ничего не выгружая")
    parser.add_argument("--resume-file", type=Path, default=DEFAULT_RESUME_FILE,
                        help=f"файл-отчёт о выгрузке (по умолчанию {DEFAULT_RESUME_FILE})")
    parser.add_argument("--quiet", action="store_true", help="только итог")
    return parser.parse_args(argv)


def parse_limit(raw: str) -> Optional[int]:
    text = str(raw).strip().lower()
    if text == "all":
        return None
    try:
        value = int(text)
    except ValueError:
        raise SystemExit(f"Неверный лимит: {raw!r} (ожидается целое число или 'all')")
    if value <= 0:
        raise SystemExit("Лимит должен быть положительным числом или 'all'")
    return value


def fmt_int(value: int) -> str:
    return f"{int(value):,}".replace(",", " ")


def human_size(size: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def report(message: str, *fmt_args: Any) -> None:
    """Итоговые сообщения печатаются даже в --quiet."""
    print(message % fmt_args if fmt_args else message, file=sys.stderr)


class ResumeLog:
    """Отчёт parq_resume.txt: когда, что и сколько выгрузили (или ошибку).

    Файл дописывается блоком на каждый запуск, поэтому в нём остаётся
    история всех выгрузок. Пишется всегда — и при успехе, и при ошибке.
    """

    def __init__(self, path: Path, args: argparse.Namespace, started_at: datetime):
        self.path = path
        self.args = args
        self.started_at = started_at
        self.finished_at: Optional[datetime] = None
        self.limit: Optional[int] = None
        self.results: Optional[List[TaskResult]] = None
        self.status: Optional[str] = None
        self.fatal_error: Optional[str] = None
        self.elapsed: float = 0.0
        self.enabled = True

    def __enter__(self) -> "ResumeLog":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.finished_at = datetime.now()
        if exc is not None and self.fatal_error is None:
            if isinstance(exc, SystemExit):
                code = exc.code
                if code not in (0, None):
                    self.fatal_error = str(code)
            elif isinstance(exc, KeyboardInterrupt):
                self.status = "ОТМЕНЕНО"
                self.fatal_error = "прервано пользователем (Ctrl+C)"
            else:
                self.fatal_error = f"{exc_type.__name__}: {exc}"
        try:
            self.write()
        except Exception as err:  # noqa: BLE001 - отчёт не должен ронять выгрузку
            LOG.warning("Не удалось записать %s: %s", self.path, err)
        return False

    def write(self) -> None:
        if not self.enabled:
            return
        block = "\n".join(self._format()) + "\n"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        prefix = ""
        if self.path.exists() and self.path.stat().st_size > 0:
            prefix = "\n"
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(prefix + block)

    def _format(self) -> List[str]:
        args = self.args
        results = self.results or []
        failed = [r for r in results if not r.ok]
        skipped = [r for r in results if r.ok and r.skipped]
        done = [r for r in results if r.ok and not r.skipped]

        if self.status:
            status = self.status
        elif self.fatal_error:
            status = "ОШИБКА"
        elif failed:
            status = "ОШИБКА (частично)"
        elif self.results is None:
            status = "ОШИБКА"
        else:
            status = "УСПЕШНО"

        lines = [
            "=" * 80,
            f"Запуск:       {self.started_at:%Y-%m-%d %H:%M:%S}",
        ]
        if self.finished_at is not None:
            lines.append(f"Завершение:   {self.finished_at:%Y-%m-%d %H:%M:%S}")
        lines += [
            f"Команда:      {' '.join(sys.argv)}",
            f"Таблица:      {args.schema}.{args.table}",
            f"Ключ:         {args.key}",
            f"Лимит:        {'all' if self.limit is None else self.limit} (на файл)",
            f"Каталог:      {args.output_dir.expanduser()}",
            f"Статус:       {status}",
        ]

        if results:
            lines.append(
                f"Файлов:       {len(done)} (пропущено: {len(skipped)}, ошибок: {len(failed)})"
            )
            lines.append(f"Строк:        {fmt_int(sum(r.rows for r in done))}")
            lines.append(f"Объём:        {human_size(sum(r.size for r in done))}")
            if self.elapsed:
                lines.append(f"Длительность: {self.elapsed:.1f} c")
            lines.append("")
            lines.append("Файлы:")
            for result in results:
                if not result.ok:
                    lines.append(f"  {result.parquet_path.name}: ОШИБКА — {result.error}")
                elif result.skipped:
                    lines.append(
                        f"  {result.parquet_path.name}: пропущен (уже был, "
                        f"{fmt_int(result.rows)} строк)"
                    )
                else:
                    lines.append(
                        f"  {result.parquet_path.name}: {fmt_int(result.rows)} строк, "
                        f"{human_size(result.size)}, {result.seconds:.1f} c"
                    )
        elif self.status == "НЕТ ДАННЫХ":
            lines.append("Строк:        0")

        if self.fatal_error:
            lines.append("")
            lines.append(f"Ошибка:       {self.fatal_error}")
        if failed:
            lines.append("")
            lines.append("Ошибки по файлам:")
            for result in failed:
                lines.append(f"  {result.key_value}: {result.error}")
        return lines


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    _CANCELLED.clear()
    resume = ResumeLog(args.resume_file, args, datetime.now())
    with resume:
        return run_export(args, resume)


def run_export(args: argparse.Namespace, resume: ResumeLog) -> int:
    limit = parse_limit(args.limit)
    resume.limit = limit
    cfg = build_config(args.env_file)
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = (args.temp_dir.expanduser().resolve() if args.temp_dir else default_temp_dir())
    temp_dir.mkdir(parents=True, exist_ok=True)

    if args.compression_level is None and args.compression in ("zstd", "gzip", "brotli"):
        args.compression_level = DEFAULT_COMPRESSION_LEVEL
    if args.compression not in ("zstd", "gzip", "brotli"):
        if args.compression_level is not None:
            LOG.warning("Для сжатия %s уровень не задаётся — параметр игнорируется",
                        args.compression)
        args.compression_level = None

    LOG.info("БД: %s", cfg.describe())
    LOG.info("Таблица: %s.%s, ключ: %s, лимит на файл: %s",
             args.schema, args.table, args.key, "all" if limit is None else limit)

    started = time.time()
    conn = connect(cfg)
    try:
        all_columns = fetch_columns(conn, args.schema, args.table)

        if args.columns:
            wanted = [c.strip() for c in args.columns.split(",") if c.strip()]
            by_name = {name.lower(): (name, duck_type) for name, duck_type in all_columns}
            selected: List[Tuple[str, str]] = []
            for name in wanted:
                item = by_name.get(name.lower())
                if item is None:
                    raise SystemExit(
                        f"Колонка {name!r} не найдена. Доступны: "
                        + ", ".join(n for n, _ in all_columns)
                    )
                selected.append(item)
            columns = selected
        else:
            columns = all_columns

        table_columns = {name.lower() for name, _ in all_columns}
        if args.key.lower() not in table_columns:
            raise SystemExit(
                f"Ключевое поле {args.key!r} отсутствует в таблице. Доступны: "
                + ", ".join(name for name, _ in all_columns)
            )

        LOG.info("Колонок: %d", len(columns))
        groups = fetch_key_groups(conn, args.schema, args.table, args.key, args.where)
        headroom = connection_headroom(conn)
    finally:
        close_conn(conn)

    # Параллельные воркеры открывают отдельные соединения; не упираемся
    # в rolconnlimit роли.
    if args.workers > 0:
        workers = args.workers
        if headroom is not None and workers > headroom:
            LOG.warning(
                "Запрошено воркеров: %d, но у роли свободно ~%d соединений — "
                "возможны ошибки 'too many connections'",
                workers, headroom,
            )
    else:
        workers = DEFAULT_WORKERS
        if headroom is not None:
            workers = min(workers, max(1, headroom))
    workers = max(1, workers)
    LOG.info("Воркеров: %d", workers)

    threads_per_worker = args.threads_per_worker
    if threads_per_worker <= 0:
        threads_per_worker = max(1, (os.cpu_count() or 4) // workers)

    if not groups:
        LOG.warning("По заданным условиям не найдено ни одной строки — делать нечего")
        resume.status = "НЕТ ДАННЫХ"
        return 0

    if args.max_files > 0 and len(groups) > args.max_files:
        raise SystemExit(
            f"Ключ {args.key} даёт {len(groups)} файлов (предохранитель --max-files={args.max_files}). "
            "Уточните --where или увеличьте --max-files (0 — без ограничения)."
        )

    key_values = [value for value, _ in groups]
    output_names = build_output_names(args.schema, args.table, key_values)
    tasks: List[Tuple[Any, int, Path]] = []
    for (value, count), name in zip(groups, output_names):
        expected = count if limit is None else min(count, limit)
        tasks.append((value, expected, output_dir / name))

    total_rows = sum(expected for _, expected, _ in tasks)
    LOG.info("Значений ключа: %d, строк к выгрузке: %s",
             len(tasks), f"{total_rows:,}".replace(",", " "))
    if limit is not None:
        LOG.info("Напоминание: --limit задаёт максимум строк НА КАЖДЫЙ файл")

    if args.dry_run:
        for value, expected, path in tasks:
            report("  %-40s %10s строк -> %s",
                   str(value), f"{expected:,}".replace(",", " "), path.name)
        report("Dry-run: файлы не создавались")
        resume.enabled = False
        return 0

    lock = threading.Lock()
    done = 0

    def run(task: Tuple[Any, int, Path]) -> TaskResult:
        nonlocal done
        value, expected, path = task
        if not (path.exists() and not args.overwrite):
            LOG.info("  старт: %s -> %s (ожидается %s строк)",
                     value, path.name, f"{expected:,}".replace(",", " "))
        result = export_group(
            cfg=cfg,
            schema=args.schema,
            table=args.table,
            key=args.key,
            key_value=value,
            columns=columns,
            where=args.where,
            order_by=args.order_by,
            limit=limit,
            expected_rows=expected,
            output_path=path,
            temp_dir=temp_dir,
            compression=args.compression,
            compression_level=args.compression_level,
            row_group_size=args.row_group_size,
            threads=threads_per_worker,
            keep_temp=args.keep_temp,
            overwrite=args.overwrite,
            retries=args.retries,
        )
        with lock:
            done += 1
            index = done
        if result.ok:
            if result.skipped:
                LOG.info("[%d/%d] %s -> %s: уже существует, пропущен",
                         index, len(tasks), value, path.name)
            else:
                LOG.info("[%d/%d] %s -> %s: %s строк, %s, %.1f c",
                         index, len(tasks), value, path.name,
                         f"{result.rows:,}".replace(",", " "),
                         human_size(result.size), result.seconds)
                if expected and result.rows != expected:
                    LOG.warning(
                        "  %s: ожидалось %d строк, в parquet %d — таблица менялась во время выгрузки?",
                        value, expected, result.rows,
                    )
        else:
            LOG.error("[%d/%d] %s -> %s: ОШИБКА %s",
                      index, len(tasks), value, path.name, result.error)
        return result

    results: List[TaskResult] = []
    try:
        if workers == 1 or len(tasks) == 1:
            results = [run(task) for task in tasks]
        else:
            with cf.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="gp") as pool:
                futures = [pool.submit(run, task) for task in tasks]
                for future in cf.as_completed(futures):
                    results.append(future.result())
    except KeyboardInterrupt:
        cancel_all()
        LOG.error("Прервано пользователем — отменяю активные COPY")
        close_all_conns()
        resume.status = "ОТМЕНЕНО"
        resume.fatal_error = "прервано пользователем (Ctrl+C)"
        resume.results = results
        resume.elapsed = time.time() - started
        return 130

    close_all_conns()

    elapsed = time.time() - started
    ok_results = [r for r in results if r.ok and not r.skipped]
    failed = [r for r in results if not r.ok]
    skipped = [r for r in results if r.ok and r.skipped]
    resume.results = results
    resume.elapsed = elapsed

    report("-" * 72)
    report("Готово за %.1f c: файлов %d, строк %s, объём %s",
           elapsed, len(ok_results),
           f"{sum(r.rows for r in ok_results):,}".replace(",", " "),
           human_size(sum(r.size for r in ok_results)))
    if skipped:
        report("Пропущено (уже есть): %d", len(skipped))
    if failed:
        LOG.error("Ошибок: %d", len(failed))
        for result in failed:
            LOG.error("  %s: %s", result.key_value, result.error)
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        LOG.error("Прервано пользователем")
        sys.exit(130)
