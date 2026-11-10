#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gp_from_parquet.py — загрузка parquet-файлов в Greenplum.

Скрипт читает один parquet-файл или все файлы по маске, при необходимости
фильтрует и сортирует данные, а затем грузит их в таблицу Greenplum
через COPY ... FROM STDIN порциями (каждая порция фиксируется COMMIT-ом).

Примеры
-------
Загрузить один файл:

    ./gp_from_parquet.py exp_schema_tbl_129.parquet exp_schema tbl

Все файлы по маске, сортировка по rrrr_key, батч 50 000 строк:

    ./gp_from_parquet.py "exp_schema_tbl_*.parquet" exp_schema tbl 50000 \
        --order-by rrrr_key

С фильтром:

    ./gp_from_parquet.py "exp_*.parquet" exp_schema tbl 10000 \
        --where "rrrr_name='7777777777'"

Посмотреть план, ничего не загружая:

    ./gp_from_parquet.py "exp_*.parquet" exp_schema tbl --dry-run

Как это работает
----------------
1. DuckDB читает parquet (glob-маска раскрывается скриптом), применяет WHERE
   и сортировку и один раз материализует результат во временную таблицу
   с номером строки __rn. Это позволяет дальше резать данные на порции
   без повторной сортировки и без медленного OFFSET.
2. Каждая порция выгружается DuckDB в CSV (нативный быстрый writer)
   и сразу заливается в Greenplum через COPY ... FROM STDIN WITH CSV —
   это самый быстрый штатный способ вставки.
3. После каждой порции выполняется COMMIT (размер порции = --batch-size,
   по умолчанию 10 000 строк).
4. По окончании работы в каталоге скрипта дописывается отчёт
   parq_exp_resume.txt: время выполнения и загрузки, таблица, сколько строк
   загружено, условия сортировки/фильтрации — или текст ошибки.

Параметры подключения берутся из .env.secret_db рядом со скриптом
(GP_IN_HOST, GP_IN_PORT, GP_IN_DB, GP_IN_USER, GP_IN_PASSWORD,
опционально GP_IN_SSLMODE, GP_IN_GSSENCMODE).

Зависимости: psycopg2-binary, duckdb (pyarrow не требуется).
"""

from __future__ import annotations

import argparse
import getpass
import logging
import os
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import duckdb
import psycopg2

LOG = logging.getLogger("gp_from_parquet")

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_ENV_FILE = SCRIPT_DIR / ".env.secret_db"
DEFAULT_RESUME_FILE = SCRIPT_DIR / "parq_exp_resume.txt"
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_DUCKDB_THREADS = max(1, (os.cpu_count() or 4))


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

    def connect_kwargs(self, application_name: str = "gp_from_parquet") -> Dict[str, Any]:
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
        host=get("GP_IN_HOST", required=True),
        port=int(get("GP_IN_PORT", "5432")),
        dbname=get("GP_IN_DB", required=True),
        user=get("GP_IN_USER", required=True),
        password=get("GP_IN_PASSWORD", required=True),
        sslmode=get("GP_IN_SSLMODE", "prefer"),
        gssencmode=get("GP_IN_GSSENCMODE", "disable"),
        connect_timeout=int(get("GP_IN_CONNECT_TIMEOUT", "30")),
    )


def connect(cfg: GpConfig, application_name: str = "gp_from_parquet"):
    conn = psycopg2.connect(**cfg.connect_kwargs(application_name))
    conn.autocommit = False
    return conn


# --------------------------------------------------------------------------- #
# SQL-хелперы
# --------------------------------------------------------------------------- #

def qi(identifier: str) -> str:
    """Экранирует идентификатор двойными кавычками (PG и DuckDB)."""
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


# --------------------------------------------------------------------------- #
# Файлы-источники
# --------------------------------------------------------------------------- #

def resolve_sources(raw: str) -> List[Path]:
    """Раскрывает имя файла или маску. Относительные пути — от каталога скрипта."""
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = SCRIPT_DIR / path

    if any(ch in raw for ch in "*?["):
        files = sorted(Path(path.parent).glob(path.name))
    else:
        files = [path] if path.is_file() else []

    files = [f for f in files if f.is_file()]
    if not files:
        raise SystemExit(f"Не найдено ни одного parquet-файла по шаблону: {raw}")
    return files


# --------------------------------------------------------------------------- #
# DuckDB: чтение parquet, фильтр, сортировка, нарезка на батчи
# --------------------------------------------------------------------------- #

def duckdb_connect(temp_dir: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET threads = {int(DEFAULT_DUCKDB_THREADS)}")
    con.execute(f"SET temp_directory = {sql_str(str(temp_dir))}")
    con.execute("SET preserve_insertion_order = true")
    return con


def parquet_columns(con, files: Sequence[Path]) -> List[Tuple[str, str]]:
    """[(имя колонки, тип DuckDB)] по первому файлу набора."""
    file_list = ", ".join(sql_str(str(f)) for f in files)
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet([{file_list}])"
    ).fetchall()
    if not rows:
        raise SystemExit("Не удалось прочитать схему parquet-файлов")
    return [(row[0], row[1]) for row in rows]


def materialize(
    con,
    files: Sequence[Path],
    where: Optional[str],
    order_by: Optional[str],
    limit: Optional[int],
) -> int:
    """Создаёт временную таблицу _load с колонкой __rn. Возвращает число строк."""
    file_list = ", ".join(sql_str(str(f)) for f in files)
    order_sql = f"ORDER BY {order_by}" if order_by else ""
    where_sql = f"WHERE {where}" if where else ""
    limit_sql = f"LIMIT {int(limit)}" if limit is not None else ""

    con.execute("DROP TABLE IF EXISTS _load")
    con.execute(
        f"""
        CREATE TEMP TABLE _load AS
        SELECT row_number() OVER ({order_sql}) AS __gp_load_rn, *
        FROM read_parquet([{file_list}])
        {where_sql}
        {limit_sql}
        """
    )
    return int(con.execute("SELECT count(*) FROM _load").fetchone()[0])


def export_batch(con, columns: Sequence[str], last_rn: int, batch_size: int, csv_path: Path) -> None:
    """Пишет очередную порцию в CSV (нативный writer DuckDB)."""
    select_list = ", ".join(qi(name) for name in columns)
    if csv_path.exists():
        csv_path.unlink()
    con.execute(
        f"""
        COPY (
            SELECT {select_list}
            FROM _load
            WHERE __gp_load_rn > {int(last_rn)}
            ORDER BY __gp_load_rn
            LIMIT {int(batch_size)}
        ) TO {sql_str(str(csv_path))}
        (FORMAT csv, HEADER false, DELIMITER ',', QUOTE '"', ESCAPE '"', NULL '\\N')
        """
    )


# --------------------------------------------------------------------------- #
# Greenplum: проверка таблицы и COPY
# --------------------------------------------------------------------------- #

def fetch_target_columns(conn, schema: str, table: str) -> List[str]:
    sql = """
        SELECT a.attname
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
        rows = [row[0] for row in cur.fetchall()]
    if not rows:
        raise SystemExit(f"Таблица {schema}.{table} не найдена или недоступна")
    return rows


def map_columns(
    parquet_cols: Sequence[str], target_cols: Sequence[str], schema: str, table: str
) -> List[Tuple[str, str]]:
    """[(имя в parquet, имя в таблице)] — сопоставление без учёта регистра."""
    by_lower = {name.lower(): name for name in target_cols}
    result: List[Tuple[str, str]] = []
    missing: List[str] = []
    for name in parquet_cols:
        actual = by_lower.get(name.lower())
        if actual is None:
            missing.append(name)
        else:
            result.append((name, actual))
    if missing:
        raise SystemExit(
            f"В таблице {schema}.{table} нет колонок: {', '.join(missing)}. "
            f"Доступны: {', '.join(target_cols)}"
        )
    return result


def copy_batch(conn, schema: str, table: str, columns: Sequence[str], csv_path: Path) -> None:
    """COPY ... FROM STDIN WITH CSV + COMMIT."""
    column_list = ", ".join(qi(name) for name in columns)
    copy_sql = (
        f"COPY {qi(schema)}.{qi(table)} ({column_list}) "
        f"FROM STDIN WITH CSV NULL AS '\\N'"
    )
    with open(csv_path, "rb") as fh:
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, fh)
    conn.commit()


# --------------------------------------------------------------------------- #
# Отчёт parq_exp_resume.txt
# --------------------------------------------------------------------------- #

@dataclass
class LoadStats:
    files: List[Path]
    total_rows: int = 0
    loaded_rows: int = 0
    batches_done: int = 0
    batches_total: int = 0
    load_seconds: float = 0.0
    prepare_seconds: float = 0.0


class ResumeLog:
    """Отчёт parq_exp_resume.txt: время, таблица, строки, сортировка/фильтр, ошибки."""

    def __init__(self, path: Path, args: argparse.Namespace, started_at: datetime):
        self.path = path
        self.args = args
        self.started_at = started_at
        self.finished_at: Optional[datetime] = None
        self.stats: Optional[LoadStats] = None
        self.total_seconds: float = 0.0
        self.status: Optional[str] = None
        self.error: Optional[str] = None
        self.enabled = True

    def __enter__(self) -> "ResumeLog":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.finished_at = datetime.now()
        if exc is not None and self.error is None:
            if isinstance(exc, SystemExit):
                code = exc.code
                if code not in (0, None):
                    self.error = str(code)
            elif isinstance(exc, KeyboardInterrupt):
                self.status = "ОТМЕНЕНО"
                self.error = "прервано пользователем (Ctrl+C)"
            else:
                self.error = f"{exc_type.__name__}: {exc}"
        try:
            self.write()
        except Exception as err:  # noqa: BLE001 - отчёт не должен ронять загрузку
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
        stats = self.stats

        if self.status:
            status = self.status
        elif self.error:
            status = "ОШИБКА"
        elif stats is None:
            status = "ОШИБКА"
        else:
            status = "УСПЕШНО"

        files = stats.files if stats else []
        lines = [
            "=" * 80,
            f"Запуск:           {self.started_at:%Y-%m-%d %H:%M:%S}",
        ]
        if self.finished_at is not None:
            lines.append(f"Завершение:       {self.finished_at:%Y-%m-%d %H:%M:%S}")
        lines += [
            f"Команда:          {' '.join(sys.argv)}",
            f"Источник:         {args.source}",
            f"Файлов:           {len(files)}",
            f"Таблица:          {args.schema}.{args.table}",
            f"Размер батча:     {fmt_int(args.batch_size)}",
            f"Сортировка:       {args.order_by or 'нет'}",
            f"Фильтр:           {args.where or 'нет'}",
            f"Статус:           {status}",
        ]

        if stats is not None:
            lines.append(f"Строк загружено:  {fmt_int(stats.loaded_rows)}")
            if stats.total_rows != stats.loaded_rows:
                lines.append(f"Строк в источнике: {fmt_int(stats.total_rows)}")
            lines.append(f"Батчей:           {stats.batches_done} из {stats.batches_total}")
            lines.append(f"Время выполнения: {self.total_seconds:.1f} c")
            lines.append(f"Время загрузки:   {stats.load_seconds:.1f} c")
            if stats.prepare_seconds:
                lines.append(f"Подготовка:       {stats.prepare_seconds:.1f} c")
            if files:
                lines.append("")
                lines.append("Файлы:")
                for path in files:
                    try:
                        size = human_size(path.stat().st_size)
                    except OSError:
                        size = "?"
                    lines.append(f"  {path.name} ({size})")

        if self.error:
            lines.append("")
            lines.append(f"Ошибка:           {self.error}")
        return lines


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

# Параметры можно задавать и позиционно, и в форме key=value:
#   ./gp_from_parquet.py source=exp_*.parquet schema=exp_schema table=tbl
_KEY_VALUE_SOURCE = ("source", "file", "filename")
_KEY_VALUE_POSITIONAL = ("schema", "table")
_KEY_VALUE_BATCH = ("batch_size", "batch-size", "batch")
_KEY_VALUE_OPTIONS = {
    "order_by": "--order-by",
    "order-by": "--order-by",
    "where": "--where",
    "limit": "--limit",
}


def normalize_argv(argv: Sequence[str]) -> List[str]:
    """Переводит аргументы вида source=..., schema=..., where=... в обычные.

    Позволяет запускать скрипт в форме
        ./gp_from_parquet.py source=exp_*.parquet schema=exp_schema table=t
    наравне с позиционной
        ./gp_from_parquet.py "exp_*.parquet" exp_schema t
    """
    positional: Dict[str, str] = {}
    options: List[str] = []
    rest: List[str] = []

    for arg in argv:
        key, sep, value = arg.partition("=")
        if not sep:
            rest.append(arg)
        elif key in _KEY_VALUE_SOURCE:
            positional["source"] = value
        elif key in _KEY_VALUE_POSITIONAL:
            positional[key] = value
        elif key in _KEY_VALUE_BATCH:
            positional["batch_size"] = value
        elif key in _KEY_VALUE_OPTIONS:
            options += [_KEY_VALUE_OPTIONS[key], value]
        else:
            rest.append(arg)

    ordered = [
        positional[name]
        for name in ("source", "schema", "table", "batch_size")
        if name in positional
    ]
    return ordered + options + rest


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="gp_from_parquet.py",
        description=(
            "Загрузка parquet-файлов в Greenplum через COPY FROM STDIN. "
            "Поддерживает маску файлов, фильтрацию и сортировку перед вставкой."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Примеры:\n"
            "  %(prog)s exp_schema_tbl_129.parquet exp_schema tbl\n"
            "  %(prog)s \"exp_schema_tbl_*.parquet\" exp_schema tbl 50000 \\\n"
            "      --order-by rrrr_key\n"
            "  %(prog)s \"exp_*.parquet\" exp_schema tbl 10000 \\\n"
            "      --where \"rrrr_name='7777777777'\"\n"
            "\n"
            "Параметры можно задавать в форме key=value:\n"
            "  %(prog)s source=\"exp_*.parquet\" schema=exp_schema table=tbl \\\n"
            "      batch_size=10000 where=\"rrrr_name='7777777777'\" order_by=rrrr_key\n"
        ),
    )
    parser.add_argument("source", help="имя parquet-файла или маска (например exp_*.parquet)")
    parser.add_argument("schema", help="схема таблицы назначения")
    parser.add_argument("table", help="таблица назначения")
    parser.add_argument(
        "batch_size",
        nargs="?",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"строк в одной порции (COMMIT после каждой), по умолчанию {DEFAULT_BATCH_SIZE}",
    )

    parser.add_argument("--order-by", default=None,
                        help="поле(я) сортировки перед вставкой, например \"rrrr_key\" "
                             "или \"src_id, rrrr_key\"; пусто — не сортировать")
    parser.add_argument("--where", default=None,
                        help="условие фильтрации (SQL DuckDB), например \"rrrr_name='745...'\"")
    parser.add_argument("--limit", type=int, default=None,
                        help="загрузить не более N строк (для проверки)")
    parser.add_argument("--truncate", action="store_true",
                        help="очистить таблицу назначения перед загрузкой")
    parser.add_argument("--dry-run", action="store_true",
                        help="показать план и выйти, ничего не загружая")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE,
                        help=f"файл с параметрами подключения (по умолчанию {DEFAULT_ENV_FILE})")
    parser.add_argument("--resume-file", type=Path, default=DEFAULT_RESUME_FILE,
                        help=f"файл-отчёт о загрузке (по умолчанию {DEFAULT_RESUME_FILE})")
    parser.add_argument("--temp-dir", type=Path, default=None,
                        help="каталог для временных CSV (по умолчанию /tmp/.private/<user>/opencode)")
    parser.add_argument("--synchronous-commit", choices=["on", "off"], default="off",
                        help="synchronous_commit на время загрузки (по умолчанию off — быстрее)")
    parser.add_argument("--keep-temp", action="store_true",
                        help="не удалять временный CSV")
    parser.add_argument("--quiet", action="store_true", help="только итог")
    if argv is None:
        argv = sys.argv[1:]
    return parser.parse_args(normalize_argv(argv))


def default_temp_dir() -> Path:
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


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )
    if args.batch_size <= 0:
        raise SystemExit("Размер батча должен быть положительным числом")

    resume = ResumeLog(args.resume_file, args, datetime.now())
    with resume:
        return run_load(args, resume)


def run_load(args: argparse.Namespace, resume: ResumeLog) -> int:
    started = time.time()
    cfg = build_config(args.env_file)
    temp_dir = (args.temp_dir.expanduser().resolve() if args.temp_dir else default_temp_dir())
    temp_dir.mkdir(parents=True, exist_ok=True)
    csv_path = temp_dir / f"gp_load_{os.getpid()}_{uuid.uuid4().hex[:8]}.csv"

    files = resolve_sources(args.source)
    stats = LoadStats(files=files)
    resume.stats = stats
    LOG.info("БД: %s", cfg.describe())
    LOG.info("Источник: %s (%d файл(ов))", args.source, len(files))
    LOG.info("Таблица: %s.%s, батч: %s строк",
             args.schema, args.table, fmt_int(args.batch_size))
    if args.order_by:
        LOG.info("Сортировка: %s", args.order_by)
    if args.where:
        LOG.info("Фильтр: %s", args.where)

    con = duckdb_connect(temp_dir)
    try:
        parquet_cols = parquet_columns(con, files)
        parquet_names = [name for name, _ in parquet_cols]
        LOG.info("Колонок в parquet: %d", len(parquet_names))

        conn = connect(cfg)
        try:
            target_cols = fetch_target_columns(conn, args.schema, args.table)
            load_columns = map_columns(parquet_names, target_cols, args.schema, args.table)
            parquet_load_cols = [name for name, _ in load_columns]
            target_load_cols = [name for _, name in load_columns]
            with conn.cursor() as cur:
                cur.execute(f"SET synchronous_commit = {args.synchronous_commit}")
                cur.execute("SET statement_timeout = 0")
            conn.commit()

            prepare_started = time.time()
            try:
                total_rows = materialize(
                    con, files, args.where, args.order_by, args.limit
                )
            except Exception as exc:  # noqa: BLE001
                message = str(exc)
                if "Conversion Error" in message or "Could not convert" in message:
                    message += (
                        "\nПодсказка: если колонка строковая, заключите значение "
                        "в кавычки, например --where \"rrrr_name='7777777777'\""
                    )
                raise SystemExit(f"Ошибка фильтрации/сортировки: {message}")
            prepare_seconds = time.time() - prepare_started

            batches_total = (total_rows + args.batch_size - 1) // args.batch_size
            stats.total_rows = total_rows
            stats.batches_total = batches_total
            stats.prepare_seconds = prepare_seconds
            LOG.info("Строк к загрузке: %s, батчей: %d (подготовка %.1f c)",
                     fmt_int(total_rows), batches_total, prepare_seconds)

            if args.dry_run:
                for index, path in enumerate(files, 1):
                    LOG.info("  %d. %s", index, path.name)
                report("Dry-run: данные не загружались")
                resume.enabled = False
                return 0

            if total_rows == 0:
                LOG.warning("После фильтрации не осталось строк — загружать нечего")
                resume.status = "НЕТ ДАННЫХ"
                resume.total_seconds = time.time() - started
                return 0

            if args.truncate:
                LOG.info("TRUNCATE TABLE %s.%s", args.schema, args.table)
                with conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {qi(args.schema)}.{qi(args.table)}")
                conn.commit()

            last_rn = 0
            try:
                for index in range(1, batches_total + 1):
                    batch_started = time.time()
                    export_batch(con, parquet_load_cols, last_rn, args.batch_size, csv_path)
                    copy_batch(conn, args.schema, args.table, target_load_cols, csv_path)
                    batch_seconds = time.time() - batch_started

                    last_rn = min(index * args.batch_size, total_rows)
                    stats.loaded_rows = last_rn
                    stats.batches_done = index
                    stats.load_seconds += batch_seconds

                    LOG.info(
                        "[%d/%d] %s / %s строк (%d%%), %.1f c",
                        index, batches_total, fmt_int(last_rn), fmt_int(total_rows),
                        int(last_rn * 100 / total_rows), batch_seconds,
                    )
            except KeyboardInterrupt:
                conn.rollback()
                resume.status = "ОТМЕНЕНО"
                resume.error = "прервано пользователем (Ctrl+C)"
                resume.total_seconds = time.time() - started
                LOG.error("Прервано пользователем — текущий батч откатан")
                return 130
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                resume.error = f"{type(exc).__name__}: {exc}"
                resume.total_seconds = time.time() - started
                LOG.error("Ошибка загрузки на батче %d: %s", stats.batches_done + 1, exc)
                return 1

            resume.total_seconds = time.time() - started
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
    finally:
        try:
            con.close()
        except Exception:  # noqa: BLE001
            pass
        if csv_path.exists():
            if args.keep_temp:
                LOG.info("Временный CSV сохранён: %s", csv_path)
            else:
                try:
                    csv_path.unlink()
                except OSError:
                    pass

    elapsed = time.time() - started
    resume.total_seconds = elapsed
    report("-" * 72)
    report("Готово за %.1f c: загружено %s строк в %s.%s (загрузка %.1f c)",
           elapsed, fmt_int(stats.loaded_rows), args.schema, args.table,
           stats.load_seconds)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        LOG.error("Прервано пользователем")
        sys.exit(130)
