# app/models/manager.py
from __future__ import annotations

from functools import lru_cache
from importlib import import_module
from typing import Any, Dict, Iterable

from psycopg import connect
from psycopg.rows import dict_row


@lru_cache(maxsize=1)
def _default_dsn() -> str:
    """
    Build a psycopg-compatible connection string from local_settings.
    Falls back to an empty string (lib default search order) if settings
    cannot be imported.
    """
    settings: Dict[str, Any] | None = None
    for module_name in ("app.local_settings", "local_settings"):
        try:
            module = import_module(module_name)
        except ImportError:
            continue
        candidate = getattr(module, "settings", None)
        if isinstance(candidate, dict):
            settings = candidate
            break
    if not isinstance(settings, dict):
        settings = {}

    db = settings.get("DB", {})
    if not isinstance(db, dict):
        db = {}

    url = db.get("URL")
    if isinstance(url, str) and url:
        # SQLAlchemy-style URLs often include the driver suffix.
        if url.startswith("postgresql+"):
            return "postgresql://" + url.split("://", 1)[1]
        return url

    parts = []
    mapping = {
        "NAME": "dbname",
        "USER": "user",
        "PASSWORD": "password",
        "HOST": "host",
        "PORT": "port",
    }
    for key, conn_key in mapping.items():
        value = db.get(key)
        if value:
            parts.append(f"{conn_key}={value}")

    return " ".join(parts)


class _BoundManager:
    def __init__(self, model, dsn: str, *, table: str, columns: tuple[str, ...], id_column: str, row_processor):
        self.model = model
        self.dsn = dsn
        self.table = table
        self.columns = columns
        self.id_column = id_column
        self.row_processor = row_processor

    def using(self, dsn: str) -> "_BoundManager":
        return _BoundManager(
            self.model,
            dsn,
            table=self.table,
            columns=self.columns,
            id_column=self.id_column,
            row_processor=self.row_processor,
        )

    def _select_sql(self) -> str:
        column_sql = ", ".join(self.columns)
        return f"SELECT {column_sql} FROM {self.table}"

    def sample(self, n: int = 5):
        sql = self._select_sql() + " ORDER BY gen_random_uuid() LIMIT %(n)s"
        with connect(self.dsn, row_factory=dict_row) as cx, cx.cursor() as cur:
            cur.execute(sql, {"n": n})
            rows = cur.fetchall()
        return [self.model(**self.row_processor(row)) for row in rows]

    def get(self, id_value):
        sql = self._select_sql() + f" WHERE {self.id_column} = %(id)s"
        with connect(self.dsn, row_factory=dict_row) as cx, cx.cursor() as cur:
            cur.execute(sql, {"id": id_value})
            row = cur.fetchone()
        return self.model(**self.row_processor(row)) if row else None


class Manager:
    """Descriptor that binds a manager to the model class (not instances)."""

    def __init__(
        self,
        *,
        table: str,
        columns: Iterable[str],
        id_column: str = "id",
        row_processor=None,
        dsn: str | None = None,
    ):
        self._configured_dsn = dsn
        self._table = table
        self._columns = tuple(columns)
        self._id_column = id_column
        self._row_processor = row_processor

    def _default_row_processor(self):
        selected = self._columns

        def _processor(row):
            return {col: row.get(col) for col in selected}

        return _processor

    def row_processor(self):
        return self._row_processor or self._default_row_processor()

    def configure(self, dsn: str):
        self._configured_dsn = dsn
        return self

    def _resolve_dsn(self) -> str:
        return self._configured_dsn or _default_dsn()

    def __get__(self, instance, owner):
        # called as CandidateDoc.objects (instance is None, owner is the class)
        return _BoundManager(
            owner,
            self._resolve_dsn(),
            table=self._table,
            columns=self._columns,
            id_column=self._id_column,
            row_processor=self.row_processor(),
        )
