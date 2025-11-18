# app/models/manager.py
from __future__ import annotations

from typing import Iterable

from ..db import db

RANDOM_SUTTA_VERSE_SQL = """
    SELECT
        s.identifier,
        s.nikaya,
        COALESCE(s.vagga, '') AS vagga,
        ordinality - 1 AS verse_num,
        verse_elem->>'text' AS verse_text
    FROM ati_suttas AS s
    CROSS JOIN LATERAL jsonb_array_elements(s.verses) WITH ORDINALITY AS t(verse_elem, ordinality)
    WHERE s.doc_type = 'sutta'
    ORDER BY random()
    LIMIT 1;
"""

class _BoundManager:
    def __init__(
        self,
        model,
        dsn: str,
        *,
        table: str,
        columns: tuple[str, ...],
        id_column: str,
        row_processor,
        save_handler=None,
    ):
        self.model = model
        self.dsn = dsn
        self.table = table
        self.columns = columns
        self.id_column = id_column
        self.row_processor = row_processor
        self._save_handler = save_handler

    def using(self, dsn: str) -> "_BoundManager":
        return _BoundManager(
            self.model,
            dsn,
            table=self.table,
            columns=self.columns,
            id_column=self.id_column,
            row_processor=self.row_processor,
            save_handler=self._save_handler,
        )

    def _select_sql(self) -> str:
        column_sql = ", ".join(self.columns)
        return f"SELECT {column_sql} FROM {self.table}"

    def sample(self, n: int = 5):
        sql = self._select_sql() + " ORDER BY gen_random_uuid() LIMIT %(n)s"
        rows = db.fetch_all(sql, {"n": n}, dsn=self.dsn)
        return [self.model(**self.row_processor(row)) for row in rows]

    def random_sutta_verse(self):
        return db.fetch_one(RANDOM_SUTTA_VERSE_SQL, dsn=self.dsn)

    def get(self, id_value):
        sql = self._select_sql() + f" WHERE {self.id_column} = %(id)s"
        row = db.fetch_one(sql, {"id": id_value}, dsn=self.dsn)
        return self.model(**self.row_processor(row)) if row else None

    def save(self, obj, **kwargs):
        if not self._save_handler:
            raise NotImplementedError("Save operation not configured for this manager")
        return self._save_handler(self, obj, **kwargs)

    def connect(self):
        return db.connect(self.dsn)

    @property
    def dsn_value(self) -> str:
        return self.dsn


class Manager:
    """Descriptor that binds a manager to the model class (not instances)."""

    def __init__(
        self,
        *,
        table: str,
        columns: Iterable[str],
        id_column: str = "id",
        row_processor=None,
        save_handler=None,
        dsn: str | None = None,
    ):
        self._configured_dsn = dsn
        self._table = table
        self._columns = tuple(columns)
        self._id_column = id_column
        self._row_processor = row_processor
        self._save_handler = save_handler

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
        return self._configured_dsn or db.default_dsn()

    def __get__(self, instance, owner):
        # called as CandidateDoc.objects (instance is None, owner is the class)
        return _BoundManager(
            owner,
            self._resolve_dsn(),
            table=self._table,
            columns=self._columns,
            id_column=self._id_column,
            row_processor=self.row_processor(),
            save_handler=self._save_handler,
        )
