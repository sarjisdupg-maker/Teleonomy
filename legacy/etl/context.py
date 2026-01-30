from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, Optional, Union

from sqlalchemy import create_engine

from ..schema_manager import SchemaManager


class ETLContext:
    def __init__(
        self,
        db_uri: str,
        schema_dir: Optional[Union[Path, str]],
        logging_enabled: bool,
        pk: Union[str, list],
        chunksize: int,
        add_missing_cols: bool,
        failure_threshold: int,
        rename_column: bool,
        schema_aware_casting: bool,
        cast_kwargs: Dict[str, Any],
    ):
        self.engine = create_engine(db_uri)
        self.dialect = self.engine.dialect.name
        if self.dialect == "postgresql":
            self.dialect = "postgresql"
        elif self.dialect == "oracle":
            self.dialect = "oracle"

        self.schema_dir = Path(schema_dir) if schema_dir else None
        self.logging_enabled = logging_enabled
        self.default_pk = pk
        self.default_chunksize = chunksize
        self.default_add_missing_cols = add_missing_cols
        self.default_failure_threshold = failure_threshold
        self.default_rename_column = rename_column
        self.schema_aware_casting = schema_aware_casting
        self.cast_kwargs = cast_kwargs
        if "infer_threshold" not in self.cast_kwargs:
            self.cast_kwargs["infer_threshold"] = self.default_failure_threshold

        self.sm = SchemaManager(self.engine)
