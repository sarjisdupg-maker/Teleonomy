from __future__ import annotations

from typing import Any, Dict, Optional, List
import logging

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .test_harness import EnhancedCrudTestHarness
from .where_builder import sql_where


class EvoCSVPipeline:
    def __init__(
        self,
        engine: Optional[Engine] = None,
        connection_url: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        log_level: int = logging.INFO,
        log_handler: Optional[logging.Handler] = None,
    ):
        if engine is None and not connection_url:
            raise ValueError("Either engine or connection_url must be provided")
        self.engine = engine or create_engine(connection_url)
        self.logger = logger or logging.getLogger(__name__)
        self.configure_logging(level=log_level, handler=log_handler)

    def configure_logging(self, level: int = logging.INFO, handler: Optional[logging.Handler] = None) -> None:
        self.logger.setLevel(level)
        if handler:
            if handler not in self.logger.handlers:
                self.logger.addHandler(handler)
        elif not self.logger.handlers:
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(level)
            formatter = logging.Formatter(
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
            )
            stream_handler.setFormatter(formatter)
            self.logger.addHandler(stream_handler)

    def close(self) -> None:
        if self.engine:
            self.engine.dispose()

    def run_full_cycle(
        self,
        df: pd.DataFrame,
        pk_cols: List[str] | str,
        constraint_cols: List[str] | str,
        table_name: str,
        dialect: Optional[str] = None,
        where: Optional[List[Any]] = None,
        expression: Optional[str] = None,
    ) -> Dict[str, Any]:
        active_dialect = (dialect or self.engine.dialect.name).lower()
        self.logger.info("Starting full cycle test", extra={"table": table_name, "dialect": active_dialect})

        harness = EnhancedCrudTestHarness(
            df_src=df,
            pk_cols=pk_cols,
            constraint_cols=constraint_cols,
            table_name=table_name,
        )

        result = harness.run_full_cycle_test(self.engine, dialect=active_dialect)
        self.logger.info(
            "Full cycle validation complete",
            extra={"passed": result.validation.passed, "rows": result.validation.actual_rows},
        )

        where_sql = None
        where_params = None
        if where is not None:
            mapped_dialect = "postgres" if active_dialect == "postgresql" else active_dialect
            where_sql, where_params = sql_where(where, expression, dialect=mapped_dialect)
            self.logger.info("WHERE clause prepared", extra={"sql": where_sql})

        return {
            "dialect": active_dialect,
            "test_result": result,
            "where_sql": where_sql,
            "where_params": where_params,
        }
