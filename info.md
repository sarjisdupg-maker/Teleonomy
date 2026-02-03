**Public API Inventory**

`crud/logger.py`
- `load_config() -> dict` — load logging settings from config or defaults; example: `cfg = load_config()`
- `log_call(func)` — decorate callable to log inputs/outputs when enabled; example: `@log_call\ndef fn(x): return x*2`
- `log_string(label: str, value: str) -> None` — append labeled string snapshot; example: `log_string("sql", "SELECT 1")`
- `log_json(label: str, obj) -> None` — append labeled JSON snapshot; example: `log_json("stats", {"rows": 5})`
- `log_dataframe(label: str, df: pd.DataFrame, max_rows=20) -> None` — log dataframe shape/head; example: `log_dataframe("preview", df)`
- `conditional_log_call(func)` — wrap with `log_call` only if logging enabled; example: `wrapped = conditional_log_call(fn)`

`crud/crud_build.py`
- `parse_sql_condition(cond: str) -> dict` — parse SQL condition string; example: `parse_sql_condition("id = 1")`
- `sql_where(conditions, expression: str | None = None, dialect: str = 'sqlite') -> tuple[str, dict]` — build WHERE SQL and params; example: `sql_where([("id","=",1)], dialect="postgres")`
- `build_update(data, table: str, where: list, dialect: str = 'sqlite', expression: str | None = None, update_cols: list | None = None, allow_missing_where_cols: bool = True) -> tuple[str, dict]` — build UPDATE with aligned columns; example: `build_update({"name":"x"},"t",[("id","=",1)])`
- `build_select(data, table: str, where: list, dialect: str = 'sqlite', expression: str | None = None, columns: list | None = None, allow_missing_where_cols: bool = False) -> tuple[str, dict]` — build SELECT with aligned columns; example: `build_select({"id":1},"t",[("id","=",1)])`
- `build_insert(data, table: str, dialect: str = 'sqlite') -> tuple[str, dict]` — build INSERT with parameters; example: `build_insert({"id":1,"name":"a"},"t")`

`crud/crud_harness.py`
- `CrudTestHarness(df_src: pd.DataFrame, pk_cols, constraint_cols, table_name: str = "t")` — initialize deterministic CRUD test harness; example: `h = CrudTestHarness(df, "id", "cid")`
- `CrudTestHarness.insert_df` — property returning mutated insert frame; example: `h.insert_df`
- `CrudTestHarness.upsert_df` — property returning mutated upsert frame; example: `h.upsert_df`
- `CrudTestHarness.update_df` — property returning mutated update frame; example: `h.update_df`
- `CrudTestHarness.get_insert_check_query() -> tuple[str, dict]` — SELECT to verify inserts; example: `sql, params = h.get_insert_check_query()`
- `CrudTestHarness.get_upsert_check_query() -> tuple[str, dict]` — SELECT to verify upserts; example: `sql, params = h.get_upsert_check_query()`
- `CrudTestHarness.get_update_check_query() -> tuple[str, dict]` — SELECT to verify updates; example: `sql, params = h.get_update_check_query()`
- `CrudTestHarness.assert_same_pk_set(db_df, expected_df) -> None` — assert PK sets equal; example: `h.assert_same_pk_set(db, expected)`
- `CrudTestHarness.assert_frames_close(actual, expected, rtol=1e-5, atol=1e-8, dt_tolerance=pd.Timedelta("5s"), msg: str = "") -> None` — compare frames with tolerances; example: `h.assert_frames_close(db, exp)`
- `CrudTestHarness.validate_after_insert(db_df) -> None` — validate state after insert; example: `h.validate_after_insert(db)`
- `CrudTestHarness.validate_after_upsert(db_df) -> None` — validate state after upsert; example: `h.validate_after_upsert(db)`
- `CrudTestHarness.validate_after_full_cycle(db_df) -> None` — validate after insert→upsert→update; example: `h.validate_after_full_cycle(db)`

`crud/crud_v2.py`
- `oracle_upsert(engine: Engine, data, table, constrain: list[str], chunk: int = 10000, tolerance: int = 5, trace_sql: bool = False) -> dict` — perform Oracle MERGE upsert with lazy fallback; example: `oracle_upsert(engine, rows, "users", ["id"])`
- `oracle_insert(engine: Engine, data, table, chunk_size: int = 10000, tolerance: int = 5, trace_sql: bool = False) -> int` — bulk insert into Oracle with fallback; example: `oracle_insert(engine, rows, "users")`
- `sqlite_upsert(engine: Engine, data, table, constrain: list[str], chunk: int = 10000, tolerance: int = 5, trace_sql: bool = False) -> dict` — SQLite upsert with ON CONFLICT; example: `sqlite_upsert(engine, rows, "users", ["id"])`
- `sqlite_insert(engine: Engine, data, table, chunk_size: int = 10000, tolerance: int = 5, trace_sql: bool = False) -> int` — SQLite bulk insert; example: `sqlite_insert(engine, rows, "users")`
- `mysql_upsert(engine: Engine, data, table, constrain: list[str], chunk: int = 10000, tolerance: int = 5, trace_sql: bool = False) -> dict` — MySQL ON DUPLICATE KEY upsert; example: `mysql_upsert(engine, rows, "users", ["id"])`
- `mysql_insert(engine: Engine, data, table, chunk_size: int = 10000, tolerance: int = 5, trace_sql: bool = False) -> int` — MySQL bulk insert; example: `mysql_insert(engine, rows, "users")`
- `postgres_upsert(engine: Engine, data, table, constrain: list[str], chunk: int = 10000, tolerance: int = 5, trace_sql: bool = False) -> dict` — PostgreSQL ON CONFLICT upsert; example: `postgres_upsert(engine, rows, "users", ["id"])`
- `postgres_insert(engine: Engine, data, table, chunk_size: int = 10000, tolerance: int = 5, trace_sql: bool = False) -> int` — PostgreSQL bulk insert; example: `postgres_insert(engine, rows, "users")`
- `auto_upsert(engine: Engine, data, table, constrain: list[str], chunk: int = 10000, tolerance: int = 5, add_missing_cols: bool = True, failure_threshold: float = 3, semantic_meta: dict | None = None, trace_sql: bool = False) -> dict` — auto-align data then dialect-specific upsert; example: `auto_upsert(engine, df, "users", ["id"])`
- `auto_insert(engine: Engine, data, table, chunk_size: int = 10000, tolerance: int = 5, add_missing_cols: bool = True, failure_threshold: float = 3, semantic_meta: dict | None = None, trace_sql: bool = False) -> int` — auto-align then bulk insert; example: `auto_insert(engine, df, "users")`
- `auto_update(engine: Engine, table, data, where: list, expression: str | None = None, add_missing_cols: bool = True, failure_threshold: float = 3, semantic_meta: dict | None = None, trace_sql: bool = False) -> int` — auto-align then targeted update; example: `auto_update(engine, "users", {"name":"n"}, [("id","=",1)])`
- `lazy_upsert_only(engine: Engine, data, table, constrain: list[str], trace_sql: bool = False) -> dict` — force row-by-row upsert; example: `lazy_upsert_only(engine, rows, "users", ["id"])`
- `mssql_upsert(engine: Engine, data, table, constrain: list[str], chunk: int = 10000, tolerance: int = 5, trace_sql: bool = False) -> dict` — MSSQL MERGE upsert; example: `mssql_upsert(engine, rows, "users", ["id"])`
- `mssql_insert(engine: Engine, data, table, chunk_size: int = 10000, tolerance: int = 5, trace_sql: bool = False) -> int` — MSSQL bulk insert; example: `mssql_insert(engine, rows, "users")`

`crud/evo_csv.py`
- `EvoCSVPipeline(engine: Engine | None = None, connection_url: str | None = None, logger: logging.Logger | None = None, log_level: int = logging.INFO, log_handler: logging.Handler | None = None)` — initialize CSV CRUD pipeline; example: `p = EvoCSVPipeline(connection_url="sqlite://")`
- `EvoCSVPipeline.configure_logging(level: int = logging.INFO, handler: logging.Handler | None = None) -> None` — configure pipeline logger; example: `p.configure_logging(log_level)`
- `EvoCSVPipeline.close() -> None` — dispose engine; example: `p.close()`
- `EvoCSVPipeline.run_full_cycle(df: pd.DataFrame, pk_cols, constraint_cols, table_name: str, dialect: str | None = None, where: list | None = None, expression: str | None = None) -> dict` — run full CRUD validation cycle; example: `p.run_full_cycle(df, "id", "uniq", "users")`

`crud/evo_pipeline.py`
- `OperationResult(name: str, status: str, details: dict = ..., error: str | None = None, duration_ms: float = 0.0)` — record one operation result; example: `OperationResult("insert","passed")`
- `PipelineReport(table: str, dialect: str, pk: list[str], operations: list[OperationResult] = ..., meta: dict = ...)` — aggregate pipeline report; example: `rep = PipelineReport("t","sqlite",["id"])`
- `PipelineReport.to_dict(self) -> dict` — serialize report; example: `rep.to_dict()`
- `PipelineReport.all_passed(self) -> bool` — check all operations passed; example: `rep.all_passed()`
- `PipelineReport.passed_operations(self) -> list[str]` — list passed operation names; example: `rep.passed_operations()`
- `PipelineConfig(csv_path: Path, table_name: str, output_dir: Path, pk: list[str] | None = None, dialect: str = "sqlite", chunksize: int = 10000)` — pipeline configuration; example: `cfg = PipelineConfig("data.csv","users","out")`
- `EvoCSVJobPipeline(config: PipelineConfig)` — pipeline orchestrator; example: `pipe = EvoCSVJobPipeline(cfg)`
- `EvoCSVJobPipeline.run(engine: Engine | None = None, db_uri: str | None = None) -> PipelineReport` — execute full pipeline; example: `report = pipe.run()`
- `EvoCSVJobPipeline.ddl_sql -> str | None` — property exposing generated CREATE TABLE SQL; example: `pipe.ddl_sql`
- `EvoCSVJobPipeline.processed_dataframe -> pd.DataFrame | None` — property exposing processed DataFrame; example: `pipe.processed_dataframe`

`preops/data_cleaner.py`
- `CleaningConfig(...)` — cleaning options dataclass; example: `cfg = CleaningConfig(remove_special_chars=True)`
- `clean_string(text: Any, config: CleaningConfig | None = None) -> Any` — clean single value; example: `clean_string(" Name", cfg)`
- `clean_series(s: pd.Series, config: CleaningConfig | None = None) -> pd.Series` — clean Series values; example: `clean_series(df["name"])`
- `clean_headers(df: pd.DataFrame, config: CleaningConfig | None = None) -> pd.DataFrame` — sanitize column names; example: `clean_headers(df)`
- `robust_clean(df: pd.DataFrame, config: CleaningConfig | None = None, columns: list[str] | None = None) -> pd.DataFrame` — clean headers and selected columns; example: `robust_clean(df)`
- `quick_clean(df: pd.DataFrame) -> pd.DataFrame` — apply standard cleaning defaults; example: `quick_clean(df)`

`preops/data_profiler.py`
- `profile_dataframe(df: pd.DataFrame) -> pd.DataFrame` — compute column profiling stats; example: `info = profile_dataframe(df)`
- `sample_dispatcher(df: pd.DataFrame, percent: int = 5, sort: bool = True, filter_none: bool = True, sort_key: str | None = None) -> tuple[pd.DataFrame, dict]` — sample head/tail rows with metadata; example: `sample, meta = sample_dispatcher(df, percent=10)`
- `get_pk(df: pd.DataFrame, dfinfo: pd.DataFrame, uniqueness_threshold: float = 0.8, max_composite_size: int = 5) -> tuple[pd.DataFrame, str, dict]` — derive primary key column/composite; example: `df_pk, pk_name, meta = get_pk(df, info)`

`preops/casting.py`
- `CastConfig(...)` — casting configuration dataclass; example: `cfg = CastConfig(infer_threshold=0.8)`
- `cast_df(obj, dtype: Mapping[str, str] | None = None, config: CastConfig | None = None, return_dtype_meta: bool = False, **kwargs) -> pd.DataFrame | tuple[pd.DataFrame, dict]` — convert input to DataFrame and cast types; example: `df_cast = cast_df(data)`
- `auto_cast(df: pd.DataFrame, **kwargs) -> pd.DataFrame` — auto casting with defaults; example: `auto_cast(df)`
- `fast_cast(df: pd.DataFrame, **kwargs) -> pd.DataFrame` — fast casting with minimal validation; example: `fast_cast(df)`
- `safe_cast(df: pd.DataFrame, **kwargs) -> pd.DataFrame` — conservative casting with stricter validation; example: `safe_cast(df)`

`preops/ddl_create.py`
- `sanitize_cols(obj, allow_space: bool = False, to_lower: bool = True, fallback: str = 'col_', dialect: str = 'postgresql')` — sanitize column names/structures; example: `sanitize_cols(df)`
- `escape_identifier(name: str, server: str = 'oracle') -> str` — quote identifier per dialect; example: `escape_identifier("Order", "postgres")`
- `get_sql_type(col_dtype, server: str, varchar_sizes: dict | None = None, col_name: str | None = None, semantic_type: str | None = None) -> str` — map pandas dtype to SQL type; example: `get_sql_type(df["a"].dtype, "mysql")`
- `build_create_table_statement(df: pd.DataFrame, table: str, schema_name: str | None, pk, fk, unique, autoincrement, server: str, varchar_sizes: dict | None = None, dtype_semantics: dict | None = None) -> str` — generate CREATE TABLE DDL; example: `ddl = build_create_table_statement(df,"t",None,"id",None,None,None,"sqlite")`
- `build_schema_json(df: pd.DataFrame, table: str, schema_name: str | None, pk, fk, unique, autoincrement, server: str, varchar_sizes: dict | None = None, dtype_semantics: dict | None = None, column_mapping: dict | None = None) -> dict` — assemble schema metadata; example: `meta = build_schema_json(df,"t",None,"id",None,None,None,"sqlite")`
- `df_ddl(input_df_or_csv, table: str, server: str = 'oracle', schema_name: str | None = None, pk: str | list | None = None, fk: list | None = None, unique: list | None = None, autoincrement: tuple[str,int] | None = None, _default_varchar: int = 255, varchar_sizes: dict | None = None, cast: bool = True, cast_kwargs: dict | None = None, sanitize: bool = False, rename_column: bool = False, return_dtype_meta: bool = False, **kwargs) -> tuple | tuple[...]` — generate DDL and metadata from DataFrame/CSV; example: `df2, ddl, meta = df_ddl(df, "users", server="postgresql")`
- `generate_sqlalchemy_model(df: pd.DataFrame, table_name: str, class_name: str | None = None, sanitize: bool = True, cast: bool = True, pk: str | list | None = None, autoincrement: str | None = None, cast_kwargs: dict | None = None) -> str` — emit declarative model code; example: `code = generate_sqlalchemy_model(df, "users")`
- `df_ddl_create(conn_dict: Mapping[str,str], df: pd.DataFrame, table: str, schema_name: str | None = None, pk: str | list | None = None, fk: list | None = None, unique: list | None = None, autoincrement: tuple[str,int] | None = None, varchar_sizes: dict | None = None, cast: bool = True, cast_kwargs: dict | None = None, sanitize: bool = False) -> dict` — create tables and load data across dialects; example: `results = df_ddl_create({"sqlite":"sqlite://"}, df, "users")`
- `csv_ddl_create(conn_dict: Mapping[str,str], csv_path: str, table: str, schema_name: str | None = None, pk: str | list | None = None, fk: list | None = None, unique: list | None = None, autoincrement: tuple[str,int] | None = None, varchar_sizes: dict | None = None, cast: bool = True, cast_kwargs: dict | None = None, csv_overwrite: bool = True) -> dict` — run DDL/create from CSV and optionally overwrite; example: `csv_ddl_create({"sqlite":"sqlite://"}, "data.csv", "users")`

`schema_introspect/schema_manager.py`
- `normalize_db_type(dialect_name: str) -> str` — normalize dialect name to canonical; example: `normalize_db_type("postgresql")`
- `get_logger(name: str) -> logging.Logger` — retrieve logger; example: `logger = get_logger("schema")`
- `SchemaManager(engine: Engine)` — introspection helper; example: `sm = SchemaManager(engine)`
- `SchemaManager.db_type -> str` — property returning normalized dialect; example: `sm.db_type`
- `SchemaManager.refresh(self) -> None` — refresh inspector cache; example: `sm.refresh()`
- `SchemaManager.list_tables(self, schema: str | None = None, pattern: str | None = None) -> list[str]` — list table names with optional regex; example: `sm.list_tables(pattern="^t")`
- `SchemaManager.get_columns(self, table: str, schema: str | None = None) -> list[dict]` — fetch column metadata; example: `sm.get_columns("users")`
- `SchemaManager.get_primary_keys(self, table: str, schema: str | None = None) -> list[str]` — get PK columns; example: `sm.get_primary_keys("users")`
- `SchemaManager.get_unique_constraints(self, table: str, schema: str | None = None) -> list[list[str]]` — list unique constraint columns; example: `sm.get_unique_constraints("users")`
- `SchemaManager.find_column(self, col_pattern: str, schema: str | None = None) -> dict[str, list[str]]` — find tables with matching columns; example: `sm.find_column("id")`
- `SchemaManager.get_table_details(self, table: str, schema: str | None = None) -> dict` — deep table inspection; example: `sm.get_table_details("users")`
- `SchemaManager.get_identity_columns(self, table: str, schema: str | None = None) -> set[str]` — detect identity/autoincrement columns; example: `sm.get_identity_columns("users")`
- `SchemaManager.validate_upsert_constraints(self, table: str, key_cols: list[str], schema: str | None = None) -> None` — verify upsert keys covered by PK/unique; example: `sm.validate_upsert_constraints("users", ["id"])`
- `SchemaManager.compare_to_structure(self, table: str, structure: dict[str,str], schema: str | None = None) -> dict` — compare expected vs actual column types; example: `sm.compare_to_structure("users", {"id":"INTEGER"})`
- `SchemaManager.has_table(self, table: str, schema: str | None = None) -> bool` — check table existence; example: `sm.has_table("users")`
- `SchemaManager.add_column(self, table: str, col_name: str, col_type: str, schema: str | None = None) -> None` — add column via ALTER; example: `sm.add_column("users","city","TEXT")`
- `SchemaManager.alter_column_type(self, table: str, col_name: str, new_type: str, schema: str | None = None) -> None` — alter column type dialect-aware; example: `sm.alter_column_type("users","name","VARCHAR(100)")`
- `SchemaManager.drop_table(self, table: str, schema: str | None = None) -> None` — drop table; example: `sm.drop_table("users")`
- `SchemaManager.drop_table_if_exists(self, table: str, schema: str | None = None) -> None` — drop table if present; example: `sm.drop_table_if_exists("users")`
- `SchemaManager.get_row_count(self, table: str, schema: str | None = None) -> int` — count rows; example: `sm.get_row_count("users")`
- `SchemaManager.execute_ddl(self, sql: str, msg: str) -> None` — execute DDL with logging; example: `sm.execute_ddl("CREATE TABLE t(id int)", "created")`

`schema_introspect/schema_corrector.py`
- `SchemaAligner(conn: Connection | Engine, db_type: str | None = None, on_error: str = 'coerce', failure_threshold: float = 3, validate_fk: bool = False, add_missing_cols: bool = False, col_map: dict | None = None)` — align DataFrame to table schema with strict coercion; example: `aligner = SchemaAligner(engine)`
- `SchemaAligner.db_type -> str` — property returning current dialect; example: `aligner.db_type`
- `SchemaAligner.align(self, df: pd.DataFrame, table: str, schema: str | None = None, conn: Connection | Engine | None = None, on_error: str | None = None, failure_threshold: float | None = None, validate_fk: bool | None = None, add_missing_cols: bool | None = None, col_map: dict | None = None, semantic_type_meta: dict | None = None) -> pd.DataFrame` — main alignment entry point; example: `aligned = aligner.align(df, "users")`

`schema_introspect/schema_forge.py`
- `LogicalType`, `OnError`, `LengthOverflow`, `OutlierStrategy` — enums for typing/correction; example: `LogicalType.INTEGER`
- `ColumnSpec(...)` — column metadata dataclass; example: `ColumnSpec("id","INTEGER",LogicalType.INTEGER,nullable=False)`
- `ForeignKeySpec(...)`, `ConstraintSpec(...)`, `TableSchema(...)`, `ColumnOverride(...)`, `CorrectionConfig(...)`, `ColumnReport(...)`, `MappingReport(...)`, `MigrationOp(...)`, `MigrationPlan(...)`, `CorrectionReport(...)` — schema and correction data holders; example: `cfg = CorrectionConfig(on_error=OnError.COERCE)`
- `CorrectionError(message: str)` — correction failure exception; example: `raise CorrectionError("bad data")`
- `SchemaIntrospectionError(message: str)` — introspection exception; example: `raise SchemaIntrospectionError("introspect failed")`
- `ConstraintViolationError(message: str)` — constraint violation exception; example: `raise ConstraintViolationError("fk missing")`
- `TypeRegistry()` — registry mapping SQL types to logical types; example: `reg = TypeRegistry()`
- `TypeRegistry.register_generic(self, sa_type: type, logical_type: LogicalType) -> None` — add generic mapping; example: `reg.register_generic(sa.String, LogicalType.STRING)`
- `TypeRegistry.register_for_dialect(self, dialect: str, sa_type: type, logical_type: LogicalType) -> None` — add dialect mapping; example: `reg.register_for_dialect("oracle", VARCHAR2, LogicalType.STRING)`
- `TypeRegistry.resolve(self, dialect: str, sa_type) -> LogicalType` — resolve logical type; example: `reg.resolve("postgresql", sa.Integer())`
- `normalize_identifier(name: str) -> str` — normalize string to lowercase identifier; example: `normalize_identifier("Customer ID")`
- `build_column_mapping(df_columns: list[str], db_columns: list[str], explicit_map: dict[str,str]) -> MappingReport` — map DataFrame to DB columns; example: `mapping = build_column_mapping(df_cols, db_cols, {})`
- `apply_column_mapping(df: pd.DataFrame, mapping: MappingReport) -> pd.DataFrame` — rename/trim columns per mapping; example: `df2 = apply_column_mapping(df, mapping)`
- `inspect_table_schema(conn, schema_name: str | None, table_name: str, registry: TypeRegistry) -> TableSchema` — introspect table schema; example: `schema = inspect_table_schema(engine, None, "users", reg)`
- `apply_semantic_tags(schema: TableSchema, semantic_tags: dict[str,str]) -> TableSchema` — attach semantic tags; example: `schema2 = apply_semantic_tags(schema, {"payload":"JSON"})`
- `make_policy(config: CorrectionConfig, spec: ColumnSpec) -> FailurePolicy` — build failure policy; example: `policy = make_policy(cfg, spec)`
- `apply_overrides(spec: ColumnSpec, config: CorrectionConfig) -> ColumnSpec` — apply column overrides; example: `spec2 = apply_overrides(spec, cfg)`
- `OutlierDetector.detect(self, series: pd.Series) -> pd.Series` — interface; example: `mask = NoOutlierDetector().detect(series)`
- `NoOutlierDetector.detect(self, series: pd.Series) -> pd.Series` — return all False mask; example: `mask = NoOutlierDetector().detect(series)`
- `IqrOutlierDetector(k: float = 3.0)` — IQR-based detector; example: `mask = IqrOutlierDetector().detect(series)`
- `CoercionResult(series: pd.Series, failures: pd.Series, action: str, bad_values_sample: list[str])` — result container; example: `res = CoercionResult(series, failures, "coerce_to_null", [])`
- `coerce_integer(series: pd.Series, spec: ColumnSpec, policy: FailurePolicy) -> CoercionResult` — cast integers with validation; example: `res = coerce_integer(df["a"], spec, policy)`
- `coerce_decimal(series: pd.Series, spec: ColumnSpec, policy: FailurePolicy) -> CoercionResult` — cast decimals; example: `res = coerce_decimal(df["a"], spec, policy)`
- `coerce_string(series: pd.Series, spec: ColumnSpec, policy: FailurePolicy) -> CoercionResult` — cast strings with length enforcement; example: `res = coerce_string(df["a"], spec, policy)`
- `coerce_bool(series: pd.Series, spec: ColumnSpec, policy: FailurePolicy) -> CoercionResult` — cast booleans; example: `res = coerce_bool(df["a"], spec, policy)`
- `coerce_datetime(series: pd.Series, spec: ColumnSpec, policy: FailurePolicy) -> CoercionResult` — cast datetimes; example: `res = coerce_datetime(df["ts"], spec, policy)`
- `coerce_json(series: pd.Series, spec: ColumnSpec, policy: FailurePolicy) -> CoercionResult` — validate JSON-like content; example: `res = coerce_json(df["payload"], spec, policy)`
- `coerce_binary(series: pd.Series, spec: ColumnSpec, policy: FailurePolicy) -> CoercionResult` — validate binary content; example: `res = coerce_binary(df["blob"], spec, policy)`
- `coerce_series(series: pd.Series, spec: ColumnSpec, policy: FailurePolicy) -> CoercionResult` — dispatch coercion by logical type; example: `res = coerce_series(df["col"], spec, policy)`
- `validate_required_columns_present(df: pd.DataFrame, schema: TableSchema) -> list[str]` — find missing non-nullable columns; example: `missing = validate_required_columns_present(df, schema)`
- `validate_non_nullable(df: pd.DataFrame, schema: TableSchema) -> list[str]` — detect nulls in non-nullable columns; example: `errors = validate_non_nullable(df, schema)`
- `validate_uniques_in_batch(df: pd.DataFrame, unique_sets: list[list[str]]) -> list[str]` — check duplicates for unique sets; example: `errs = validate_uniques_in_batch(df, unique_sets)`
- `validate_unique_against_db(conn, df: pd.DataFrame, schema: TableSchema, chunk_size: int) -> list[str]` — check duplicates against DB; example: `errs = validate_unique_against_db(engine, df, schema, 1000)`
- `validate_foreign_keys(conn, df: pd.DataFrame, schema: TableSchema, chunk_size: int) -> list[str]` — check FK existence; example: `errs = validate_foreign_keys(engine, df, schema, 1000)`
- `infer_add_column_type(series: pd.Series) -> sa.types.TypeEngine` — infer SQL type for new column; example: `t = infer_add_column_type(df["new"])`
- `plan_schema_evolution(schema: TableSchema, df: pd.DataFrame) -> MigrationPlan` — plan ADD COLUMN ops; example: `plan = plan_schema_evolution(schema, df)`
- `execute_migration(conn, schema: str | None, table: str, plan: MigrationPlan) -> None` — execute migration plan; example: `execute_migration(engine, None, "t", plan)`
- `finalize_dataframe(df: pd.DataFrame, schema: TableSchema) -> pd.DataFrame` — convert series to driver-friendly types; example: `df2 = finalize_dataframe(df, schema)`
- `DataCorrector(registry: TypeRegistry | None = None)` — high-level corrector; example: `dc = DataCorrector()`
- `DataCorrector.correct(self, df: pd.DataFrame, conn, schema_name: str | None, table_name: str, config: CorrectionConfig) -> tuple[pd.DataFrame, CorrectionReport]` — correct DataFrame to schema with validation/evolution; example: `clean_df, report = dc.correct(df, engine, None, "users", cfg)`

`schema_introspect/df_align_to_sql.py`
- `normalize_sql_type(col_type: TypeEngine) -> str` — map SQLAlchemy type to canonical; example: `normalize_sql_type(table.c.id.type)`
- `coerce_series(series: pd.Series, canonical: str) -> pd.Series` — cast series to canonical dtype; example: `coerce_series(df["id"], "int")`
- `cast_qualify(before: pd.Series, after: pd.Series, threshold: float = 10.0) -> bool` — check null increase after cast; example: `cast_qualify(orig, casted)`
- `detect_outliers(series: pd.Series, method: str = 'iqr', iqr_scale: float = 1.5, z_threshold: float = 3.0) -> pd.Series` — mark valid values; example: `mask = detect_outliers(df["x"])`
- `correct_outliers(series: pd.Series, valid_mask: pd.Series) -> pd.Series` — null outliers; example: `fixed = correct_outliers(df["x"], mask)`
- `align_column(series: pd.Series, sql_type: TypeEngine, threshold: float = 10.0, fix_outliers: bool = False, outlier_method: str = 'iqr', iqr_scale: float = 1.5, z_threshold: float = 3.0) -> tuple[pd.Series, dict]` — align one column to SQL type; example: `aligned, report = align_column(df["a"], col.type)`
- `detect_table_schema(engine: Engine, table: str) -> str | None` — find schema containing table; example: `schema = detect_table_schema(engine, "users")`
- `infer_sql_type(series: pd.Series) -> TypeEngine` — infer SQL type from series; example: `t = infer_sql_type(df["a"])`
- `add_column_to_table(engine: Engine, table: str, column_name: str, sql_type: TypeEngine, schema: str | None = None) -> None` — add column if missing; example: `add_column_to_table(engine,"users","extra",infer_sql_type(df["extra"]))`
- `align_dataframe_to_schema(df: pd.DataFrame, engine: Engine, table: str, schema: str | None = None, threshold: float = 10.0, fix_outliers: bool = False, auto_alter: bool = False, outlier_method: str = 'iqr', iqr_scale: float = 1.5, z_threshold: float = 3.0) -> tuple[pd.DataFrame, dict]` — align full DF to table; example: `aligned, report = align_dataframe_to_schema(df, engine, "users")`
- `generate_schema(df: pd.DataFrame, dialect: str = 'postgresql') -> dict[str, TypeEngine]` — build dtype mapping for to_sql; example: `schema = generate_schema(df)`
- `save_aligned_dataframe(df: pd.DataFrame, engine: Engine, table: str, schema: str | None = None, if_exists: str = 'append') -> None` — write DF to table preserving dtypes; example: `save_aligned_dataframe(aligned, engine, "users")`

`schema_introspect/df_to_schema.py`
- `df_to_sql_schema(df: pd.DataFrame, table_name: str, engine: create_engine | None = None, dialect: str = 'postgresql', varchar_length: int = 255, include_nullable: bool = True) -> str` — generate CREATE TABLE SQL from DF; example: `sql = df_to_sql_schema(df, "users", dialect="sqlite")`
- `df_to_postgres_schema(df: pd.DataFrame, table_name: str, varchar_length: int = 255) -> str` — Postgres-specific schema SQL; example: `df_to_postgres_schema(df,"users")`
- `df_to_mysql_schema(df: pd.DataFrame, table_name: str, varchar_length: int = 255) -> str` — MySQL schema SQL; example: `df_to_mysql_schema(df,"users")`
- `df_to_oracle_schema(df: pd.DataFrame, table_name: str, varchar_length: int = 255) -> str` — Oracle schema SQL; example: `df_to_oracle_schema(df,"users")`
- `df_to_sqlite_schema(df: pd.DataFrame, table_name: str, varchar_length: int = 255) -> str` — SQLite schema SQL; example: `df_to_sqlite_schema(df,"users")`
- `df_to_mssql_schema(df: pd.DataFrame, table_name: str, varchar_length: int = 255) -> str` — MSSQL schema SQL; example: `df_to_mssql_schema(df,"users")`

**Notes**
- `crud/logger.py` — Logging utilities; load config, decorate calls, snapshot strings/JSON/dataframes, optional wrapper.
- `crud/crud_build.py` — SQL WHERE/CRUD statement builder; parse conditions, build where, select, update, insert with column alignment.
- `crud/crud_harness.py` — Deterministic CRUD test harness; prepare mutated datasets, build verification queries, assert equivalence.
- `crud/crud_v2.py` — Dialect-aware CRUD executors; bulk/row-fallback insert/upsert/update with auto-alignment helpers.
- `crud/evo_csv.py` — CSV CRUD pipeline wrapper; configure logging, run full-cycle validation, optional WHERE prep.
- `crud/evo_pipeline.py` — CSV→DDL→CRUD pipeline; config/report structures, orchestrates run and validation steps with properties.
- `preops/data_cleaner.py` — Data cleaning utilities; config plus string/series/header cleaning and quick pipeline.
- `preops/data_profiler.py` — Profiling and PK inference; profile dataframe, sample dispatcher, derive primary key.
- `preops/casting.py` — Type casting engine; configs and casting variants (auto/fast/safe) with conversion validation.
- `preops/ddl_create.py` — DDL generation/execution; sanitize identifiers, type mapping, build DDL/metadata, generate models, run multi-dialect create/load, CSV entrypoint.
- `schema_introspect/schema_manager.py` — Schema inspector/modifier; normalize dialect, inspect structures, manage DDL and counts.
- `schema_introspect/schema_corrector.py` — Strict DataFrame-to-schema aligner; SchemaAligner with type coercion/validation pipeline.
- `schema_introspect/schema_forge.py` — Logical type system and correction engine; schemas, policies, coercers, validators, migration planning, DataCorrector.
- `schema_introspect/df_align_to_sql.py` — DataFrame alignment utilities; type normalization/coercion, outlier handling, schema detection/evolution, saving aligned data.
- `schema_introspect/df_to_schema.py` — DataFrame→CREATE TABLE generator; dialect-specific schema helpers.