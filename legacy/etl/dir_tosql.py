from __future__ import annotations

from datetime import datetime
from pathlib import Path
import yaml

from .ops import ETL


def _save_config(config_path: Path, config: dict) -> None:
    with config_path.open("w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


def run_batch_etl(engine_or_uri, csv_dir, config_path: str = "csv/config.yml"):
    """Compact batch processor: checks mtime, but DOES NOT write to config file."""
    config_file = Path(config_path)
    with config_file.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    for file in Path(csv_dir).glob("*.csv"):
        cfg = config.get(file.stem)
        if not cfg:
            continue

        mtime = datetime.fromtimestamp(file.stat().st_mtime)
        last_exec = cfg.get("executed_at")
        if last_exec and mtime <= (
            datetime.fromisoformat(last_exec) if isinstance(last_exec, str) else last_exec
        ):
            print(f"Skipping {file.name} (not modified since {last_exec})")
            continue

        print(f"Processing {file.name}...")
        try:
            etl = ETL(engine_or_uri, add_missing_cols=cfg.get("table_column_addition", False))
            op = cfg.get("operation", "upsert").lower()
            pk = cfg.get("pk")
            table_name = cfg.get("table_name", file.stem)
            if op == "upsert":
                success = etl.auto_etl(file, table_name, pk=pk, dtype=cfg.get("dtypes", {}))
            else:
                if etl.engine.dialect.has_table(etl.engine.connect(), table_name):
                    success = etl.append(file, table_name, dtype=cfg.get("dtypes", {}))
                else:
                    success = etl.create_load(
                        file,
                        table_name,
                        pk=pk,
                        drop_if_exists=False,
                        dtype=cfg.get("dtypes", {}),
                    )

            if success:
                print(f"Success: {file.name}")
                print(f"  [INFO] Processed at: {datetime.now().isoformat()}")
                cfg["executed_at"] = datetime.now().isoformat()
                _save_config(config_file, config)
                if pk == "derive":
                    print(
                        f"  [MANUAL UPDATE] Suggested PK for '{file.stem}': "
                        "Check logs above for derived components."
                    )
        except Exception as e:
            print(f"Error {file.name}: {e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        run_batch_etl(sys.argv[2] if len(sys.argv) > 2 else None, sys.argv[1])
