"""Bronze hospital directory, loaded by Auto Loader. All columns stay STRING."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F

from common import HOSPITAL_SRC_MAP, audit, autoloader, dp, hospital_path, snake


@dp.table(
    name="bronze.hospital_directory_raw",
    comment="Raw hospital directory as loaded by Auto Loader. All STRING.",
    table_properties={"quality": "bronze"},
)
def hospital_directory_raw():
    df = audit(autoloader(hospital_path()))
    by_snake = {snake(c): c for c in df.columns}
    missing = [s for s in HOSPITAL_SRC_MAP if s not in by_snake]
    if missing:
        raise ValueError(f"Hospital file is missing expected columns: {missing}")
    cols = [F.col(f"`{by_snake[s]}`").alias(d) for s, d in HOSPITAL_SRC_MAP.items()]
    return df.select(*cols, "_ingest_ts", "_source_file")
