"""Single Auto Loader read of the NFHS file, renamed positionally to snake_case.

Theme tables in the other bronze_nfhs_*.py files stream from the nfhs_raw view.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F

from common import NFHS_SOURCE_ORDER, audit, autoloader, dp, nfhs_path


@dp.temporary_view(name="nfhs_raw")
def nfhs_raw():
    df = audit(autoloader(nfhs_path()))
    source_cols = [c for c in df.columns if c not in ("_ingest_ts", "_source_file", "_rescued_data")]
    if len(source_cols) < len(NFHS_SOURCE_ORDER):
        raise ValueError(
            f"NFHS file has {len(source_cols)} columns, expected {len(NFHS_SOURCE_ORDER)}.")
    renamed = [F.col(f"`{old}`").alias(new)
               for old, new in zip(source_cols[:len(NFHS_SOURCE_ORDER)], NFHS_SOURCE_ORDER)]
    return df.select(*renamed, "_ingest_ts", "_source_file")
