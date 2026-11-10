"""Bronze NFHS-5 child immunisation."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import TABLE_THEME, dp, nfhs_theme

_TABLE = "nfhs_child_immunization"


@dp.table(
    name=f"bronze.{_TABLE}",
    comment=f"Raw NFHS-5 district fact sheet, theme: {TABLE_THEME[_TABLE]}. All STRING.",
    table_properties={"quality": "bronze"},
)
def nfhs_child_immunization():
    return nfhs_theme(_TABLE)
