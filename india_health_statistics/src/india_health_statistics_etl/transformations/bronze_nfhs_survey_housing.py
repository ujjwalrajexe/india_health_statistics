"""Bronze NFHS-5 survey sample, housing, and household amenities."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import TABLE_THEME, dp, nfhs_theme

_TABLE = "nfhs_survey_housing"


@dp.table(
    name=f"bronze.{_TABLE}",
    comment=f"Raw NFHS-5 district fact sheet, theme: {TABLE_THEME[_TABLE]}. All STRING.",
    table_properties={"quality": "bronze"},
)
def nfhs_survey_housing():
    return nfhs_theme(_TABLE)
