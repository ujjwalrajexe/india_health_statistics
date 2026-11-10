"""Bronze NFHS-5 blood sugar, blood pressure, screening, and lifestyle."""
from common import TABLE_THEME, dp, nfhs_theme

_TABLE = "nfhs_ncd_screening_lifestyle"


@dp.table(
    name=f"bronze.{_TABLE}",
    comment=f"Raw NFHS-5 district fact sheet, theme: {TABLE_THEME[_TABLE]}. All STRING.",
    table_properties={"quality": "bronze"},
)
def nfhs_ncd_screening_lifestyle():
    return nfhs_theme(_TABLE)
