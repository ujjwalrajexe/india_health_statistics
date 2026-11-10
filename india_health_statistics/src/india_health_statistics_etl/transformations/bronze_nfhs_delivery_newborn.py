"""Bronze NFHS-5 delivery, C-section, and newborn care."""
from common import TABLE_THEME, dp, nfhs_theme

_TABLE = "nfhs_delivery_newborn"


@dp.table(
    name=f"bronze.{_TABLE}",
    comment=f"Raw NFHS-5 district fact sheet, theme: {TABLE_THEME[_TABLE]}. All STRING.",
    table_properties={"quality": "bronze"},
)
def nfhs_delivery_newborn():
    return nfhs_theme(_TABLE)
