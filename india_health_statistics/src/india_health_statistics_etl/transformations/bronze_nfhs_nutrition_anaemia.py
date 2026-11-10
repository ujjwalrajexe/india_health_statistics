"""Bronze NFHS-5 nutrition and anaemia."""
from common import TABLE_THEME, dp, nfhs_theme

_TABLE = "nfhs_nutrition_anaemia"


@dp.table(
    name=f"bronze.{_TABLE}",
    comment=f"Raw NFHS-5 district fact sheet, theme: {TABLE_THEME[_TABLE]}. All STRING.",
    table_properties={"quality": "bronze"},
)
def nfhs_nutrition_anaemia():
    return nfhs_theme(_TABLE)
