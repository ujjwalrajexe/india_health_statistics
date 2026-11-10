"""Bronze NFHS-5 women's literacy, fertility, and family planning."""
from common import TABLE_THEME, dp, nfhs_theme

_TABLE = "nfhs_women_fertility_fp"


@dp.table(
    name=f"bronze.{_TABLE}",
    comment=f"Raw NFHS-5 district fact sheet, theme: {TABLE_THEME[_TABLE]}. All STRING.",
    table_properties={"quality": "bronze"},
)
def nfhs_women_fertility_fp():
    return nfhs_theme(_TABLE)
