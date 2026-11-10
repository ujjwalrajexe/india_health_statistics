# india_health_statistics

Lakeflow Spark Declarative Pipeline that turns the NFHS-5 district fact sheet and the national hospital directory into district-level health KPIs, a need score, and a service-gap classification.

Source lives in `src/india_health_statistics_etl/transformations/pipeline.py`. Landing files are read from the `source` pipeline configuration (default `/Volumes/india_health_lakehouse/bronze/landing`):

- `<source>/hospitals_directory/*.csv`
- `<source>/health_statistic/*.csv`

All bronze columns stay strings. Silver parses numbers, maps districts, and records data-quality results. Gold is published only when the unmatched-district gate passes.

## Bronze

Nine streaming tables, all loaded with Auto Loader. Nothing is typed or filtered here.

| Table | What it holds |
| --- | --- |
| `bronze.hospital_directory_raw` | One row per hospital. Kept columns: name, category, care type, discipline, address, state, district, subdistrict, pincode, coordinates. Serial number, location text, and telephone are dropped. |
| `bronze.nfhs_survey_housing` | Sample sizes plus housing, registration, water, sanitation, clean fuel, iodized salt, insurance, schooling. |
| `bronze.nfhs_women_fertility_fp` | Women's literacy, early marriage, fertility, and family-planning use and unmet need. |
| `bronze.nfhs_maternal_care` | Antenatal care, IFA, MCP card, postnatal care, and out-of-pocket cost of a public delivery. |
| `bronze.nfhs_delivery_newborn` | Institutional and home births, skilled attendance, C-section, newborn check-up. |
| `bronze.nfhs_child_immunization` | Full vaccination and individual antigens. |
| `bronze.nfhs_child_illness_feeding` | Diarrhoea, ARI, care-seeking, breastfeeding, and diet. |
| `bronze.nfhs_nutrition_anaemia` | Stunting, wasting, underweight, overweight, BMI, and anaemia. |
| `bronze.nfhs_ncd_screening_lifestyle` | Blood sugar, blood pressure, cancer screening, tobacco, and alcohol. |

NFHS headers are renamed by column position, because the source headers contain footnote digits. The order is fixed in `NFHS_SOURCE_ORDER`.

## Silver

NFHS district names are the master list. Hospital spellings are mapped onto that list.

| Table | Grain | Logic |
| --- | --- | --- |
| `silver.dim_district` | One row per NFHS district | `district_key` is `xxhash64` of the normalised state and district. |
| `silver.bridge_district_name_map` | One row per raw state + district spelling | Match order: manual override, exact, normalised, then Levenshtein within the same state. A fuzzy match is kept only when similarity is at least `fuzzy_threshold` (default 0.85). Anything below that is `UNMATCHED`. |
| `silver.hospital_clean` | One row per hospital | Category becomes `PUBLIC`, `PRIVATE`, or `OTHER`. Discipline becomes `ALLOPATHY`, `AYUSH`, or `OTHER`. Coordinates are kept only inside India (lat 6–38, lon 68–98). Pincode must be 6 digits. Duplicates of the same name, pincode, district, and address are dropped. Rows with no district or no name are dropped. |
| `silver.dim_indicator` | One row per indicator | Unit, polarity (`HIGHER_BETTER` or `LOWER_BETTER`), whether it enters the need score, and whether it is a headline KPI. |
| `silver.fact_district_indicator` | One row per district × indicator | The eight NFHS bronze tables are unpivoted. Parentheses and thousands separators are stripped. `*` is `SUPPRESSED`, a parenthetical number is `LOW_SAMPLE`, blanks and `na` are `NA`. Percentages outside 0–100 and unmapped districts are dropped. |
| `silver.dq_results` | One row per rule | Pass rate for district mapping, pincode, coordinates, category, duplicates, and percentage range. |
| `silver.dq_gate` | One row per checked table | Fails the update when the unmapped share exceeds `max_unmatched_health` (default 2% of NFHS rows) or `max_unmatched_hospital` (default 30% of hospital rows). |

Normalisation is uppercase letters and digits only. `&` becomes `AND`, and the words `DISTRICT` and `DIST` are removed. Known state aliases (Orissa → Odisha, NCT of Delhi → Delhi, and similar) are applied before the match.

## Gold

Five tables. `district_hospital_summary`, `district_health_kpi`, and `district_health_score` are cross-joined to a passing `dq_gate`, so a failed gate publishes no rows. The gap and state tables read those outputs, so they stay empty too.

### `gold.district_health_kpi`

One row per district. These are the headline NFHS-5 indicators, pivoted wide. Only values flagged `OK` or `LOW_SAMPLE` are used. `LOW_SAMPLE` means the source printed the number in parentheses; the number is still shown.

| Column | Meaning | Better direction |
| --- | --- | --- |
| `institutional_births_pct` | Births in a health facility (%) | Higher |
| `anc_4plus_visits_pct` | Mothers with 4 or more antenatal visits (%) | Higher |
| `fully_vaccinated_card_or_recall_pct` | Children fully vaccinated, by card or recall (%) | Higher |
| `stunted_u5_pct` | Children under 5 who are stunted (%) | Lower |
| `wasted_u5_pct` | Children under 5 who are wasted (%) | Lower |
| `underweight_u5_pct` | Children under 5 who are underweight (%) | Lower |
| `anaemia_women_15_49_pct` | Women aged 15–49 with anaemia (%) | Lower |
| `anaemia_children_6_59m_pct` | Children aged 6–59 months with anaemia (%) | Lower |
| `women_bp_elevated_or_meds_pct` | Women with elevated blood pressure or on medication (%) | Lower |
| `men_bp_elevated_or_meds_pct` | Men with elevated blood pressure or on medication (%) | Lower |
| `women_sugar_high_or_meds_pct` | Women with high blood sugar or on medication (%) | Lower |
| `fp_unmet_need_total_pct` | Women with an unmet need for family planning (%) | Lower |
| `hh_health_insurance_pct` | Households covered by health insurance (%) | Higher |
| `hh_clean_fuel_pct` | Households using clean cooking fuel (%) | Higher |
| `pop_improved_sanitation_pct` | Population with improved sanitation (%) | Higher |
| `oop_per_delivery_public_rs` | Out-of-pocket cost of a delivery in a public facility (rupees) | Lower |

Each value is the district figure from the fact sheet, not a recalculation from microdata. If a district has no usable value, the column is null.

### `gold.district_health_score`

Demand side. Higher means worse health need relative to other districts in the country.

The score uses the indicators marked `use_in_score` in `silver.dim_indicator`, which is a wider set than the headline KPIs. Insurance, clean fuel, and sanitation are headline KPIs but are not in the score. Scored indicators are grouped into four domains:

- **maternal** — antenatal and postnatal care, institutional and skilled birth, newborn postnatal care, unmet family-planning need, and public-delivery out-of-pocket cost
- **child** — full vaccination and selected antigens, vitamin A, diarrhoea and ARI prevalence and care, exclusive breastfeeding, and adequate diet
- **nutrition** — stunting, wasting, severe wasting, underweight, low BMI in women, and anaemia in children, pregnant women, and women 15–49
- **ncd** — high blood sugar and elevated blood pressure (women and men), tobacco use, and cervical and oral screening

Logic, per district:

1. For each scored indicator, compute a national z-score: `(value − national mean) / national population standard deviation`. Null when the value is missing or the standard deviation is 0.
2. Flip the sign by polarity. `LOWER_BETTER` keeps the z-score, so a high stunting or anaemia rate is high need. `HIGHER_BETTER` uses `−z`, so low immunisation or low antenatal coverage is high need. After this step, `need_z` is always "higher = worse".
3. Domain score = average of `need_z` for the indicators in that domain that have a value.
4. `composite_need_score` = unweighted average of the domain scores that are not null. A district missing one domain is scored on the domains it has.
5. `need_percentile` = `percent_rank` of the composite × 100, over districts that have a score. Spark defines this as `(rank − 1) / (n − 1) × 100`, so the lowest-need district is 0 and the highest-need district is just under 100.
6. `need_rank_in_state` and `need_rank_national` rank districts by composite descending, so rank 1 is the highest need. Ties share a rank.
7. `valid_indicator_share` = share of scored indicators that produced a `need_z`. This drives confidence on the gap table.

### `gold.district_hospital_summary`

Supply side. One row per NFHS district, including districts with zero matched hospitals.

| Column | Logic |
| --- | --- |
| `total_hospitals`, `public_hospitals`, `private_hospitals`, `other_category_hospitals` | Counts from `silver.hospital_clean`. Missing districts are filled with 0. |
| `allopathy_hospitals`, `ayush_hospitals` | Counts by medicine group. |
| `public_share`, `allopathy_share` | Share of `total_hospitals`. Null when the district has no hospitals. |
| `hospitals_by_care_type` | Map of care type to count (for example hospital, clinic, nursing home). |
| `state_percentile_hospitals` | `percent_rank` of `total_hospitals` within the state × 100. Higher means more hospitals than peer districts in the state. |
| `national_percentile_hospitals` | Same rank across all districts. |

Percentiles are based on facility counts, not population, so a large district is not adjusted for size.

### `gold.district_service_gap`

Compares need with hospital supply. This is the table to use for prioritisation.

| Column | Logic |
| --- | --- |
| `need_percentile` | From `district_health_score`. |
| `supply_percentile` | `national_percentile_hospitals` by default. Set pipeline config `gap_scope` to `state` to use `state_percentile_hospitals` instead. |
| `quadrant` | See below. |
| `gap_flag` | True only when the quadrant is `CRITICAL_GAP`. |
| `confidence` | `HIGH` when `valid_indicator_share` ≥ 0.8, `MEDIUM` when ≥ 0.5, otherwise `LOW`. |
| `top_weak_indicator_1` … `_3` | The three scored indicators with the highest positive `need_z` (worse than the national mean after polarity adjustment). |

Thresholds default to 50 and are configurable (`need_threshold`, `supply_threshold`):

| Quadrant | Rule |
| --- | --- |
| `CRITICAL_GAP` | Need percentile ≥ 50 and supply percentile < 50. High need, thin hospital supply. |
| `PRESSURE` | Need percentile ≥ 50 and supply percentile ≥ 50. High need, supply is not in the bottom half. |
| `MONITOR` | Need percentile < 50 and supply percentile < 50. Need is not high, but supply is thin. |
| `ADEQUATE` | Need percentile < 50 and supply percentile ≥ 50. |
| `UNKNOWN` | Need percentile is null, so the district could not be scored. |

### `gold.state_summary`

One row per state, rolled up from the district gold tables. Averages are unweighted means of district values, not population-weighted.

| Column | Logic |
| --- | --- |
| `n_districts` | Districts in the state on the gap table. |
| `total_hospitals` | Sum of district hospital counts. |
| `public_share` | State public hospitals ÷ state total hospitals. |
| `avg_need_score` | Mean of `composite_need_score`. |
| `avg_institutional_births_pct` | Mean of the district institutional-birth KPI. |
| `avg_full_immunization_pct` | Mean of `fully_vaccinated_card_or_recall_pct`. |
| `avg_stunting_pct` | Mean of `stunted_u5_pct`. |
| `n_critical_gap`, `n_pressure`, `n_monitor`, `n_adequate` | District counts in each quadrant. |
| `pct_districts_critical` | `n_critical_gap / n_districts × 100`, rounded to 2 decimals. |

## Getting started

Choose how you want to work on this project:

(a) Directly in your Databricks workspace, see
    https://docs.databricks.com/dev-tools/bundles/workspace.

(b) Locally with an IDE like Cursor or VS Code, see
    https://docs.databricks.com/dev-tools/vscode-ext.html.

(c) With command line tools, see https://docs.databricks.com/dev-tools/cli/databricks-cli.html

### Using this project with the CLI

1. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

2. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys a pipeline called
    `[dev yourname] india_health_statistics_etl` to your workspace.
    You can find that resource by opening your workspace and clicking on **Jobs & Pipelines**.

3. To deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```
   The job in `resources/sample_job.job.yml` refreshes the pipeline once a day.
   The schedule is paused when deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

4. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

Catalog is `india_health_lakehouse`. The dev schema is the current user; prod uses schema `prod`. Tables are created in the `bronze`, `silver`, and `gold` schemas from the two-part names in the pipeline.
