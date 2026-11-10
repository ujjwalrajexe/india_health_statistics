"""
India Public Health Service Intelligence Platform
Lakeflow Spark Declarative Pipeline (PySpark) with Auto Loader

Layers
  bronze (9)  : bronze.hospital_directory_raw + 8 bronze.nfhs_* tables   (streaming, Auto Loader)
  silver (6)  : dim_district, bridge_district_name_map, hospital_clean,
                dim_indicator, fact_district_indicator, dq_results      (+ dq_gate)
  gold   (5)  : district_hospital_summary, district_health_kpi, district_health_score,
                district_service_gap, state_summary

Pipeline configuration (Settings > Configuration), all optional:
  landing_path            default /Volumes/main/health/landing
                          expects  <landing_path>/hospital/*.csv  and  <landing_path>/nfhs/*.csv
  fuzzy_threshold         default 0.85   min similarity for a FUZZY district match
  max_unmatched_health    default 0.02   DQ gate: max share of unmapped NFHS rows
  max_unmatched_hospital  default 0.02   DQ gate: max share of unmapped hospital rows
  need_threshold          default 50     percentile at/above which need is "high"
  supply_threshold        default 50     percentile below which supply is "low"
  gap_scope               default national   (national | state) percentile used for supply
"""
import re
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    from pyspark import pipelines as dp          # Lakeflow Spark Declarative Pipelines
except ImportError:                               # older runtimes: import dlt
    import dlt as dp
    if not hasattr(dp, "materialized_view"):
        dp.materialized_view = dp.table
        dp.temporary_view = dp.view

# --------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------
LANDING = spark.conf.get("source")
HOSPITAL_PATH = f"{LANDING}/hospitals_directory"
NFHS_PATH = f"{LANDING}/health_statistic"

FUZZY_THRESHOLD = float(spark.conf.get("fuzzy_threshold", "0.85"))
MAX_UNMATCHED = {
    "silver.fact_district_indicator": float(spark.conf.get("max_unmatched_health", "0.02")),
    "silver.hospital_clean": float(spark.conf.get("max_unmatched_hospital", "0.30")),
}
NEED_THRESHOLD = float(spark.conf.get("need_threshold", "50"))
SUPPLY_THRESHOLD = float(spark.conf.get("supply_threshold", "50"))
GAP_SCOPE = spark.conf.get("gap_scope", "national")

# --------------------------------------------------------------------------------------
# NFHS column metadata
# The NFHS file is renamed POSITIONALLY (headers contain footnote digits and odd chars),
# so the order below must match the column order of your source file.
# --------------------------------------------------------------------------------------
SAMPLE_COLS = ["households_surveyed", "women_15_49_interviewed", "men_15_54_interviewed"]

HOUSING = SAMPLE_COLS + [
    "female_pop_6plus_ever_attended_school_pct", "pop_below_15_pct",
    "sex_ratio_total_pop", "sex_ratio_at_birth_last_5y",
    "under5_birth_registered_pct", "deaths_3y_registered_pct",
    "pop_electricity_pct", "pop_improved_water_pct", "pop_improved_sanitation_pct",
    "hh_clean_fuel_pct", "hh_iodized_salt_pct", "hh_health_insurance_pct",
    "children_5y_pre_primary_pct"]

WOMEN_FP = [
    "women_literate_pct", "women_10plus_schooling_pct",
    "women_20_24_married_before_18_pct", "births_3rd_order_plus_pct",
    "women_15_19_mother_or_pregnant_pct", "women_15_24_hygienic_menstrual_pct",
    "fp_any_method_pct", "fp_modern_method_pct", "fp_female_sterilization_pct",
    "fp_male_sterilization_pct", "fp_iud_ppiud_pct", "fp_pill_pct", "fp_condom_pct",
    "fp_injectables_pct", "fp_unmet_need_total_pct", "fp_unmet_need_spacing_pct",
    "fp_hw_talked_to_nonusers_pct", "fp_users_told_side_effects_pct"]

MATERNAL = [
    "anc_first_trimester_pct", "anc_4plus_visits_pct", "tetanus_protected_birth_pct",
    "ifa_100_days_pct", "ifa_180_days_pct", "mcp_card_pct",
    "pnc_mother_2days_pct", "oop_per_delivery_public_rs"]

# In the source file the two newborn-care columns come BEFORE the institutional-birth columns.
DELIVERY_POSTNATAL = ["home_birth_newborn_checkup_24h_pct", "newborn_pnc_2days_pct"]
DELIVERY_BIRTH = [
    "institutional_births_pct", "institutional_births_public_pct",
    "home_births_skilled_pct", "births_skilled_attendant_pct",
    "csection_total_pct", "csection_private_pct", "csection_public_pct"]

IMMUNIZATION = [
    "fully_vaccinated_card_or_recall_pct", "fully_vaccinated_card_only_pct",
    "bcg_pct", "polio_3dose_pct", "penta_dpt_3dose_pct", "mcv1_pct", "mcv2_24_35m_pct",
    "rota_3dose_pct", "penta_hepb_3dose_pct", "vitamin_a_9_35m_pct",
    "vaccination_public_facility_pct", "vaccination_private_facility_pct"]

CHILD_ILLNESS = [
    "diarrhoea_prevalence_pct", "diarrhoea_ors_pct", "diarrhoea_zinc_pct",
    "diarrhoea_care_seeking_pct", "ari_prevalence_pct", "fever_ari_care_seeking_pct",
    "breastfed_within_1h_pct", "exclusive_bf_under6m_pct", "solid_food_6_8m_pct",
    "adequate_diet_breastfed_6_23m_pct", "adequate_diet_nonbreastfed_6_23m_pct",
    "adequate_diet_total_6_23m_pct"]

NUTRITION = [
    "stunted_u5_pct", "wasted_u5_pct", "severely_wasted_u5_pct", "underweight_u5_pct",
    "overweight_u5_pct", "women_bmi_below_normal_pct", "women_overweight_obese_pct",
    "women_high_waist_hip_pct", "anaemia_children_6_59m_pct",
    "anaemia_nonpregnant_women_pct", "anaemia_pregnant_women_pct",
    "anaemia_women_15_49_pct", "anaemia_women_15_19_pct"]

NCD = [
    "women_sugar_high_pct", "women_sugar_very_high_pct", "women_sugar_high_or_meds_pct",
    "men_sugar_high_pct", "men_sugar_very_high_pct", "men_sugar_high_or_meds_pct",
    "women_bp_mild_pct", "women_bp_mod_severe_pct", "women_bp_elevated_or_meds_pct",
    "men_bp_mild_pct", "men_bp_mod_severe_pct", "men_bp_elevated_or_meds_pct",
    "cervical_screening_pct", "breast_exam_pct", "oral_exam_pct",
    "tobacco_women_pct", "tobacco_men_pct", "alcohol_women_pct", "alcohol_men_pct"]

# Positional order of the source file (after the 2 key columns)
NFHS_SOURCE_ORDER = (["district_name_raw", "state_ut_raw"] + HOUSING + WOMEN_FP + MATERNAL
                     + DELIVERY_POSTNATAL + DELIVERY_BIRTH + IMMUNIZATION + CHILD_ILLNESS
                     + NUTRITION + NCD)

BRONZE_NFHS_TABLES = {
    "nfhs_survey_housing": HOUSING,
    "nfhs_women_fertility_fp": WOMEN_FP,
    "nfhs_maternal_care": MATERNAL,
    "nfhs_delivery_newborn": DELIVERY_BIRTH + DELIVERY_POSTNATAL,
    "nfhs_child_immunization": IMMUNIZATION,
    "nfhs_child_illness_feeding": CHILD_ILLNESS,
    "nfhs_nutrition_anaemia": NUTRITION,
    "nfhs_ncd_screening_lifestyle": NCD,
}

TABLE_THEME = {
    "nfhs_survey_housing": "SOCIOECONOMIC",
    "nfhs_women_fertility_fp": "FAMILY_PLANNING",
    "nfhs_maternal_care": "MATERNAL",
    "nfhs_delivery_newborn": "DELIVERY",
    "nfhs_child_immunization": "IMMUNIZATION",
    "nfhs_child_illness_feeding": "CHILD_ILLNESS",
    "nfhs_nutrition_anaemia": "NUTRITION",
    "nfhs_ncd_screening_lifestyle": "NCD",
}

# Indicators that go into the fact table (sample-size columns are excluded)
INDICATOR_TABLES = {t: [c for c in cols if c not in SAMPLE_COLS]
                    for t, cols in BRONZE_NFHS_TABLES.items()}

LOWER_BETTER_EXACT = {
    "women_20_24_married_before_18_pct", "births_3rd_order_plus_pct",
    "women_15_19_mother_or_pregnant_pct", "fp_unmet_need_total_pct",
    "fp_unmet_need_spacing_pct", "oop_per_delivery_public_rs",
    "diarrhoea_prevalence_pct", "ari_prevalence_pct", "csection_total_pct",
    "csection_private_pct", "csection_public_pct"}
LOWER_BETTER_PREFIX = ("anaemia_", "women_sugar", "men_sugar", "women_bp", "men_bp",
                       "tobacco_", "alcohol_", "stunted", "wasted", "severely_wasted",
                       "underweight", "overweight", "women_bmi", "women_overweight",
                       "women_high_waist")

# Indicators used in the need score
SCORE_INDICATORS = {
    "anc_first_trimester_pct", "anc_4plus_visits_pct", "ifa_180_days_pct", "mcp_card_pct",
    "pnc_mother_2days_pct", "oop_per_delivery_public_rs", "fp_unmet_need_total_pct",
    "institutional_births_pct", "births_skilled_attendant_pct", "newborn_pnc_2days_pct",
    "fully_vaccinated_card_or_recall_pct", "bcg_pct", "penta_dpt_3dose_pct", "mcv1_pct",
    "vitamin_a_9_35m_pct", "diarrhoea_prevalence_pct", "diarrhoea_ors_pct",
    "diarrhoea_care_seeking_pct", "ari_prevalence_pct", "fever_ari_care_seeking_pct",
    "exclusive_bf_under6m_pct", "adequate_diet_total_6_23m_pct",
    "stunted_u5_pct", "wasted_u5_pct", "severely_wasted_u5_pct", "underweight_u5_pct",
    "women_bmi_below_normal_pct", "anaemia_children_6_59m_pct",
    "anaemia_pregnant_women_pct", "anaemia_women_15_49_pct",
    "women_sugar_high_or_meds_pct", "men_sugar_high_or_meds_pct",
    "women_bp_elevated_or_meds_pct", "men_bp_elevated_or_meds_pct",
    "tobacco_women_pct", "tobacco_men_pct", "cervical_screening_pct", "oral_exam_pct"}

HEADLINE_KPIS = [
    "institutional_births_pct", "anc_4plus_visits_pct", "fully_vaccinated_card_or_recall_pct",
    "stunted_u5_pct", "wasted_u5_pct", "underweight_u5_pct",
    "anaemia_women_15_49_pct", "anaemia_children_6_59m_pct",
    "women_bp_elevated_or_meds_pct", "men_bp_elevated_or_meds_pct",
    "women_sugar_high_or_meds_pct", "fp_unmet_need_total_pct",
    "hh_health_insurance_pct", "hh_clean_fuel_pct", "pop_improved_sanitation_pct",
    "oop_per_delivery_public_rs"]

THEME_TO_DOMAIN = {
    "MATERNAL": "maternal", "DELIVERY": "maternal", "FAMILY_PLANNING": "maternal",
    "IMMUNIZATION": "child", "CHILD_ILLNESS": "child",
    "NUTRITION": "nutrition", "NCD": "ncd",
}
DOMAINS = ["maternal", "child", "nutrition", "ncd"]

# --------------------------------------------------------------------------------------
# District / state normalisation helpers
# --------------------------------------------------------------------------------------
# Keys are normalised (UPPERCASE, A-Z0-9 only). Values are the normalised NFHS name.
STATE_ALIASES = {
    "ORISSA": "ODISHA",
    "UTTARANCHAL": "UTTARAKHAND",
    "NCTOFDELHI": "DELHI",
    "PONDICHERRY": "PUDUCHERRY",
    "ANDAMANANDNICOBARISLANDS": "ANDAMANANDNICOBAR",
    "DADRAANDNAGARHAVELI": "DADRAANDNAGARHAVELIANDDAMANANDDIU",
    "DAMANANDDIU": "DADRAANDNAGARHAVELIANDDAMANANDDIU",
    "NORTHTWENTYFOURPARGANAS": "WESTBENGAL",   # district name typed into the state column
    "MAHARASHTRA": "MAHARASTRA",               # NFHS file spells it "Maharastra"
}

# Manual fixes after inspecting bridge rows with match_method = 'UNMATCHED'.
# Key:   "<STATE_NORM>|<DISTRICT_NORM>" as it appears in the hospital file
# Value: "<STATE_NORM>|<DISTRICT_NORM>" as it appears in NFHS (state can change too).
# Normalised = UPPERCASE, letters and digits only.
DISTRICT_OVERRIDES = {
    "ANDHRAPRADESH|HYDERABAD": "TELANGANA|HYDERABAD",
}


def _case(col, mapping):
    """CASE WHEN col = k THEN v ... (NULL when nothing matches)."""
    expr = None
    for k, v in mapping.items():
        expr = F.when(col == k, F.lit(v)) if expr is None else expr.when(col == k, F.lit(v))
    return expr if expr is not None else F.lit(None).cast("string")


def _alnum_upper(col):
    c = F.upper(F.trim(col))
    c = F.regexp_replace(c, r"&", " AND ")
    c = F.regexp_replace(c, r"\b(DISTRICT|DIST)\b", " ")
    return F.regexp_replace(c, r"[^A-Z0-9]", "")


def _state_norm(col):
    s = _alnum_upper(col)
    return F.coalesce(_case(s, STATE_ALIASES), s)


def _district_norm(col):
    return _alnum_upper(col)


def _snake(name):
    return re.sub(r"[^0-9a-z]+", "_", name.strip().lower()).strip("_")


# ======================================================================================
# BRONZE  (Auto Loader, all columns STRING)
# ======================================================================================
def _autoloader(path):
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "false")     # keep everything STRING
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("header", "true")
            .option("multiLine", "true")
            .option("quote", '"')
            .option("escape", '"')
            .load(path))


def _audit(df):
    return (df.withColumn("_ingest_ts", F.current_timestamp())
              .withColumn("_source_file", F.col("_metadata.file_path")))


HOSPITAL_SRC_MAP = {
    "location_coordinates": "location_coordinates",
    "hospital_name": "hospital_name",
    "hospital_category": "hospital_category",
    "hospital_care_type": "hospital_care_type",
    "discipline_systems_of_medicine": "discipline_systems_of_medicine",
    "address_original_first_line": "address_line",
    "state": "state",
    "district": "district",
    "subdistrict": "subdistrict",
    "pincode": "pincode",
}   # Dropped on purpose: Sr_No, Location, Telephone


@dp.table(
    name="bronze.hospital_directory_raw",
    comment="Raw hospital directory as loaded by Auto Loader. All STRING.",
    table_properties={"quality": "bronze"},
)
def hospital_directory_raw():
    df = _audit(_autoloader(HOSPITAL_PATH))
    by_snake = {_snake(c): c for c in df.columns}
    missing = [s for s in HOSPITAL_SRC_MAP if s not in by_snake]
    if missing:
        raise ValueError(f"Hospital file is missing expected columns: {missing}")
    cols = [F.col(f"`{by_snake[s]}`").alias(d) for s, d in HOSPITAL_SRC_MAP.items()]
    return df.select(*cols, "_ingest_ts", "_source_file")


@dp.temporary_view(name="nfhs_raw")
def nfhs_raw():
    """Single Auto Loader read of the NFHS file, renamed positionally to snake_case."""
    df = _audit(_autoloader(NFHS_PATH))
    source_cols = [c for c in df.columns if c not in ("_ingest_ts", "_source_file", "_rescued_data")]
    if len(source_cols) < len(NFHS_SOURCE_ORDER):
        raise ValueError(
            f"NFHS file has {len(source_cols)} columns, expected {len(NFHS_SOURCE_ORDER)}.")
    renamed = [F.col(f"`{old}`").alias(new)
               for old, new in zip(source_cols[:len(NFHS_SOURCE_ORDER)], NFHS_SOURCE_ORDER)]
    return df.select(*renamed, "_ingest_ts", "_source_file")


def _make_bronze_nfhs(table, codes):
    @dp.table(
        name=f"bronze.{table}",
        comment=f"Raw NFHS-5 district fact sheet, theme: {TABLE_THEME[table]}. All STRING.",
        table_properties={"quality": "bronze"},
    )
    def _bronze():
        return (spark.readStream.table("nfhs_raw")
                .select("district_name_raw", "state_ut_raw", *codes, "_ingest_ts", "_source_file"))
    return _bronze


for _t, _codes in BRONZE_NFHS_TABLES.items():
    _make_bronze_nfhs(_t, _codes)


# ======================================================================================
# SILVER
# ======================================================================================
@dp.materialized_view(
    name="silver.dim_district",
    comment="One row per district. NFHS is the master list of districts.",
    table_properties={"quality": "silver"},
)
def dim_district():
    src = (spark.read.table("bronze.nfhs_survey_housing")
           .select("state_ut_raw", "district_name_raw")
           .filter(F.col("state_ut_raw").isNotNull() & F.col("district_name_raw").isNotNull())
           .distinct())
    df = (src
          .withColumn("state_std", F.initcap(F.trim("state_ut_raw")))
          .withColumn("district_std", F.initcap(F.regexp_replace(F.trim("district_name_raw"), r"\s+", " ")))
          .withColumn("district_norm", _district_norm(F.col("district_std")))
          .withColumn("district_key", F.xxhash64(_state_norm(F.col("state_std")), F.col("district_norm")))
          .dropDuplicates(["district_key"]))
    return df.select("district_key", "state_std", "district_std", "district_norm")


def _normalised_pairs(df, source_system, state_col, district_col):
    p = (df.select(F.lit(source_system).alias("source_system"),
                   F.col(state_col).alias("state_raw"),
                   F.col(district_col).alias("district_raw"))
           .filter(F.col("state_raw").isNotNull() & F.col("district_raw").isNotNull())
           .distinct()
           .withColumn("s_norm", _state_norm(F.col("state_raw")))
           .withColumn("d_norm0", _district_norm(F.col("district_raw")))
           .withColumn("override", _case(F.concat_ws("|", "s_norm", "d_norm0"), DISTRICT_OVERRIDES))
           .withColumn("s_norm", F.coalesce(F.split("override", r"\|")[0], F.col("s_norm")))
           .withColumn("d_norm", F.coalesce(F.split("override", r"\|")[1], F.col("d_norm0")))
           .withColumn("overridden", F.col("override").isNotNull()))
    return p.drop("d_norm0", "override")


@dp.materialized_view(
    name="silver.bridge_district_name_map",
    comment="Maps every raw (state, district) spelling to a district_key.",
    table_properties={"quality": "silver"},
)
def bridge_district_name_map():
    dim = spark.read.table("silver.dim_district")
    d = dim.select(F.col("district_key").alias("dk"), "district_std",
                   _state_norm(F.col("state_std")).alias("d_snorm"),
                   F.col("district_norm").alias("d_dnorm"))

    nfhs = _normalised_pairs(spark.read.table("bronze.nfhs_survey_housing"),
                             "NFHS", "state_ut_raw", "district_name_raw")
    hosp = _normalised_pairs(spark.read.table("bronze.hospital_directory_raw"),
                             "HOSPITAL", "state", "district")
    pairs = nfhs.unionByName(hosp)
    pair_cols = pairs.columns

    # 1) exact / normalised / manual match on (state_norm, district_norm)
    joined = pairs.join(d, (pairs.s_norm == d.d_snorm) & (pairs.d_norm == d.d_dnorm), "left")
    matched = (joined.filter(F.col("dk").isNotNull())
               .select("source_system", "state_raw", "district_raw",
                       F.col("dk").alias("district_key"),
                       F.when(F.col("overridden"), "MANUAL")
                        .when(F.upper(F.trim("district_raw")) == F.upper("district_std"), "EXACT")
                        .otherwise("NORMALIZED").alias("match_method"),
                       F.lit(1.0).alias("match_score"),
                       F.col("district_std").alias("closest_district_std")))

    # 2) fuzzy match (Levenshtein similarity) within the same state for the rest
    leftover = joined.filter(F.col("dk").isNull()).select(*pair_cols)
    cand = leftover.join(d, leftover.s_norm == d.d_snorm, "left")
    cand = cand.withColumn(
        "score",
        F.when(F.col("dk").isNotNull(),
               1 - F.levenshtein("d_norm", "d_dnorm")
                   / F.greatest(F.length("d_norm"), F.length("d_dnorm"), F.lit(1))))
    w = Window.partitionBy("source_system", "state_raw", "district_raw") \
              .orderBy(F.col("score").desc_nulls_last())
    best = cand.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
    fuzzy = best.select(
        "source_system", "state_raw", "district_raw",
        F.when(F.col("score") >= FUZZY_THRESHOLD, F.col("dk")).alias("district_key"),
        F.when(F.col("score") >= FUZZY_THRESHOLD, "FUZZY").otherwise("UNMATCHED").alias("match_method"),
        F.col("score").alias("match_score"),
        F.col("district_std").alias("closest_district_std"))

    return matched.unionByName(fuzzy)


@dp.temporary_view(name="hospital_staged")
def hospital_staged():
    """All hospital rows with district_key, standardised columns and DQ flags (nothing dropped)."""
    h = spark.read.table("bronze.hospital_directory_raw")
    bridge = (spark.read.table("silver.bridge_district_name_map")
              .filter("source_system = 'HOSPITAL'")
              .select(F.col("state_raw").alias("b_state"),
                      F.col("district_raw").alias("b_district"), "district_key"))
    h = h.join(bridge, (h.state == bridge.b_state) & (h.district == bridge.b_district), "left")

    cat = F.lower(F.trim("hospital_category"))
    disc = F.lower(F.coalesce(F.col("discipline_systems_of_medicine"), F.lit("")))
    coord = F.col("location_coordinates")
    pat = r"(-?\d+(?:\.\d+)?)\s*[,;\s]\s*(-?\d+(?:\.\d+)?)"
    lat_s, lon_s = F.regexp_extract(coord, pat, 1), F.regexp_extract(coord, pat, 2)
    lat = F.when(F.length(lat_s) > 0, lat_s.cast("double"))
    lon = F.when(F.length(lon_s) > 0, lon_s.cast("double"))
    in_india = F.coalesce(lat.between(6, 38) & lon.between(68, 98), F.lit(False))
    pin = F.regexp_replace(F.trim("pincode"), r"\.0+$", "")

    df = (h
          .withColumn("hospital_name", F.initcap(F.regexp_replace(F.trim("hospital_name"), r"\s+", " ")))
          .withColumn("hospital_category",
                      F.when(cat.rlike("public|govern|govt"), "PUBLIC")
                       .when(cat.rlike("private"), "PRIVATE")
                       .otherwise("OTHER"))
          .withColumn("hospital_care_type",
                      F.coalesce(F.initcap(F.trim("hospital_care_type")), F.lit("Unknown")))
          .withColumn("discipline", F.initcap(F.trim("discipline_systems_of_medicine")))
          .withColumn("medicine_group",
                      F.when(disc.rlike("allopath|modern"), "ALLOPATHY")
                       .when(disc.rlike("ayush|ayurved|yoga|unani|siddha|homeo|naturopath"), "AYUSH")
                       .otherwise("OTHER"))
          .withColumn("address_line", F.trim("address_line"))
          .withColumn("subdistrict", F.initcap(F.trim("subdistrict")))
          .withColumn("pincode", F.when(pin.rlike(r"^[1-9][0-9]{5}$"), pin))
          .withColumn("coords_valid", in_india)
          .withColumn("latitude", F.when(in_india, lat))
          .withColumn("longitude", F.when(in_india, lon))
          .withColumn("hospital_id",
                      F.xxhash64(_alnum_upper(F.col("hospital_name")),
                                 F.coalesce("pincode", F.lit("")),
                                 F.coalesce("district_key", F.lit(0).cast("bigint")),
                                 F.lower(F.coalesce("address_line", F.lit(""))))))
    dup_w = Window.partitionBy("hospital_id").orderBy("_ingest_ts")
    return df.withColumn("is_duplicate", F.row_number().over(dup_w) > 1)


@dp.materialized_view(
    name="silver.hospital_clean",
    comment="One row per hospital: standardised, mapped to district_key, de-duplicated.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop({
    "district_mapped": "district_key IS NOT NULL",
    "name_present": "hospital_name IS NOT NULL AND hospital_name <> ''",
    "category_allowed": "hospital_category IN ('PUBLIC','PRIVATE','OTHER')",
})
@dp.expect_all({
    "pincode_valid": "pincode IS NOT NULL",
    "coords_in_india": "coords_valid",
})
def hospital_clean():
    return (spark.read.table("hospital_staged")
            .filter(~F.col("is_duplicate"))
            .select("hospital_id", "hospital_name", "hospital_category", "hospital_care_type",
                    "discipline", "medicine_group", "address_line", "subdistrict", "pincode",
                    "latitude", "longitude", "coords_valid", "district_key"))


def _indicator_rows():
    rows = []
    for table, codes in INDICATOR_TABLES.items():
        for c in codes:
            if c.endswith("_pct"):
                unit = "pct"
            elif c.endswith("_rs"):
                unit = "rs"
            elif c.startswith("sex_ratio"):
                unit = "ratio"
            else:
                unit = "count"
            lower = c in LOWER_BETTER_EXACT or c.startswith(LOWER_BETTER_PREFIX)
            rows.append((c, c.replace("_", " "), TABLE_THEME[table], unit,
                         "LOWER_BETTER" if lower else "HIGHER_BETTER",
                         c in SCORE_INDICATORS, c in HEADLINE_KPIS))
    return rows


@dp.materialized_view(
    name="silver.dim_indicator",
    comment="Indicator metadata that drives generic scoring.",
    table_properties={"quality": "silver"},
)
def dim_indicator():
    return spark.createDataFrame(
        _indicator_rows(),
        "indicator_code string, indicator_name string, theme string, unit string, "
        "polarity string, use_in_score boolean, is_headline_kpi boolean")


@dp.temporary_view(name="fact_staged")
def fact_staged():
    """Unpivot all 8 bronze health tables and parse survey flags. Nothing is dropped."""
    def unpivot(table, codes):
        stack = (f"stack({len(codes)}, "
                 + ", ".join(f"'{c}', `{c}`" for c in codes)
                 + ") AS (indicator_code, value_raw)")
        return (spark.read.table(f"bronze.{table}")
                .select("district_name_raw", "state_ut_raw", F.expr(stack)))

    tall = reduce(lambda a, b: a.unionByName(b),
                  [unpivot(t, c) for t, c in INDICATOR_TABLES.items()])

    bridge = (spark.read.table("silver.bridge_district_name_map")
              .filter("source_system = 'NFHS'")
              .select(F.col("state_raw").alias("b_state"),
                      F.col("district_raw").alias("b_district"), "district_key"))
    unit = spark.read.table("silver.dim_indicator").select("indicator_code", "unit")

    df = (tall
          .join(bridge, (tall.state_ut_raw == bridge.b_state)
                        & (tall.district_name_raw == bridge.b_district), "left")
          .join(F.broadcast(unit), "indicator_code", "left"))

    raw = F.trim("value_raw")
    cleaned = F.regexp_replace(raw, r"[(),\s]", "")           # strip (xx.x) brackets and thousands commas
    num = F.when(cleaned.rlike(r"^-?\d+(\.\d+)?$"), cleaned.cast("decimal(10,2)"))
    flag = (F.when(raw.isNull() | (raw == "") | F.lower(raw).isin("na", "n/a", "-", "--"), "NA")
             .when(raw == "*", "SUPPRESSED")
             .when(raw.startswith("("), "LOW_SAMPLE")
             .otherwise("OK"))

    df = (df.withColumn("value_num", num)
            .withColumn("value_flag", F.when(flag.isin("OK", "LOW_SAMPLE") & num.isNull(), "NA")
                                       .otherwise(flag))
            .withColumn("pct_out_of_range",
                        (F.col("unit") == "pct") & F.col("value_num").isNotNull()
                        & ((F.col("value_num") < 0) | (F.col("value_num") > 100))))
    return df.select("district_key", "indicator_code", "value_raw", "value_num",
                     "value_flag", "unit", "pct_out_of_range")


@dp.materialized_view(
    name="silver.fact_district_indicator",
    comment="Tall table: one row per district x indicator.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("district_mapped", "district_key IS NOT NULL")
def fact_district_indicator():
    return (spark.read.table("fact_staged")
            .filter(~F.col("pct_out_of_range"))
            .select("district_key", "indicator_code", "value_num", "value_raw", "value_flag"))


def _rule(df, table, rule_name, fail_expr):
    failed = F.sum(F.when(F.expr(fail_expr), 1).otherwise(0)).cast("bigint")
    return (df.agg(F.count(F.lit(1)).cast("bigint").alias("rows_checked"),
                   F.coalesce(failed, F.lit(0).cast("bigint")).alias("rows_failed"))
              .select(F.lit(table).alias("table_name"), F.lit(rule_name).alias("rule_name"),
                      "rows_checked", "rows_failed"))


@dp.materialized_view(
    name="silver.dq_results",
    comment="Data quality results for the latest run (history is in the pipeline event log).",
    table_properties={"quality": "silver"},
)
def dq_results():
    hs = spark.read.table("hospital_staged")
    fs = spark.read.table("fact_staged")
    ht, ft = "silver.hospital_clean", "silver.fact_district_indicator"
    rules = [
        _rule(hs, ht, "district_key_mapped", "district_key IS NULL"),
        _rule(hs, ht, "pincode_6_digits", "pincode IS NULL"),
        _rule(hs, ht, "coords_inside_india", "NOT coords_valid"),
        _rule(hs, ht, "category_allowed", "hospital_category NOT IN ('PUBLIC','PRIVATE','OTHER')"),
        _rule(hs, ht, "no_duplicate_hospitals", "is_duplicate"),
        _rule(fs, ft, "district_key_mapped", "district_key IS NULL"),
        _rule(fs, ft, "pct_between_0_100", "pct_out_of_range"),
    ]
    out = reduce(lambda a, b: a.unionByName(b), rules)
    return out.select(
        F.current_timestamp().alias("run_ts"), "table_name", "rule_name", "rows_checked", "rows_failed",
        F.when(F.col("rows_checked") > 0,
               1 - F.col("rows_failed") / F.col("rows_checked")).alias("pass_rate"))


@dp.materialized_view(
    name="silver.dq_gate",
    comment="Fails the update when the unmatched-district rate exceeds its threshold.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("unmatched_rate_within_limit", "unmatched_rate <= max_unmatched_rate")
def dq_gate():
    return (spark.read.table("silver.dq_results")
            .filter("rule_name = 'district_key_mapped'")
            .select("table_name",
                    (1 - F.col("pass_rate")).alias("unmatched_rate"),
                    _case(F.col("table_name"), MAX_UNMATCHED).cast("double").alias("max_unmatched_rate"))
            .withColumn("gate_status",
                        F.when(F.col("unmatched_rate") <= F.col("max_unmatched_rate"), "PASS")
                         .otherwise("FAIL")))


def _gated(df):
    """Make a gold table depend on the DQ gate."""
    gate = (spark.read.table("silver.dq_gate")
            .filter("gate_status = 'PASS'").select(F.lit(1).alias("_g")).distinct())
    return df.crossJoin(gate).drop("_g")


# ======================================================================================
# GOLD
# ======================================================================================
@dp.materialized_view(
    name="gold.district_hospital_summary",
    comment="Supply side: hospital counts per district (zero-hospital districts included).",
    table_properties={"quality": "gold"},
)
def district_hospital_summary():
    dim = spark.read.table("silver.dim_district").select("district_key", "state_std", "district_std")
    h = spark.read.table("silver.hospital_clean")

    cnt = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
    agg = h.groupBy("district_key").agg(
        F.count("*").alias("total_hospitals"),
        cnt(F.col("hospital_category") == "PUBLIC").alias("public_hospitals"),
        cnt(F.col("hospital_category") == "PRIVATE").alias("private_hospitals"),
        cnt(F.col("hospital_category") == "OTHER").alias("other_category_hospitals"),
        cnt(F.col("medicine_group") == "ALLOPATHY").alias("allopathy_hospitals"),
        cnt(F.col("medicine_group") == "AYUSH").alias("ayush_hospitals"))

    by_care = (h.groupBy("district_key", "hospital_care_type").count()
                .groupBy("district_key")
                .agg(F.map_from_entries(F.collect_list(
                    F.struct("hospital_care_type", F.col("count").cast("int")))).alias("hospitals_by_care_type")))

    df = dim.join(agg, "district_key", "left").join(by_care, "district_key", "left")
    for c in ["total_hospitals", "public_hospitals", "private_hospitals",
              "other_category_hospitals", "allopathy_hospitals", "ayush_hospitals"]:
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0)).cast("int"))
    df = (df
          .withColumn("hospitals_by_care_type",
                      F.coalesce("hospitals_by_care_type", F.expr("cast(map() as map<string,int>)")))
          .withColumn("public_share",
                      F.when(F.col("total_hospitals") > 0, F.col("public_hospitals") / F.col("total_hospitals")))
          .withColumn("allopathy_share",
                      F.when(F.col("total_hospitals") > 0, F.col("allopathy_hospitals") / F.col("total_hospitals")))
          .withColumn("state_percentile_hospitals",
                      F.round(F.percent_rank().over(
                          Window.partitionBy("state_std").orderBy("total_hospitals")) * 100, 2))
          .withColumn("national_percentile_hospitals",
                      F.round(F.percent_rank().over(Window.orderBy("total_hospitals")) * 100, 2)))
    return _gated(df.select(
        "district_key", "state_std", "district_std", "total_hospitals", "public_hospitals",
        "private_hospitals", "other_category_hospitals", "allopathy_hospitals", "ayush_hospitals",
        "public_share", "allopathy_share", "state_percentile_hospitals",
        "national_percentile_hospitals", "hospitals_by_care_type"))


@dp.materialized_view(
    name="gold.district_health_kpi",
    comment="Wide table of headline health KPIs per district (NFHS-5).",
    table_properties={"quality": "gold"},
)
def district_health_kpi():
    dim = spark.read.table("silver.dim_district").select("district_key", "state_std", "district_std")
    ind = spark.read.table("silver.dim_indicator").filter("is_headline_kpi").select("indicator_code")
    fact = (spark.read.table("silver.fact_district_indicator")
            .filter("value_flag IN ('OK','LOW_SAMPLE')")
            .join(F.broadcast(ind), "indicator_code"))
    # pivot() is not supported in declarative pipelines, so use conditional aggregation
    wide = fact.groupBy("district_key").agg(
        *[F.max(F.when(F.col("indicator_code") == c, F.col("value_num"))).alias(c)
          for c in HEADLINE_KPIS])
    return _gated(dim.join(wide, "district_key", "left"))


@dp.temporary_view(name="indicator_zscores")
def indicator_zscores():
    """District x scored indicator, z-scored nationally and polarity-adjusted (higher = worse)."""
    ind = (spark.read.table("silver.dim_indicator").filter("use_in_score")
           .select("indicator_code", "theme", "polarity"))
    skeleton = spark.read.table("silver.dim_district").select("district_key").crossJoin(F.broadcast(ind))
    fact = (spark.read.table("silver.fact_district_indicator")
            .filter("value_flag IN ('OK','LOW_SAMPLE')")
            .select("district_key", "indicator_code", F.col("value_num").cast("double").alias("value_num")))
    s = skeleton.join(fact, ["district_key", "indicator_code"], "left")

    w = Window.partitionBy("indicator_code")
    s = (s.withColumn("mean", F.avg("value_num").over(w))
          .withColumn("std", F.stddev_pop("value_num").over(w))
          .withColumn("z", F.when(F.col("std") > 0, (F.col("value_num") - F.col("mean")) / F.col("std")))
          .withColumn("need_z", F.when(F.col("polarity") == "LOWER_BETTER", F.col("z")).otherwise(-F.col("z")))
          .withColumn("domain", _case(F.col("theme"), THEME_TO_DOMAIN))
          .withColumn("is_valid", F.col("need_z").isNotNull()))
    return s.select("district_key", "indicator_code", "domain", "value_num", "need_z", "is_valid")


@dp.materialized_view(
    name="gold.district_health_score",
    comment="Demand side: polarity-adjusted, nationally z-scored need score. Higher = worse.",
    table_properties={"quality": "gold"},
)
def district_health_score():
    dim = spark.read.table("silver.dim_district").select("district_key", "state_std", "district_std")
    z = spark.read.table("indicator_zscores")

    agg_cols = [F.avg(F.when(F.col("domain") == d, F.col("need_z"))).alias(f"{d}_need_score")
                for d in DOMAINS]
    scores = z.groupBy("district_key").agg(
        *agg_cols, F.avg(F.col("is_valid").cast("double")).alias("valid_indicator_share"))

    dom_cols = [f"{d}_need_score" for d in DOMAINS]
    num = reduce(lambda a, b: a + b, [F.coalesce(F.col(c), F.lit(0.0)) for c in dom_cols])
    den = reduce(lambda a, b: a + b, [F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in dom_cols])
    df = (dim.join(scores, "district_key", "left")
             .withColumn("composite_need_score", F.when(den > 0, num / den)))

    # percentiles and ranks only over districts that actually have a score
    scored = df.filter(F.col("composite_need_score").isNotNull())
    unscored = (df.filter(F.col("composite_need_score").isNull())
                  .withColumn("need_percentile", F.lit(None).cast("double"))
                  .withColumn("need_rank_in_state", F.lit(None).cast("int"))
                  .withColumn("need_rank_national", F.lit(None).cast("int")))
    scored = (scored
              .withColumn("need_percentile",
                          F.round(F.percent_rank().over(Window.orderBy("composite_need_score")) * 100, 2))
              .withColumn("need_rank_in_state",
                          F.rank().over(Window.partitionBy("state_std")
                                        .orderBy(F.col("composite_need_score").desc())).cast("int"))
              .withColumn("need_rank_national",
                          F.rank().over(Window.orderBy(F.col("composite_need_score").desc())).cast("int")))
    out = scored.unionByName(unscored)
    return _gated(out.select(
        "district_key", "state_std", "district_std", "maternal_need_score", "child_need_score",
        "nutrition_need_score", "ncd_need_score", "composite_need_score", "need_percentile",
        "need_rank_in_state", "need_rank_national", "valid_indicator_share"))


@dp.materialized_view(
    name="gold.district_service_gap",
    comment="Headline table: need percentile vs hospital-supply percentile with quadrant label.",
    table_properties={"quality": "gold"},
)
def district_service_gap():
    need = spark.read.table("gold.district_health_score").select(
        "district_key", "state_std", "district_std", "need_percentile", "valid_indicator_share")
    supply_col = "state_percentile_hospitals" if GAP_SCOPE == "state" else "national_percentile_hospitals"
    supply = spark.read.table("gold.district_hospital_summary").select(
        "district_key", F.col(supply_col).alias("supply_percentile"))

    zs = spark.read.table("indicator_zscores").filter("is_valid AND need_z > 0")
    w = Window.partitionBy("district_key").orderBy(F.col("need_z").desc())
    ranked = zs.withColumn("rn", F.row_number().over(w)).filter("rn <= 3")
    weak = ranked.groupBy("district_key").agg(
        *[F.max(F.when(F.col("rn") == i, F.col("indicator_code"))).alias(f"top_weak_indicator_{i}")
          for i in (1, 2, 3)])

    df = need.join(supply, "district_key", "left").join(weak, "district_key", "left")
    high_need = F.col("need_percentile") >= NEED_THRESHOLD
    low_supply = F.col("supply_percentile") < SUPPLY_THRESHOLD
    df = (df
          .withColumn("quadrant",
                      F.when(F.col("need_percentile").isNull(), "UNKNOWN")
                       .when(high_need & low_supply, "CRITICAL_GAP")
                       .when(high_need & ~low_supply, "PRESSURE")
                       .when(~high_need & low_supply, "MONITOR")
                       .otherwise("ADEQUATE"))
          .withColumn("gap_flag", F.col("quadrant") == "CRITICAL_GAP")
          .withColumn("confidence",
                      F.when(F.col("valid_indicator_share") >= 0.8, "HIGH")
                       .when(F.col("valid_indicator_share") >= 0.5, "MEDIUM")
                       .otherwise("LOW")))
    return df.select("district_key", "state_std", "district_std", "need_percentile",
                     "supply_percentile", "quadrant", "gap_flag", "confidence",
                     "top_weak_indicator_1", "top_weak_indicator_2", "top_weak_indicator_3")


@dp.materialized_view(
    name="gold.state_summary",
    comment="State-level rollup of supply, need and gap quadrants.",
    table_properties={"quality": "gold"},
)
def state_summary():
    gap = spark.read.table("gold.district_service_gap").select("district_key", "state_std", "quadrant")
    need = spark.read.table("gold.district_health_score").select("district_key", "composite_need_score")
    sup = spark.read.table("gold.district_hospital_summary").select(
        "district_key", "total_hospitals", "public_hospitals")
    kpi = spark.read.table("gold.district_health_kpi").select(
        "district_key", "institutional_births_pct", "fully_vaccinated_card_or_recall_pct", "stunted_u5_pct")

    df = (gap.join(need, "district_key", "left")
             .join(sup, "district_key", "left")
             .join(kpi, "district_key", "left"))
    q = lambda name: F.sum(F.when(F.col("quadrant") == name, 1).otherwise(0)).cast("int")
    out = df.groupBy("state_std").agg(
        F.count("*").cast("int").alias("n_districts"),
        F.sum("total_hospitals").cast("int").alias("total_hospitals"),
        (F.sum("public_hospitals") / F.sum("total_hospitals")).alias("public_share"),
        F.avg("composite_need_score").alias("avg_need_score"),
        F.avg("institutional_births_pct").alias("avg_institutional_births_pct"),
        F.avg("fully_vaccinated_card_or_recall_pct").alias("avg_full_immunization_pct"),
        F.avg("stunted_u5_pct").alias("avg_stunting_pct"),
        q("CRITICAL_GAP").alias("n_critical_gap"), q("PRESSURE").alias("n_pressure"),
        q("MONITOR").alias("n_monitor"), q("ADEQUATE").alias("n_adequate"))
    return out.withColumn("pct_districts_critical",
                          F.round(F.col("n_critical_gap") / F.col("n_districts") * 100, 2))