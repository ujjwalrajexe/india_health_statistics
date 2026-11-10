"""Shared configuration, NFHS column metadata, and column helpers.

Dataset definitions live in the bronze, silver, and gold modules. This module
does not declare pipeline datasets.
"""
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from pyspark import pipelines as dp          # Lakeflow Spark Declarative Pipelines
except ImportError:                               # older runtimes: import dlt
    import dlt as dp
    if not hasattr(dp, "materialized_view"):
        dp.materialized_view = dp.table
        dp.temporary_view = dp.view


def spark_session():
    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError("No active Spark session.")
    return session


def landing_path():
    return spark_session().conf.get("source")


def hospital_path():
    return f"{landing_path()}/hospitals_directory"


def nfhs_path():
    return f"{landing_path()}/health_statistic"


def fuzzy_threshold():
    return float(spark_session().conf.get("fuzzy_threshold", "0.85"))


def max_unmatched():
    spark = spark_session()
    return {
        "silver.fact_district_indicator": float(spark.conf.get("max_unmatched_health", "0.02")),
        "silver.hospital_clean": float(spark.conf.get("max_unmatched_hospital", "0.30")),
    }


def need_threshold():
    return float(spark_session().conf.get("need_threshold", "50"))


def supply_threshold():
    return float(spark_session().conf.get("supply_threshold", "50"))


def gap_scope():
    return spark_session().conf.get("gap_scope", "national")


# --------------------------------------------------------------------------------------
# NFHS column metadata
# The NFHS file is renamed POSITIONALLY (headers contain footnote digits and odd chars),
# so the order below must match the column order of the source file.
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


def case_map(col, mapping):
    """CASE WHEN col = k THEN v ... (NULL when nothing matches)."""
    expr = None
    for k, v in mapping.items():
        expr = F.when(col == k, F.lit(v)) if expr is None else expr.when(col == k, F.lit(v))
    return expr if expr is not None else F.lit(None).cast("string")


def alnum_upper(col):
    c = F.upper(F.trim(col))
    c = F.regexp_replace(c, r"&", " AND ")
    c = F.regexp_replace(c, r"\b(DISTRICT|DIST)\b", " ")
    return F.regexp_replace(c, r"[^A-Z0-9]", "")


def state_norm(col):
    s = alnum_upper(col)
    return F.coalesce(case_map(s, STATE_ALIASES), s)


def district_norm(col):
    return alnum_upper(col)


def snake(name):
    return re.sub(r"[^0-9a-z]+", "_", name.strip().lower()).strip("_")


def autoloader(path):
    return (spark_session().readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "false")     # keep everything STRING
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("header", "true")
            .option("multiLine", "true")
            .option("quote", '"')
            .option("escape", '"')
            .load(path))


def audit(df):
    return (df.withColumn("_ingest_ts", F.current_timestamp())
              .withColumn("_source_file", F.col("_metadata.file_path")))


def nfhs_theme(table):
    """Project one NFHS theme from the shared bronze stream."""
    codes = BRONZE_NFHS_TABLES[table]
    return (spark_session().readStream.table("nfhs_raw")
            .select("district_name_raw", "state_ut_raw", *codes, "_ingest_ts", "_source_file"))


def indicator_rows():
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


def normalised_pairs(df, source_system, state_col, district_col):
    p = (df.select(F.lit(source_system).alias("source_system"),
                   F.col(state_col).alias("state_raw"),
                   F.col(district_col).alias("district_raw"))
           .filter(F.col("state_raw").isNotNull() & F.col("district_raw").isNotNull())
           .distinct()
           .withColumn("s_norm", state_norm(F.col("state_raw")))
           .withColumn("d_norm0", district_norm(F.col("district_raw")))
           .withColumn("override", case_map(F.concat_ws("|", "s_norm", "d_norm0"), DISTRICT_OVERRIDES))
           .withColumn("s_norm", F.coalesce(F.split("override", r"\|")[0], F.col("s_norm")))
           .withColumn("d_norm", F.coalesce(F.split("override", r"\|")[1], F.col("d_norm0")))
           .withColumn("overridden", F.col("override").isNotNull()))
    return p.drop("d_norm0", "override")
