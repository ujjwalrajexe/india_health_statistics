"""Silver tables for the hospital directory and the NFHS district fact sheet.

District matching, hospital cleaning, the indicator fact, and the data-quality
gate all live here because each one uses both sources.
"""
import sys
from functools import reduce
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import (
    INDICATOR_TABLES,
    alnum_upper,
    case_map,
    district_norm,
    dp,
    fuzzy_threshold,
    indicator_rows,
    max_unmatched,
    normalised_pairs,
    spark_session,
    state_norm,
)


@dp.materialized_view(
    name="silver.dim_district",
    comment="One row per district. NFHS is the master list of districts.",
    table_properties={"quality": "silver"},
)
def dim_district():
    spark = spark_session()
    src = (spark.read.table("bronze.nfhs_survey_housing")
           .select("state_ut_raw", "district_name_raw")
           .filter(F.col("state_ut_raw").isNotNull() & F.col("district_name_raw").isNotNull())
           .distinct())
    df = (src
          .withColumn("state_std", F.initcap(F.trim("state_ut_raw")))
          .withColumn("district_std", F.initcap(F.regexp_replace(F.trim("district_name_raw"), r"\s+", " ")))
          .withColumn("district_norm", district_norm(F.col("district_std")))
          .withColumn("district_key", F.xxhash64(state_norm(F.col("state_std")), F.col("district_norm")))
          .dropDuplicates(["district_key"]))
    return df.select("district_key", "state_std", "district_std", "district_norm")


@dp.materialized_view(
    name="silver.bridge_district_name_map",
    comment="Maps every raw (state, district) spelling to a district_key.",
    table_properties={"quality": "silver"},
)
def bridge_district_name_map():
    spark = spark_session()
    dim = spark.read.table("silver.dim_district")
    d = dim.select(F.col("district_key").alias("dk"), "district_std",
                   state_norm(F.col("state_std")).alias("d_snorm"),
                   F.col("district_norm").alias("d_dnorm"))

    nfhs = normalised_pairs(spark.read.table("bronze.nfhs_survey_housing"),
                            "NFHS", "state_ut_raw", "district_name_raw")
    hosp = normalised_pairs(spark.read.table("bronze.hospital_directory_raw"),
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
        F.when(F.col("score") >= fuzzy_threshold(), F.col("dk")).alias("district_key"),
        F.when(F.col("score") >= fuzzy_threshold(), "FUZZY").otherwise("UNMATCHED").alias("match_method"),
        F.col("score").alias("match_score"),
        F.col("district_std").alias("closest_district_std"))

    return matched.unionByName(fuzzy)


@dp.temporary_view(name="hospital_staged")
def hospital_staged():
    """All hospital rows with district_key, standardised columns and DQ flags (nothing dropped)."""
    spark = spark_session()
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
                      F.xxhash64(alnum_upper(F.col("hospital_name")),
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
    return (spark_session().read.table("hospital_staged")
            .filter(~F.col("is_duplicate"))
            .select("hospital_id", "hospital_name", "hospital_category", "hospital_care_type",
                    "discipline", "medicine_group", "address_line", "subdistrict", "pincode",
                    "latitude", "longitude", "coords_valid", "district_key"))


@dp.materialized_view(
    name="silver.dim_indicator",
    comment="Indicator metadata that drives generic scoring.",
    table_properties={"quality": "silver"},
)
def dim_indicator():
    return spark_session().createDataFrame(
        indicator_rows(),
        "indicator_code string, indicator_name string, theme string, unit string, "
        "polarity string, use_in_score boolean, is_headline_kpi boolean")


@dp.temporary_view(name="fact_staged")
def fact_staged():
    """Unpivot all 8 bronze health tables and parse survey flags. Nothing is dropped."""
    spark = spark_session()

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
    return (spark_session().read.table("fact_staged")
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
    spark = spark_session()
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
    return (spark_session().read.table("silver.dq_results")
            .filter("rule_name = 'district_key_mapped'")
            .select("table_name",
                    (1 - F.col("pass_rate")).alias("unmatched_rate"),
                    case_map(F.col("table_name"), max_unmatched()).cast("double").alias("max_unmatched_rate"))
            .withColumn("gate_status",
                        F.when(F.col("unmatched_rate") <= F.col("max_unmatched_rate"), "PASS")
                         .otherwise("FAIL")))
