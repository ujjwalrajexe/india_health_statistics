"""Gold KPIs, need scores, hospital supply, service-gap quadrants, and the state rollup."""
import sys
from functools import reduce
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import (
    DOMAINS,
    HEADLINE_KPIS,
    THEME_TO_DOMAIN,
    case_map,
    dp,
    gap_scope,
    need_threshold,
    spark_session,
    supply_threshold,
)


def _gated(df):
    """Make a gold table depend on the DQ gate."""
    gate = (spark_session().read.table("silver.dq_gate")
            .filter("gate_status = 'PASS'").select(F.lit(1).alias("_g")).distinct())
    return df.crossJoin(gate).drop("_g")


@dp.materialized_view(
    name="gold.district_hospital_summary",
    comment="Supply side: hospital counts per district (zero-hospital districts included).",
    table_properties={"quality": "gold"},
)
def district_hospital_summary():
    spark = spark_session()
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
    spark = spark_session()
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
    spark = spark_session()
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
          .withColumn("domain", case_map(F.col("theme"), THEME_TO_DOMAIN))
          .withColumn("is_valid", F.col("need_z").isNotNull()))
    return s.select("district_key", "indicator_code", "domain", "value_num", "need_z", "is_valid")


@dp.materialized_view(
    name="gold.district_health_score",
    comment="Demand side: polarity-adjusted, nationally z-scored need score. Higher = worse.",
    table_properties={"quality": "gold"},
)
def district_health_score():
    spark = spark_session()
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
    spark = spark_session()
    need = spark.read.table("gold.district_health_score").select(
        "district_key", "state_std", "district_std", "need_percentile", "valid_indicator_share")
    supply_col = "state_percentile_hospitals" if gap_scope() == "state" else "national_percentile_hospitals"
    supply = spark.read.table("gold.district_hospital_summary").select(
        "district_key", F.col(supply_col).alias("supply_percentile"))

    zs = spark.read.table("indicator_zscores").filter("is_valid AND need_z > 0")
    w = Window.partitionBy("district_key").orderBy(F.col("need_z").desc())
    ranked = zs.withColumn("rn", F.row_number().over(w)).filter("rn <= 3")
    weak = ranked.groupBy("district_key").agg(
        *[F.max(F.when(F.col("rn") == i, F.col("indicator_code"))).alias(f"top_weak_indicator_{i}")
          for i in (1, 2, 3)])

    df = need.join(supply, "district_key", "left").join(weak, "district_key", "left")
    high_need = F.col("need_percentile") >= need_threshold()
    low_supply = F.col("supply_percentile") < supply_threshold()
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
    spark = spark_session()
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
