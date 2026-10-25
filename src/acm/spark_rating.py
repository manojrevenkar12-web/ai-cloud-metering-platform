"""Rating on Spark, for backfills and for a fleet DuckDB on one machine cannot hold.

The step that grows with fleet size times time is rating: every billable session is expanded to
the minutes it covers, summed per customer, GPU type, region and minute, and netted against the
GPUs the customer has committed for that minute. At this repository's volume DuckDB does it in
under a second (models/10_gold_gpu_minutes.sql); this module is the same rule in PySpark, run on
the silver tables exported as Parquet, and `compare()` checks the two engines agree to the cent.

    acm spark-rate --root data/replay --months 2026-09 2026-10

Silver is exported by DuckDB (`export_silver`), so the de-duplication and session rules have one
implementation; only rating is duplicated, deliberately, and the test suite keeps the two equal.
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb

from acm.transform import month_bounds

SILVER_EXPORT = ["silver_vm_sessions", "silver_commitment_terms", "silver_inference", "silver_storage", "ref_prices"]


def export_silver(con: duckdb.DuckDBPyConnection, out: Path) -> Path:
    out.mkdir(parents=True, exist_ok=True)
    for t in SILVER_EXPORT:
        con.execute(f"COPY {t} TO '{out / t}.parquet' (FORMAT parquet)")
    return out


def session(app: str = "acm-rating", cores: int = 2):
    """A local Spark session; on a cluster, spark-submit provides the master instead."""
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", os.environ.get("PYTHON", "python3"))
    return (SparkSession.builder.master(f"local[{cores}]").appName(app)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", str(cores * 4))
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())


def gpu_minutes(spark, silver: Path, month: str, month_end: int):
    """Equivalent of models/10_gold_gpu_minutes.sql for one month."""
    from pyspark.sql import functions as F

    a0, _ = month_bounds(month)
    sessions = spark.read.parquet(str(silver / "silver_vm_sessions.parquet"))
    terms = spark.read.parquet(str(silver / "silver_commitment_terms.parquet"))
    prices = spark.read.parquet(str(silver / "ref_prices.parquet"))

    s = (sessions.where((F.col("started_at") < month_end) & (F.col("billed_until") > a0))
         .select("customer_id", "gpu_type", "region", "gpu_count",
                 F.greatest("started_at", F.lit(a0)).alias("a"), F.least("billed_until", F.lit(month_end)).alias("b"))
         .where(F.col("b") > F.col("a")))
    minutes = s.withColumn("minute", F.explode(F.sequence(F.floor(F.col("a") / 60).cast("long"),
                                                          F.floor((F.col("b") - 1) / 60).cast("long"))))
    usage = (minutes.withColumn("secs", (F.least("b", (F.col("minute") + 1) * 60) - F.greatest("a", F.col("minute") * 60))
                                * F.col("gpu_count"))
             .groupBy("customer_id", "gpu_type", "region", "minute").agg(F.sum("secs").alias("gpu_seconds")))
    u, c = usage.alias("u"), F.broadcast(terms).alias("c")
    committed = (u.join(c, (F.col("u.customer_id") == F.col("c.customer_id")) & (F.col("u.gpu_type") == F.col("c.gpu_type"))
                        & (F.col("u.region") == F.col("c.region")) & (F.col("u.minute") * 60 >= F.col("c.starts"))
                        & (F.col("u.minute") * 60 < F.col("c.ends")), "left")
                 .groupBy("u.customer_id", "u.gpu_type", "u.region", "u.minute", "u.gpu_seconds")
                 .agg((F.coalesce(F.sum("c.gpus"), F.lit(0)) * 60).alias("committed_seconds")))
    p = F.broadcast(prices.where(F.col("sku").startswith("gpu:"))).alias("p")
    k = committed.alias("k")
    rated = k.join(p, (F.col("p.sku") == F.concat(F.lit("gpu:"), F.col("k.gpu_type"), F.lit(":"), F.col("k.region")))
                   & (F.col("k.minute") * 60 >= F.col("p.valid_from")) & (F.col("k.minute") * 60 < F.col("p.valid_to")), "left")
    return rated.select(
        F.lit(month).alias("month"), "k.customer_id", "k.gpu_type", "k.region", "k.minute", "k.gpu_seconds",
        "k.committed_seconds",
        F.greatest(F.lit(0), F.col("k.gpu_seconds") - F.col("k.committed_seconds")).alias("overage_seconds"),
        F.col("p.usd").alias("usd_per_gpu_hour"))


def charges(spark, silver: Path, month: str, month_end: int):
    """Equivalent of models/11_gold_charges.sql for one month (before credits)."""
    from pyspark.sql import functions as F

    a0, _ = month_bounds(month)
    m = gpu_minutes(spark, silver, month, month_end)
    gpu = m.groupBy("customer_id").agg(
        F.sum(F.col("overage_seconds") / 3600.0 * F.col("usd_per_gpu_hour")).alias("gpu_on_demand"),
        F.sum("gpu_seconds").alias("gpu_seconds"))
    terms = spark.read.parquet(str(silver / "silver_commitment_terms.parquet"))
    commit = (terms.where((F.col("starts") < month_end) & (F.col("ends") > a0)).groupBy("customer_id")
              .agg(F.sum(F.col("gpus") * (F.least("ends", F.lit(month_end)) - F.greatest("starts", F.lit(a0))) / 3600.0
                         * F.col("usd_per_gpu_hour")).alias("commitment")))
    prices = spark.read.parquet(str(silver / "ref_prices.parquet"))
    inf = spark.read.parquet(str(silver / "silver_inference.parquet")).where((F.col("window_ts") >= a0) & (F.col("window_ts") < month_end))
    pi, po = F.broadcast(prices).alias("pi"), F.broadcast(prices).alias("po")
    i = inf.alias("i")
    inference = (i.join(pi, (F.col("pi.sku") == F.concat(F.lit("inference:"), F.col("i.model"), F.lit(":input")))
                        & (F.col("i.window_ts") >= F.col("pi.valid_from")) & (F.col("i.window_ts") < F.col("pi.valid_to")))
                 .join(po, (F.col("po.sku") == F.concat(F.lit("inference:"), F.col("i.model"), F.lit(":output")))
                       & (F.col("i.window_ts") >= F.col("po.valid_from")) & (F.col("i.window_ts") < F.col("po.valid_to")))
                 .groupBy("i.customer_id")
                 .agg(F.sum(F.col("i.input_tokens") / 1e6 * F.col("pi.usd") + F.col("i.output_tokens") / 1e6 * F.col("po.usd"))
                      .alias("inference")))
    sto = spark.read.parquet(str(silver / "silver_storage.parquet")).where((F.col("snapshot_ts") >= a0) & (F.col("snapshot_ts") < month_end))
    ps = F.broadcast(prices.where(F.col("sku") == "storage:object")).alias("ps")
    storage = (sto.alias("s").join(ps, (F.col("s.snapshot_ts") >= F.col("ps.valid_from")) & (F.col("s.snapshot_ts") < F.col("ps.valid_to")))
               .groupBy("s.customer_id").agg(F.sum(F.col("s.bytes_stored") / 1e9 * F.col("ps.usd") / 730).alias("storage")))
    out = (gpu.join(commit, "customer_id", "full").join(inference, "customer_id", "full").join(storage, "customer_id", "full")
           .na.fill(0.0, ["gpu_on_demand", "commitment", "inference", "storage"]))
    return out.select(F.lit(month).alias("month"), "customer_id", "gpu_on_demand", "commitment", "inference", "storage")


def compare(con: duckdb.DuckDBPyConnection, spark, silver: Path, months: list[str]) -> dict:
    """Runs Spark rating over the same span DuckDB last rated each month (gold_runs) and returns
    the largest per customer, month and component difference to gold_charges, in dollars."""
    worst, rows = 0.0, 0
    ends = dict(con.execute("SELECT month, month_end FROM gold_runs").fetchall())
    for month in months:
        theirs = {r["customer_id"]: r for r in charges(spark, silver, month, ends[month]).collect()}
        ours = {r[0]: r[1:] for r in con.execute("""SELECT customer_id, gpu_on_demand, commitment, inference, storage
                                                    FROM gold_charges WHERE month = ?""", [month]).fetchall()}
        for cid in set(theirs) | set(ours):
            o = ours.get(cid, (0.0, 0.0, 0.0, 0.0))
            t = theirs.get(cid)
            tv = (t["gpu_on_demand"], t["commitment"], t["inference"], t["storage"]) if t else (0.0, 0.0, 0.0, 0.0)
            worst = max(worst, *(abs(round(x, 2) - round(y, 2)) for x, y in zip(o, tv)))
            rows += 1
    return {"months": months, "customer_months": rows, "max_difference_usd": round(worst, 2)}
