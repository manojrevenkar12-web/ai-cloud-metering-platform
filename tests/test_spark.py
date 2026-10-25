"""Rating on Spark agrees with DuckDB to the cent (needs pyspark and a JVM)."""
import shutil

import duckdb
import pytest

pyspark = pytest.importorskip("pyspark")
pytestmark = [pytest.mark.slow, pytest.mark.skipif(not shutil.which("java"), reason="no JVM")]


def test_spark_rating_equals_duckdb(small_lake, tmp_path):
    from acm import spark_rating
    con = duckdb.connect(str(small_lake.warehouse), read_only=True)
    silver = spark_rating.export_silver(con, tmp_path / "silver")
    spark = spark_rating.session(cores=1)
    try:
        result = spark_rating.compare(con, spark, silver, ["2026-09", "2026-10"])
    finally:
        spark.stop()
        con.close()
    assert result["customer_months"] >= 8 and result["max_difference_usd"] == 0.0
