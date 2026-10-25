import duckdb

from acm.cli import main


def test_run_erase_and_lineage_from_the_command_line(small_lake_copy, capsys):
    root = str(small_lake_copy.root)
    assert main(["run", "--root", root, "--now", "2026-10-05T12:00:00Z"]) == 0
    con = duckdb.connect(str(small_lake_copy.warehouse), read_only=True)
    email = con.execute("SELECT api_key_owner FROM silver_inference LIMIT 1").fetchone()[0]
    con.close()
    assert main(["erase", "--root", root, "--email", email, "--request-id", "DSR-9", "--now", "2026-10-05T12:00:00Z"]) == 0
    capsys.readouterr()
    assert main(["lineage", "--root", root, "--trace", "mart_finance_revenue.inference"]) == 0
    traced = capsys.readouterr().out.split()
    assert "bronze_inference_usage.input_tokens" in traced and "ref_prices.usd" in traced
