from acm.pricing import lookup, price_rows, ts


def test_effective_dated_prices():
    prices = price_rows()
    assert lookup(prices, "gpu:h100:eu-north1", ts("2026-09-30T23:59:59Z")).usd == 2.95
    assert lookup(prices, "gpu:h100:eu-north1", ts("2026-10-01T00:00:00Z")).usd == 2.65
    assert lookup(prices, "inference:llama-70b:input", ts("2026-10-08T00:00:00Z")).usd == 0.10


def test_price_rows_do_not_overlap():
    by_sku = {}
    for p in price_rows():
        by_sku.setdefault(p.sku, []).append(p)
    for sku, rows in by_sku.items():
        rows.sort(key=lambda p: p.valid_from)
        for a, b in zip(rows, rows[1:]):
            assert a.valid_to == b.valid_from, sku


def test_b200_has_no_price_before_launch():
    assert lookup(price_rows(), "gpu:b200:eu-north1", ts("2026-09-10T00:00:00Z")) is None
