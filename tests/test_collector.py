from __future__ import annotations

from typing import Any

import pytest

pd = pytest.importorskip("pandas")

import collector


def test_paginate_fetch_uses_custom_symbol_param(monkeypatch):
    captured: dict[str, Any] = {}

    def fake_get(path: str, params: dict[str, Any]) -> list[list[int]]:
        captured["path"] = path
        captured["params"] = params.copy()
        return [[0]]

    monkeypatch.setattr(collector, "_get", fake_get)

    rows = collector.paginate_fetch(
        path="/example",
        symbol="BTCUSDT",
        limit=1,
        earliest_ms=0,
        build_params=lambda params: None,
        extract_timestamp=lambda row: int(row[0]),
        symbol_param="pair",
    )

    assert rows == [[0]]
    assert captured["path"] == "/example"
    assert captured["params"]["pair"] == "BTCUSDT"
    assert "symbol" not in captured["params"]


def test_fetch_index_like_forwards_symbol_param(monkeypatch):
    expected_rows = [[0, 1, 2, 3, 4, 5, 6, 7, 8]]

    def fake_paginate_fetch(**kwargs: Any) -> list[list[int]]:
        assert kwargs["symbol_param"] == "pair"
        build_params = kwargs["build_params"]
        params: dict[str, Any] = {}
        build_params(params)
        assert params == {"interval": "1m"}
        return expected_rows

    monkeypatch.setattr(collector, "paginate_fetch", fake_paginate_fetch)

    frame = collector.fetch_index_like(
        "BTCUSDT",
        "/fapi/v1/indexPriceKlines",
        "1m",
        1,
        symbol_param="pair",
    )

    assert isinstance(frame, pd.DataFrame)
    assert frame.iloc[0]["open_time"] == 0
    assert list(frame.columns) == [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "base_asset_volume",
        "trades",
    ]
