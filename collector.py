"""Binance USDS-M futures data collector.

This script fetches multiple public endpoints and stores them as CSV files. It is
intended to be executed by GitHub Actions, but it can also be invoked manually.
All timestamps in filenames and metadata are recorded in Asia/Seoul (KST).

투자 조언이 아니며, 연구 및 자동화 운영 목적으로만 사용하십시오.
"""
from __future__ import annotations

import logging
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable

import pandas as pd
import requests
from zoneinfo import ZoneInfo

# ==============================================================================
# 사용자 정의 가능 변수 (필요 시 상단에서 수정)
# ==============================================================================
SYMBOLS = ["ARBUSDT", "BTCUSDT", "ETHUSDT"]
INTERVAL = "15m"  # 선물 K라인 간격
DAYS = 30  # 최근 30일 기준 수집
OUT_DIR = "data"  # CSV 저장 디렉터리

OI_PERIOD = "1h"  # Open Interest statistics period
RATIO_PERIOD = "1h"  # Long/Short ratio 및 Taker Volume period
MAX_KLINE_LIMIT = 1500  # Binance 문서 상 Kline/Index/Mark/Premium 최대
MAX_STAT_LIMIT = 500  # openInterest, Long/Short, Taker Vol 등 최대 limit
MAX_RETRIES = 5  # 429/418 대응 재시도 횟수 한도

BASE_URL = "https://fapi.binance.com"
SESSION = requests.Session()

KST = ZoneInfo("Asia/Seoul")


@dataclass
class FetchStats:
    """단순 수집 통계."""

    success: int = 0
    skipped: int = 0


def now_kst() -> datetime:
    """현재 시간을 KST로 반환."""

    return datetime.now(tz=KST)


def to_ms(dt: datetime) -> int:
    """datetime을 밀리초 단위 epoch로 변환."""

    return int(dt.timestamp() * 1000)


def jitter_delay(base_delay: float) -> float:
    """랜덤 지터를 포함한 백오프 지연."""

    return base_delay + random.uniform(0, 1)


def _get(path: str, params: dict[str, Any] | None = None) -> Any:
    """공통 GET 요청 헬퍼.

    - 200 OK 시 JSON 반환
    - 429/418: Retry-After 헤더 준수 및 지터 백오프, 최대 MAX_RETRIES
    - 400: 잘못된 파라미터로 판단하고 로그 후 None 반환
    - 기타 오류는 예외를 발생시켜 상위에서 중단 여부 결정
    """

    url = f"{BASE_URL}{path}"
    attempts = 0
    backoff = 5.0
    while True:
        attempts += 1
        try:
            response = SESSION.get(url, params=params, timeout=15)
        except requests.RequestException as exc:  # 네트워크 예외는 재시도
            if attempts > MAX_RETRIES:
                logging.error("Request failed after retries: %s %s", url, exc)
                raise
            logging.warning("Request error (%s) -> retrying #%d", exc, attempts)
            time.sleep(jitter_delay(backoff))
            backoff *= 2
            continue

        if response.status_code == 200:
            return response.json()

        if response.status_code in {429, 418}:
            if attempts > MAX_RETRIES:
                logging.error("Exceeded retries for %s with params=%s", url, params)
                raise RuntimeError(f"Rate limited for {url}")
            retry_after = response.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else backoff
            logging.warning(
                "Rate limit (%s) encountered. Sleeping for %.2fs (attempt %d/%d)",
                response.status_code,
                delay,
                attempts,
                MAX_RETRIES,
            )
            time.sleep(jitter_delay(delay))
            backoff = min(backoff * 2, 60)
            continue

        if response.status_code == 400:
            logging.error(
                "Bad request %s params=%s -> %s", url, params, response.text
            )
            return None

        response.raise_for_status()


def ensure_dir(path: Path) -> None:
    """경로의 부모 디렉터리를 생성."""

    path.parent.mkdir(parents=True, exist_ok=True)


def write_with_metadata(path: Path, frame: pd.DataFrame) -> None:
    """CSV 파일을 작성하면서 첫 줄에 수집 시각 메타 정보를 추가."""

    ensure_dir(path)
    collected_at = now_kst().strftime("%Y-%m-%d %H:%M:%S %Z")
    with path.open("w", encoding="utf-8") as handle:
        handle.write(f"# collected_at_kst,{collected_at}\n")
    frame.to_csv(path, mode="a", index=False)
    logging.info("Saved %s (%d rows)", path, len(frame))


def paginate_fetch(
    *,
    path: str,
    symbol: str,
    limit: int,
    earliest_ms: int,
    build_params: Callable[[dict[str, Any]], None],
    extract_timestamp: Callable[[Any], int],
) -> list[Any]:
    """공통 페이지네이션 유틸리티 (과거→최신 순 정렬).

    Binance 응답은 기본적으로 과거→최신 순으로 정렬되어 있으며, endTime을 줄 경우
    해당 시점 이전의 데이터를 반환한다. earliest_ms 이전의 데이터는 제외한다.
    """

    end_time: int | None = None
    all_rows: list[Any] = []

    while True:
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if end_time is not None:
            params["endTime"] = end_time
        build_params(params)
        payload = _get(path, params)
        if not payload:
            break
        if isinstance(payload, dict) and "code" in payload:
            logging.error("API error for %s: %s", path, payload)
            break
        if not isinstance(payload, list) or not payload:
            break

        valid_rows = [row for row in payload if extract_timestamp(row) >= earliest_ms]
        all_rows.extend(valid_rows)

        first_ts = extract_timestamp(payload[0])
        if len(payload) == limit and first_ts > earliest_ms:
            end_time = first_ts - 1
            continue
        break

    # 정렬 및 중복 제거
    unique_map: dict[int, Any] = {}
    for row in all_rows:
        unique_map[extract_timestamp(row)] = row
    ordered = [unique_map[key] for key in sorted(unique_map.keys())]
    return ordered


def fetch_klines(symbol: str, interval: str, days: int) -> pd.DataFrame:
    earliest_ms = to_ms(now_kst() - pd.Timedelta(days=days))
    rows = paginate_fetch(
        path="/fapi/v1/klines",
        symbol=symbol,
        limit=MAX_KLINE_LIMIT,
        earliest_ms=earliest_ms,
        build_params=lambda p: p.update({"interval": interval}),
        extract_timestamp=lambda row: int(row[0]),
    )
    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "trades",
        "taker_base_volume",
        "taker_quote_volume",
        "ignore",
    ]
    frame = pd.DataFrame(rows, columns=columns)
    frame = frame[frame["open_time"] >= earliest_ms]
    return frame


def fetch_index_like(symbol: str, endpoint: str, interval: str, days: int) -> pd.DataFrame:
    earliest_ms = to_ms(now_kst() - pd.Timedelta(days=days))
    rows = paginate_fetch(
        path=endpoint,
        symbol=symbol,
        limit=MAX_KLINE_LIMIT,
        earliest_ms=earliest_ms,
        build_params=lambda p: p.update({"interval": interval}),
        extract_timestamp=lambda row: int(row[0]),
    )
    columns = [
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
    frame = pd.DataFrame(rows, columns=columns)
    frame = frame[frame["open_time"] >= earliest_ms]
    return frame


def fetch_stat_series(
    symbol: str,
    endpoint: str,
    *,
    period: str | None,
    days: int,
) -> pd.DataFrame:
    earliest_ms = to_ms(now_kst() - pd.Timedelta(days=days))

    def build(params: dict[str, Any]) -> None:
        if period is not None:
            params["period"] = period

    rows = paginate_fetch(
        path=endpoint,
        symbol=symbol,
        limit=MAX_STAT_LIMIT,
        earliest_ms=earliest_ms,
        build_params=build,
        extract_timestamp=lambda row: int(row["timestamp"]),
    )
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = frame.sort_values("timestamp")
    return frame


def fetch_funding(symbol: str, days: int) -> pd.DataFrame:
    earliest_ms = to_ms(now_kst() - pd.Timedelta(days=days))

    def build(params: dict[str, Any]) -> None:
        params["startTime"] = earliest_ms

    rows = paginate_fetch(
        path="/fapi/v1/fundingRate",
        symbol=symbol,
        limit=MAX_STAT_LIMIT,
        earliest_ms=earliest_ms,
        build_params=build,
        extract_timestamp=lambda row: int(row["fundingTime"]),
    )
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = frame.sort_values("fundingTime")
    return frame


def collect_symbol(symbol: str, out_dir: Path, stats: FetchStats) -> None:
    logging.info("Collecting data for %s", symbol)

    tasks: Iterable[tuple[str, Callable[[], pd.DataFrame]]] = [
        (
            f"{symbol}_klines_{INTERVAL}_{DAYS}d.csv",
            lambda: fetch_klines(symbol, INTERVAL, DAYS),
        ),
        (
            f"{symbol}_oi_{OI_PERIOD}_{DAYS}d.csv",
            lambda: fetch_stat_series(
                symbol,
                "/futures/data/openInterestHist",
                period=OI_PERIOD,
                days=DAYS,
            ),
        ),
        (
            f"{symbol}_funding_{DAYS}d.csv",
            lambda: fetch_funding(symbol, DAYS),
        ),
        (
            f"{symbol}_global_ls_{DAYS}d.csv",
            lambda: fetch_stat_series(
                symbol,
                "/futures/data/globalLongShortAccountRatio",
                period=RATIO_PERIOD,
                days=DAYS,
            ),
        ),
        (
            f"{symbol}_top_ls_accounts_{DAYS}d.csv",
            lambda: fetch_stat_series(
                symbol,
                "/futures/data/topLongShortAccountRatio",
                period=RATIO_PERIOD,
                days=DAYS,
            ),
        ),
        (
            f"{symbol}_top_ls_positions_{DAYS}d.csv",
            lambda: fetch_stat_series(
                symbol,
                "/futures/data/topLongShortPositionRatio",
                period=RATIO_PERIOD,
                days=DAYS,
            ),
        ),
        (
            f"{symbol}_taker_vol_{DAYS}d.csv",
            lambda: fetch_stat_series(
                symbol,
                "/futures/data/takerBuySellVol",
                period=RATIO_PERIOD,
                days=DAYS,
            ),
        ),
        (
            f"{symbol}_index_{INTERVAL}_{DAYS}d.csv",
            lambda: fetch_index_like(
                symbol, "/fapi/v1/indexPriceKlines", INTERVAL, DAYS
            ),
        ),
        (
            f"{symbol}_mark_{INTERVAL}_{DAYS}d.csv",
            lambda: fetch_index_like(
                symbol, "/fapi/v1/markPriceKlines", INTERVAL, DAYS
            ),
        ),
        (
            f"{symbol}_premium_{INTERVAL}_{DAYS}d.csv",
            lambda: fetch_index_like(
                symbol, "/fapi/v1/premiumIndexKlines", INTERVAL, DAYS
            ),
        ),
    ]

    for filename, loader in tasks:
        path = out_dir / filename
        try:
            frame = loader()
        except Exception:  # noqa: BLE001 - 상위에서 로깅 후 계속 진행
            stats.skipped += 1
            logging.exception("Failed to collect %s for %s", filename, symbol)
            continue

        if frame.empty:
            stats.skipped += 1
            logging.warning("No data returned for %s", filename)
            continue

        write_with_metadata(path, frame)
        stats.success += 1


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> int:
    configure_logging()
    stats = FetchStats()
    out_dir = Path(OUT_DIR)
    logging.info(
        "Starting collection for symbols=%s interval=%s days=%d",
        ",".join(SYMBOLS),
        INTERVAL,
        DAYS,
    )

    for symbol in SYMBOLS:
        collect_symbol(symbol, out_dir, stats)

    logging.info(
        "Collection finished. Success=%d skipped=%d",
        stats.success,
        stats.skipped,
    )
    return 0 if stats.success > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
