from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class BestChoiceVolumeScraper:
    tickers: list[str]
    tipo: str = "stock"  # stock | dr

    def _payload_for_ticker(self, ticker: str) -> dict[str, Any]:
        # Query each ticker directly. This avoids the strong filters used in the UI
        # that can exclude tickers and result in empty output.
        return {
            "columns": [
                "name",
                "description",
                "type",
                "exchange",
                "close",
                "change",
                "volume",
                "volume_change",
                "average_volume_30d_calc",
                "average_volume_10d_calc",
                "recommendation_mark",
                "net_margin_fy",
                "dividends_yield_current",
            ],
            "filter": [
                {"left": "type", "operation": "equal", "right": self.tipo},
                {"left": "name", "operation": "equal", "right": ticker},
            ],
            "options": {"lang": "pt"},
            "range": [0, 1],
            "sort": {"sortBy": "name", "sortOrder": "asc"},
            "symbols": {},
            "markets": ["brazil"],
        }

    @staticmethod
    def _to_int(v: Any) -> int | None:
        try:
            if v is None or v == "":
                return None
            return int(round(float(v)))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _to_float(v: Any) -> float | None:
        try:
            if v is None or v == "":
                return None
            return float(v)
        except (ValueError, TypeError):
            return None

    def _fetch_rows(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        url = "https://bestchoice-serverless.netlify.app/.netlify/functions/post"
        headers = {
            "Content-Type": "application/json",
            "x-target-url": "https://scanner.tradingview.com/brazil/scan",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        }

        resp = requests.post(url, json=payload, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json() or {}
        return data.get("data", []) or []

    def scrape(self) -> list[dict[str, Any]]:
        wanted = {str(t or "").strip().upper() for t in self.tickers}
        wanted.discard("")
        if not wanted:
            return []

        items: list[dict[str, Any]] = []
        for ticker in sorted(wanted):
            rows = self._fetch_rows(self._payload_for_ticker(ticker))
            if not rows:
                continue

            row = rows[0]
            d = row.get("d") or []
            if not d or len(d) < 13:
                continue

            # Columns order (see _payload_for_ticker)
            name = str(d[0] or "").strip().upper()
            if name != ticker:
                continue

            volume = self._to_int(d[6])
            avg30 = self._to_int(d[8])
            avg10 = self._to_int(d[9])
            volume_change_ratio = None
            if volume and avg30:
                volume_change_ratio = self._to_float(volume / avg30)

            items.append(
                {
                    "site": "bestchoice",
                    "source": "volume",
                    "ticker": name,
                    "tipo": self.tipo,
                    "description": d[1],
                    "exchange": d[3],
                    "close": self._to_float(d[4]),
                    "change": self._to_float(d[5]),
                    "volume": volume,
                    "volume_change": self._to_float(d[7]),
                    "volume_change_ratio": volume_change_ratio,
                    "average_volume_30d": avg30,
                    "average_volume_10d": avg10,
                    "recommendation_mark": self._to_float(d[10]),
                    "net_margin_fy": self._to_float(d[11]),
                    "dividends_yield_current": self._to_float(d[12]),
                }
            )

        return items
