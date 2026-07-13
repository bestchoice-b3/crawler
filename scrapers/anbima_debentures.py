from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AnbimaDebenturesScraper:
    tickers: list[str]

    # ------------------------------------------------------------------ browser
    @staticmethod
    def _fetch_via_browser(ticker: str) -> dict[str, Any] | None:
        """Run the worker subprocess that opens the Anbima page and
        intercepts the API response JSON directly from the browser."""
        worker = Path(__file__).with_name("_anbima_token_worker.py")
        if not worker.exists():
            return None

        try:
            proc = subprocess.Popen(
                [sys.executable, str(worker), ticker.lower()],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
            )
            stdout, _ = proc.communicate(timeout=90)
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            try:
                proc.kill()
            except Exception:
                pass
            return None

        if proc.returncode != 0:
            return None

        raw = (stdout or "").strip()
        if not raw:
            return None

        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return None

    # ------------------------------------------------------------------ parse
    def _normalize_item(self, raw: dict[str, Any], ticker: str) -> dict[str, Any]:
        item: dict[str, Any] = {
            "site": "anbima",
            "source": "debentures",
            "ticker": ticker,
        }
        item.update(raw)
        return item

    @staticmethod
    def _extract_records(payload: Any) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []

        if isinstance(payload, dict):
            content = payload.get("content")
            if isinstance(content, list):
                records = [r for r in content if isinstance(r, dict)]
            elif isinstance(payload.get("data"), list):
                records = [r for r in payload["data"] if isinstance(r, dict)]
            elif not content and not payload.get("data"):
                records = [payload]
        elif isinstance(payload, list):
            records = [r for r in payload if isinstance(r, dict)]

        return records

    # ------------------------------------------------------------------ main
    def scrape(self) -> list[dict[str, Any]]:
        all_items: list[dict[str, Any]] = []

        for ticker in self.tickers:
            t = (ticker or "").strip().upper()
            if not t:
                continue

            payload = self._fetch_via_browser(t)
            if payload is None:
                continue

            for raw in self._extract_records(payload):
                all_items.append(self._normalize_item(raw, t))

        return all_items
