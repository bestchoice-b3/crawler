from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class BestChoiceMagicFormulaScraper:
    def scrape(self) -> list[dict[str, Any]]:
        url = "https://n8n.semalo.com.br/webhook/magic"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        }

        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            return []

        items: list[dict[str, Any]] = []
        for row in data:
            if not isinstance(row, dict):
                continue

            ticker = str(row.get("simbolo") or "").strip().upper()
            if not ticker:
                continue

            items.append(
                {
                    "site": "bestchoice",
                    "source": "magic_formula",
                    "ticker": ticker,
                    **row,
                }
            )

        return items
