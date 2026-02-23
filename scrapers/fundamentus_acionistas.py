from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests
from bs4 import BeautifulSoup


@dataclass(frozen=True)
class FundamentusAcionistasScraper:
    tickers: list[str]
    tipo: int = 1

    def _build_url(self, ticker: str) -> str:
        return (
            "https://www.fundamentus.com.br/principais_acionistas.php"
            f"?papel={ticker}&tipo={self.tipo}"
        )

    def _fetch(self, url: str) -> str:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.text

    @staticmethod
    def _parse_percent_pt(value: str) -> float | None:
        v = (value or "").strip()
        if not v:
            return None

        v = v.replace("%", "").strip()
        v = v.replace(".", "").replace(",", ".")

        try:
            return float(v)
        except ValueError:
            return None

    @staticmethod
    def _is_target_table(table: Any) -> bool:
        try:
            headers = [
                th.get_text(strip=True).lower()
                for th in (table.find_all("th") or [])
            ]
        except Exception:
            return False

        if not headers:
            return False

        has_acionista = any("acionista" in h for h in headers)
        has_participacao = any("particip" in h for h in headers)
        return has_acionista and has_participacao

    def _parse_table(self, html: str, ticker: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "lxml")

        tables = soup.find_all("table")
        table = None
        for t in tables:
            if self._is_target_table(t):
                table = t
                break

        if table is None:
            return []

        rows = table.find_all("tr")
        items: list[dict[str, Any]] = []

        for tr in rows[1:]:
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            acionista = tds[0].get_text(strip=True)
            participacao_txt = tds[1].get_text(strip=True)

            item = {
                "site": "fundamentus",
                "source": "acionistas",
                "ticker": ticker,
                "tipo": self.tipo,
                "acionista": acionista or None,
                "participacao": self._parse_percent_pt(participacao_txt),
            }
            items.append(item)

        return items

    def scrape(self) -> list[dict[str, Any]]:
        all_items: list[dict[str, Any]] = []

        for ticker in self.tickers:
            t = (ticker or "").strip().upper()
            if not t:
                continue

            url = self._build_url(t)
            html = self._fetch(url)
            all_items.extend(self._parse_table(html, t))

        return all_items
