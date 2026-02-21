from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import requests
from bs4 import BeautifulSoup


@dataclass(frozen=True)
class FundamentusInsidersScraper:
    tickers: list[str]
    tipo: int = 1

    def _build_url(self, ticker: str) -> str:
        return f"https://www.fundamentus.com.br/insiders.php?papel={ticker}&tipo={self.tipo}"

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
    def _parse_decimal_pt(value: str) -> float | None:
        v = (value or "").strip()
        if not v:
            return None
        v = v.replace("R$", "").replace("%", "").strip()

        # handle thousands '.' and decimal ','
        v = v.replace(".", "").replace(",", ".")

        try:
            return float(v)
        except ValueError:
            return None

    @staticmethod
    def _parse_int_pt(value: str) -> int | None:
        v = (value or "").strip()
        if not v:
            return None

        v = v.replace(".", "")

        # keep sign
        m = re.match(r"^[+-]?\d+$", v)
        if not m:
            return None
        return int(v)

    def _parse_table(self, html: str, ticker: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "lxml")

        table = soup.find("table")
        if table is None:
            return []

        # Header may vary slightly; we map by position based on the page shown:
        # Data | Quantidade | Valor (R$) | Preço Médio | Formulário
        rows = table.find_all("tr")
        items: list[dict[str, Any]] = []

        for tr in rows[1:]:
            tds = tr.find_all("td")
            if len(tds) < 4:
                continue

            date = tds[0].get_text(strip=True)
            quantidade_txt = tds[1].get_text(strip=True)
            valor_txt = tds[2].get_text(strip=True)
            preco_medio_txt = tds[3].get_text(strip=True)

            link = None
            if len(tds) >= 5:
                a = tds[4].find("a")
                if a and a.get("href"):
                    link = a.get("href")
                    if link and link.startswith("/"):
                        link = "https://www.fundamentus.com.br" + link

            item = {
                "site": "fundamentus",
                "source": "insiders",
                "ticker": ticker,
                "tipo": self.tipo,
                "date": date or None,
                "quantidade": self._parse_int_pt(quantidade_txt),
                "valor": self._parse_decimal_pt(valor_txt),
                "preco_medio": self._parse_decimal_pt(preco_medio_txt),
                "formulario_url": link,
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
