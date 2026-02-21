from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup


@dataclass(frozen=True)
class StatusInvestPricesScraper:
    tickers: list[str]
    cookie: str | None = None
    storage_state_path: str | None = "statusinvest_storage_state.json"
    use_browser_fallback: bool = True

    @staticmethod
    def _get_rows_for_ticker(data: dict[str, Any], ticker: str) -> list[dict[str, Any]] | None:
        t = (ticker or "").strip().lower()
        if not t:
            return None

        for k, v in data.items():
            if isinstance(k, str) and k.strip().lower() == t and isinstance(v, list):
                # ensure rows are dict-like
                rows = [r for r in v if isinstance(r, dict)]
                return rows

        return None

    def _build_url(self, ticker: str) -> str:
        return f"https://statusinvest.com.br/acoes/{ticker.lower()}"

    def _fetch(self, session: requests.Session, url: str) -> str:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        resp = session.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
        return resp.text

    def _fetch_pl_historico(self, session: requests.Session, ticker: str) -> dict[str, Any] | None:
        url = "https://statusinvest.com.br/acao/indicatorhistoricallist"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Origin": "https://statusinvest.com.br",
            "Pragma": "no-cache",
            "Referer": f"https://statusinvest.com.br/acoes/{ticker.lower()}",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }

        if self.cookie:
            headers["Cookie"] = self.cookie

        payload = None
        for code in (ticker.upper(), ticker.lower()):
            data_payload: list[tuple[str, str]] = [
                ("codes[]", code),
                ("time", "5"),
                ("byQuarter", "false"),
                ("futureData", "false"),
            ]
            resp = session.post(url, data=data_payload, headers=headers, timeout=60)
            if resp.status_code != 200:
                continue
            try:
                payload = resp.json() or {}
            except ValueError:
                continue
            break

        if not isinstance(payload, dict):
            return None

        data = payload.get("data")
        if not isinstance(data, dict):
            return None

        ticker_key = next((k for k in data.keys() if k.strip().lower() == ticker.lower()), None)
        if ticker_key is None:
            return None

        rows = data.get(ticker_key)
        if not isinstance(rows, list):
            return None

        pl_row = None
        for r in rows:
            if isinstance(r, dict) and str(r.get("key") or "").lower() == "p_l":
                pl_row = r
                break

        if not isinstance(pl_row, dict):
            return None

        return {
            "media": self._parse_decimal_pt(str(pl_row.get("avg_F") or pl_row.get("avg") or "")),
            "atual": self._parse_decimal_pt(str(pl_row.get("actual_F") or pl_row.get("actual") or "")),
            "menor_valor": self._parse_decimal_pt(str(pl_row.get("minValue_F") or pl_row.get("minValue") or "")),
            "maior_valor": self._parse_decimal_pt(str(pl_row.get("maxValue_F") or pl_row.get("maxValue") or "")),
        }

    def _maybe_fetch_pl_historico_with_browser(self, ticker: str) -> dict[str, Any] | None:
        if not self.use_browser_fallback:
            return None

        storage_path = self.storage_state_path
        if not storage_path:
            return None

        storage_file = Path(storage_path)
        try:
            from playwright.sync_api import sync_playwright
        except Exception:
            return None

        api_url = "https://statusinvest.com.br/acao/indicatorhistoricallist"
        referer_url = f"https://statusinvest.com.br/acoes/{ticker.lower()}"

        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(headless=False)
            except Exception:
                browser = p.chromium.launch(headless=True)

            context_kwargs: dict[str, Any] = {}
            if storage_file.exists():
                context_kwargs["storage_state"] = str(storage_file)

            context = browser.new_context(**context_kwargs)
            page = context.new_page()
            page.goto(referer_url, wait_until="domcontentloaded", timeout=120_000)
            context.storage_state(path=str(storage_file))

            payload = {
                "codes[]": ticker.upper(),
                "time": "5",
                "byQuarter": "false",
                "futureData": "false",
            }

            resp = context.request.post(
                api_url,
                form=payload,
                headers={
                    "Accept": "*/*",
                    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Origin": "https://statusinvest.com.br",
                    "Referer": referer_url,
                    "X-Requested-With": "XMLHttpRequest",
                },
            )

            if not resp.ok:
                browser.close()
                return None

            try:
                payload_json = resp.json()
            except Exception:
                browser.close()
                return None

            context.storage_state(path=str(storage_file))
            browser.close()

        if not isinstance(payload_json, dict):
            return None

        data = payload_json.get("data")
        if not isinstance(data, dict):
            return None

        rows = self._get_rows_for_ticker(data, ticker)
        if not isinstance(rows, list):
            return None

        pl_row = None
        for r in rows:
            if isinstance(r, dict) and str(r.get("key") or "").lower() == "p_l":
                pl_row = r
                break

        if not isinstance(pl_row, dict):
            return None

        return {
            "media": self._parse_decimal_pt(str(pl_row.get("avg_F") or pl_row.get("avg") or "")),
            "atual": self._parse_decimal_pt(str(pl_row.get("actual_F") or pl_row.get("actual") or "")),
            "menor_valor": self._parse_decimal_pt(str(pl_row.get("minValue_F") or pl_row.get("minValue") or "")),
            "maior_valor": self._parse_decimal_pt(str(pl_row.get("maxValue_F") or pl_row.get("maxValue") or "")),
        }

    @staticmethod
    def _parse_decimal_pt(value: str) -> float | None:
        v = (value or "").strip()
        if not v:
            return None
        v = v.replace("R$", "").replace("%", "").strip()
        v = v.replace(".", "").replace(",", ".")
        try:
            return float(v)
        except ValueError:
            return None

    @staticmethod
    def _find_value_by_title(soup: BeautifulSoup, title: str) -> str | None:
        # The site renders blocks like:
        # <h3 class="title">Máx. 52 semanas</h3> ... <strong class="value">31,88</strong>
        h3 = soup.find("h3", string=lambda s: isinstance(s, str) and s.strip().lower() == title.lower())
        if not h3:
            return None

        node = h3
        for _ in range(8):
            if not node:
                break

            strong = node.find("strong", class_=re.compile(r"\bvalue\b"))
            if strong:
                return strong.get_text(strip=True)

            node = node.parent

        return None

    def _parse(self, html: str, ticker: str, url: str) -> dict[str, Any] | None:
        soup = BeautifulSoup(html, "lxml")

        valor_atual_txt = self._find_value_by_title(soup, "Valor atual")
        max_52_txt = self._find_value_by_title(soup, "Máx. 52 semanas")
        dy_txt = self._find_value_by_title(soup, "D.Y")
        pl_txt = self._find_value_by_title(soup, "P/L")
        m_liquida_txt = self._find_value_by_title(soup, "M. Líquida")

        if not valor_atual_txt and not max_52_txt and not dy_txt and not pl_txt and not m_liquida_txt:
            return None

        return {
            "site": "statusinvest",
            "source": "statusinvest",
            "ticker": ticker,
            "url": url,
            "valor_atual": self._parse_decimal_pt(valor_atual_txt or ""),
            "max_52_semanas": self._parse_decimal_pt(max_52_txt or ""),
            "dy": self._parse_decimal_pt(dy_txt or ""),
            "pl": self._parse_decimal_pt(pl_txt or ""),
            "m_liquida": self._parse_decimal_pt(m_liquida_txt or ""),
            "pl_historico": None,
        }

    def scrape(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []

        for ticker in self.tickers:
            t = (ticker or "").strip().upper()
            if not t:
                continue

            url = self._build_url(t)

            session = requests.Session()
            try:
                html = self._fetch(session, url)
            except requests.HTTPError as e:
                resp = getattr(e, "response", None)
                if resp is not None and getattr(resp, "status_code", None) == 404:
                    continue
                continue
            except requests.RequestException:
                continue

            item = self._parse(html, t, url)
            if item:
                try:
                    item["pl_historico"] = self._fetch_pl_historico(session, t)
                except requests.RequestException:
                    item["pl_historico"] = None

                if item.get("pl_historico") is None:
                    item["pl_historico"] = self._maybe_fetch_pl_historico_with_browser(t)
                items.append(item)

        return items
