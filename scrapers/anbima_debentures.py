from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

import requests


_DEBENTURES_PAGE = "https://anbima.com.br/pt_br/informar/precos-e-indices/precos/debentures.htm"
_API_PREFIX = "https://data-api.prd.anbima.com.br/web-bff/v1/debentures"


@dataclass(frozen=True)
class AnbimaDebenturesScraper:
    tickers: list[str]
    google_authorization: str = ""
    page: int = 0
    size: int = 100
    view: str = "caracteristicas"
    order_field: str = "codigo_b3"
    order: str = "asc"

    _BASE_URL: str = field(
        default=_API_PREFIX,
        init=False,
        repr=False,
    )

    # ------------------------------------------------------------------ token
    @staticmethod
    def _is_token_expired(token: str) -> bool:
        """Decode JWT payload (no verification) and check `iat`."""
        import base64

        parts = token.split(".")
        if len(parts) < 2:
            return True

        padded = parts[1] + "=" * (-len(parts[1]) % 4)
        try:
            payload = json.loads(base64.urlsafe_b64decode(padded))
        except Exception:
            return True

        iat = payload.get("iat")
        if not isinstance(iat, (int, float)):
            return True

        # token timestamps from this API are in milliseconds
        iat_seconds = iat / 1000 if iat > 1e12 else iat
        age = time.time() - iat_seconds
        # consider expired after 90 seconds (conservative)
        return age > 90

    @staticmethod
    def _obtain_token_via_browser() -> str | None:
        """Open the Anbima debentures page with Playwright, wait for the
        frontend to make an API call to ``data-api.prd.anbima.com.br`` and
        capture the ``g-google-authorization`` header it sends."""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            return None

        captured_token: list[str] = []

        def _on_request(request: Any) -> None:
            if _API_PREFIX in request.url and not captured_token:
                hdr = request.headers.get("g-google-authorization")
                if hdr:
                    captured_token.append(hdr)

        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(headless=False)
            except Exception:
                browser = p.chromium.launch(headless=True)

            context = browser.new_context()
            page = context.new_page()
            page.on("request", _on_request)

            try:
                page.goto(_DEBENTURES_PAGE, wait_until="domcontentloaded", timeout=60_000)
                # wait up to 30s for the page JS to fire an API request
                for _ in range(60):
                    if captured_token:
                        break
                    page.wait_for_timeout(500)
            except Exception:
                pass
            finally:
                browser.close()

        return captured_token[0] if captured_token else None

    def _resolve_token(self) -> str | None:
        """Return a valid token: reuse the provided one if still fresh,
        otherwise obtain a new one via the browser."""
        if self.google_authorization and not self._is_token_expired(self.google_authorization):
            return self.google_authorization

        return self._obtain_token_via_browser()

    # ------------------------------------------------------------------ http
    def _build_url(self, query: str) -> str:
        return (
            f"{self._BASE_URL}"
            f"?view={self.view}"
            f"&page={self.page}"
            f"&size={self.size}"
            f"&field={self.order_field}"
            f"&order={self.order}"
            f"&q={query}"
        )

    def _fetch(self, url: str, token: str) -> dict[str, Any]:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Origin": "https://anbima.com.br",
            "Referer": "https://anbima.com.br/",
            "g-google-authorization": token,
        }

        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

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
        token = self._resolve_token()
        if not token:
            return []

        all_items: list[dict[str, Any]] = []

        for ticker in self.tickers:
            t = (ticker or "").strip().upper()
            if not t:
                continue

            url = self._build_url(t)

            try:
                payload = self._fetch(url, token)
            except requests.HTTPError as exc:
                # if 401/403, try refreshing the token once
                status = getattr(exc.response, "status_code", None)
                if status in (401, 403):
                    new_token = self._obtain_token_via_browser()
                    if new_token:
                        token = new_token
                        try:
                            payload = self._fetch(url, token)
                        except requests.RequestException:
                            continue
                    else:
                        continue
                else:
                    continue
            except requests.RequestException:
                continue

            for raw in self._extract_records(payload):
                all_items.append(self._normalize_item(raw, t))

        return all_items
