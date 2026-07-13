"""Standalone script: opens Anbima debentures page with Playwright,
intercepts the API response and prints the JSON body directly."""
from __future__ import annotations

import json
import sys

from playwright.sync_api import sync_playwright

API_PREFIX = "https://data-api.prd.anbima.com.br/web-bff/v1/debentures"
_PAGE_TEMPLATE = "https://data.anbima.com.br/busca/debentures?size=100&q={ticker}&view=caracteristicas"

captured: list[dict] = []


def on_response(response) -> None:
    if API_PREFIX in response.url and not captured:
        try:
            body = response.json()
        except Exception:
            return
        # only capture the response that has debenture records
        if isinstance(body, dict) and isinstance(body.get("content"), list):
            captured.append(body)


def main() -> None:
    ticker = sys.argv[1] if len(sys.argv) > 1 else "rani"
    page_url = _PAGE_TEMPLATE.format(ticker=ticker)

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=False)
        except Exception:
            browser = p.chromium.launch(headless=True)

        context = browser.new_context()
        page = context.new_page()
        page.on("response", on_response)

        try:
            page.goto(page_url, wait_until="domcontentloaded", timeout=60_000)
            for _ in range(60):
                if captured:
                    break
                page.wait_for_timeout(500)
        except Exception:
            pass
        finally:
            browser.close()

    if captured:
        print(json.dumps(captured[0]), end="")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
