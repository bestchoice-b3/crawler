#!/usr/bin/env python
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from scrapers.statusinvest_prices import StatusInvestPricesScraper

app = FastAPI(title="StatusInvest Scraper API", version="1.0.0")

DEFAULT_STORAGE_STATE = str(PROJECT_ROOT / "statusinvest_storage_state.json")


@app.get("/scrape/{ticker}")
def scrape_ticker(ticker: str) -> JSONResponse:
    t = ticker.strip().upper()
    if not t:
        raise HTTPException(status_code=400, detail="ticker is required")

    scraper = StatusInvestPricesScraper(
        tickers=[t],
        cookie=None,
        storage_state_path=DEFAULT_STORAGE_STATE,
        use_browser_fallback=False,
    )

    items = scraper.scrape()

    if not items:
        raise HTTPException(status_code=404, detail=f"No data found for ticker '{t}'")

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "statusinvest_prices",
        "ticker": t,
        "items_count": len(items),
        "items": items,
    }

    return JSONResponse(content=payload)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
