#!/usr/bin/env python
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

PROJECT_ROOT = Path(__file__).parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.statusinvest_prices import StatusInvestPricesScraper


def _normalize_tickers(tickers):
    if not isinstance(tickers, list):
        return []
    result = []
    seen = set()
    for t in tickers:
        v = str(t or "").strip().upper()
        if not v or v in seen:
            continue
        seen.add(v)
        result.append(v)
    return result


def _fetch_tickers_from_endpoint(url: str) -> list[str]:
    u = str(url or "").strip()
    if not u:
        return []
    resp = requests.get(u, timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    tickers = payload.get("tickers") if isinstance(payload, dict) else None
    return _normalize_tickers(tickers)


def _get_tickers(config: dict, site_cfg: dict) -> list[str]:
    endpoint = site_cfg.get("tickers_endpoint") or config.get("tickers_endpoint")
    if endpoint:
        try:
            from_endpoint = _fetch_tickers_from_endpoint(str(endpoint))
            if from_endpoint:
                return from_endpoint
        except Exception:
            pass
    tickers = site_cfg.get("tickers")
    if tickers is None:
        tickers = config.get("tickers", [])
    return _normalize_tickers(tickers)


def run_statusinvest(config_path: str, output_path: str) -> dict:
    cfg_path = Path(config_path)
    config = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    sites = config.get("sites", {}) or {}
    site_cfg = sites.get("statusinvest_prices", {}) or {}

    if not site_cfg.get("enabled", True):
        return {
            "status": "skipped",
            "reason": "statusinvest_prices disabled in config",
            "config": str(cfg_path),
        }

    tickers = _get_tickers(config, site_cfg)

    if not tickers:
        return {
            "status": "error",
            "reason": "No tickers found in config",
            "config": str(cfg_path),
        }

    scraper = StatusInvestPricesScraper(
        tickers=tickers,
        cookie=site_cfg.get("cookie"),
        storage_state_path=site_cfg.get("storage_state_path"),
        use_browser_fallback=bool(site_cfg.get("use_browser_fallback", True)),
    )

    items = scraper.scrape()

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "statusinvest_prices",
        "tickers": tickers,
        "items_count": len(items),
        "items": items,
    }

    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "status": "ok",
        "items_count": len(items),
        "tickers_count": len(tickers),
        "output": str(out_file),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run StatusInvest Prices Scraper")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--output", required=True, help="Output JSON file path")
    args = parser.parse_args()

    try:
        result = run_statusinvest(args.config, args.output)
        print(json.dumps(result, ensure_ascii=False))
        sys.exit(0)
    except Exception as e:
        error_result = {
            "status": "error",
            "error": str(e),
        }
        print(json.dumps(error_result, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    main()
