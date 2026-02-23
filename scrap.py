import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import yaml

from scrapers.fundamentus_insiders import FundamentusInsidersScraper
from scrapers.fundamentus_acionistas import FundamentusAcionistasScraper
from scrapers.bestchoice_volume import BestChoiceVolumeScraper
from scrapers.bestchoice_magic_formula import BestChoiceMagicFormulaScraper
from scrapers.statusinvest_prices import StatusInvestPricesScraper
from sharks import build_sharks


def _get_tickers(config: dict, site_cfg: dict) -> list[str]:
    tickers = site_cfg.get("tickers")
    if tickers is None:
        tickers = config.get("tickers", [])
    if not isinstance(tickers, list):
        return []

    result: list[str] = []
    seen: set[str] = set()
    for t in tickers:
        v = str(t or "").strip().upper()
        if not v or v in seen:
            continue
        seen.add(v)
        result.append(v)
    return result


def _md_escape(value: object) -> str:
    s = "" if value is None else str(value)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = s.replace("|", "\\|")
    s = s.replace("\n", "<br>")
    return s


def _md_table(rows: list[dict]) -> str:
    if not rows:
        return "(sem dados)\n"

    cols: list[str] = []
    seen: set[str] = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        for k in r.keys():
            ks = str(k)
            if ks not in seen:
                seen.add(ks)
                cols.append(ks)

    if not cols:
        return "(sem dados)\n"

    header = "| " + " | ".join(_md_escape(c) for c in cols) + " |\n"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |\n"
    body_lines: list[str] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        body_lines.append("| " + " | ".join(_md_escape(r.get(c)) for c in cols) + " |\n")
    return header + sep + "".join(body_lines)


def _md_kv_table(item: dict) -> str:
    rows = [{"campo": k, "valor": item.get(k)} for k in item.keys()]
    return _md_table(rows)


def _to_markdown(payload: dict) -> str:
    ticker = str(payload.get("ticker") or "").strip()
    source = str(payload.get("source") or "").strip()
    generated_at = str(payload.get("generated_at") or "").strip()
    items = payload.get("items")

    title = "# " + (f"{ticker} ({source})" if ticker else source or "data")
    md = title + "\n\n"
    if generated_at:
        md += f"generated_at: {generated_at}\n\n"

    if isinstance(items, list):
        dict_rows = [x for x in items if isinstance(x, dict)]
        md += _md_table(dict_rows)
        return md

    if isinstance(items, dict):
        for k in sorted(items.keys(), key=lambda x: str(x)):
            v = items.get(k)
            md += f"## {k}\n\n"
            if isinstance(v, dict):
                md += _md_kv_table(v) + "\n"
            elif isinstance(v, list):
                md += _md_table([x for x in v if isinstance(x, dict)]) + "\n"
            else:
                md += _md_escape(v) + "\n\n"
        return md

    if isinstance(payload, dict):
        md += _md_kv_table(payload)
    return md


def run(config: dict) -> list[dict]:
    results: list[dict] = []

    sites = config.get("sites", {}) or {}

    site_cfg = sites.get("fundamentus_insiders", {}) or {}
    if site_cfg.get("enabled", True):
        scraper = FundamentusInsidersScraper(
            tickers=_get_tickers(config, site_cfg),
            tipo=int(site_cfg.get("tipo", 1)),
        )
        results.extend(scraper.scrape())

    site_cfg = sites.get("fundamentus_acionistas", {}) or {}
    if site_cfg.get("enabled", True):
        scraper = FundamentusAcionistasScraper(
            tickers=_get_tickers(config, site_cfg),
            tipo=int(site_cfg.get("tipo", 1)),
        )
        results.extend(scraper.scrape())

    site_cfg = sites.get("bestchoice_volume", {}) or {}
    if site_cfg.get("enabled", True):
        scraper = BestChoiceVolumeScraper(
            tickers=_get_tickers(config, site_cfg),
            tipo=str(site_cfg.get("tipo", "stock")),
        )
        results.extend(scraper.scrape())

    site_cfg = sites.get("bestchoice_magic_formula", {}) or {}
    if site_cfg.get("enabled", True):
        scraper = BestChoiceMagicFormulaScraper()
        results.extend(scraper.scrape())

    site_cfg = sites.get("statusinvest_prices", {}) or {}
    if site_cfg.get("enabled", True):
        scraper = StatusInvestPricesScraper(
            tickers=_get_tickers(config, site_cfg),
            cookie=site_cfg.get("cookie"),
            storage_state_path=site_cfg.get("storage_state_path"),
            use_browser_fallback=bool(site_cfg.get("use_browser_fallback", True)),
        )
        results.extend(scraper.scrape())

    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    cfg_path = Path(args.config)
    config = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}

    out_dir = Path(args.out or config.get("output_dir") or "outputs")
    out_dir.mkdir(parents=True, exist_ok=True)

    output_format = str(config.get("output_format") or "json").strip().lower()
    if output_format not in {"json", "md"}:
        output_format = "json"

    generated_at = datetime.now(timezone.utc).isoformat()
    items = run(config)

    by_key: dict[tuple[str, str], list[dict]] = {}
    for item in items:
        ticker = str(item.get("ticker") or "").strip().upper() or "UNKNOWN"
        source = str(item.get("source") or "data").strip().lower() or "data"
        by_key.setdefault((ticker, source), []).append(item)

    volume_map: dict[str, dict] = {}
    magic_formula_map: dict[str, dict] = {}
    for (ticker, source), ticker_items in sorted(by_key.items()):
        if source == "volume":
            if len(ticker_items) == 1:
                volume_map[ticker] = ticker_items[0]
            else:
                volume_map[ticker] = {"items": ticker_items}
            continue

        if source == "magic_formula":
            if len(ticker_items) == 1:
                magic_formula_map[ticker] = ticker_items[0]
            else:
                magic_formula_map[ticker] = {"items": ticker_items}
            continue

        payload = {
            "generated_at": generated_at,
            "ticker": ticker,
            "source": source,
            "items": ticker_items,
        }

        if output_format == "md":
            out_path = out_dir / f"{ticker.lower()}.{source}.md"
            out_path.write_text(_to_markdown(payload), encoding="utf-8")
        else:
            out_path = out_dir / f"{ticker.lower()}.{source}.json"
            out_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        print(f"Wrote {out_path} ({len(ticker_items)} items)")

    if volume_map:
        out_path = out_dir / ("volume.md" if output_format == "md" else "volume.json")
        payload = {
            "generated_at": generated_at,
            "source": "volume",
            "items": volume_map,
        }

        if output_format == "md":
            out_path.write_text(_to_markdown(payload), encoding="utf-8")
        else:
            out_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        print(f"Wrote {out_path} ({len(volume_map)} tickers)")

    if magic_formula_map:
        out_path = out_dir / ("magic_formula.md" if output_format == "md" else "magic_formula.json")
        payload = {
            "generated_at": generated_at,
            "source": "magic_formula",
            "items": magic_formula_map,
        }

        if output_format == "md":
            out_path.write_text(_to_markdown(payload), encoding="utf-8")
        else:
            out_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        print(f"Wrote {out_path} ({len(magic_formula_map)} tickers)")

    sharks = build_sharks(out_dir)
    if sharks:
        out_path = out_dir / "sharks.json"
        out_path.write_text(
            json.dumps(sharks, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"Wrote {out_path} ({len(sharks)} sharks)")

    if not items:
        print("No items collected")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
