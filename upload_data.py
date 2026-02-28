import argparse
import base64
import json
import mimetypes
import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from supabase import create_client


@dataclass(frozen=True)
class UploadConfig:
    supabase_url: str
    supabase_key: str
    outputs_dir: Path
    mt5_files_dir: Path | None
    common_id: int
    dry_run: bool
    tickers: list[str] | None


def _load_config(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _get_tickers(config: dict[str, Any], only: list[str] | None) -> list[str] | None:
    if only:
        result: list[str] = []
        seen: set[str] = set()
        for t in only:
            v = str(t or "").strip().upper()
            if not v or v in seen:
                continue
            seen.add(v)
            result.append(v)
        return result

    tickers = config.get("tickers")
    if tickers is None:
        return None

    if not isinstance(tickers, list):
        return None

    result: list[str] = []
    seen: set[str] = set()
    for t in tickers:
        v = str(t or "").strip().upper()
        if not v or v in seen:
            continue
        seen.add(v)
        result.append(v)
    return result


def _infer_tickers_from_outputs(outputs_dir: Path) -> list[str]:
    tickers: list[str] = []
    seen: set[str] = set()
    pat = re.compile(r"^([a-z0-9]+)\.([a-z0-9_]+)\.json$", re.IGNORECASE)

    for p in sorted(outputs_dir.glob("*.json")):
        m = pat.match(p.name)
        if not m:
            continue
        t = m.group(1).strip().upper()
        if not t or t in seen:
            continue
        seen.add(t)
        tickers.append(t)
    return tickers


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _maybe_read_outputs_json(outputs_dir: Path, file_name: str) -> Any | None:
    p = outputs_dir / file_name
    if not p.exists():
        return None
    try:
        return _read_json(p)
    except Exception:
        return None


def _maybe_read_json_from_dir(base_dir: Path | None, file_name: str) -> Any | None:
    if base_dir is None:
        return None
    p = base_dir / file_name
    if not p.exists():
        return None
    try:
        return _read_json(p)
    except Exception:
        return None


def _encode_image_base64(path: Path) -> str:
    raw = path.read_bytes()
    mime, _ = mimetypes.guess_type(str(path))
    if not mime:
        mime = "application/octet-stream"
    b64 = base64.b64encode(raw).decode("ascii")
    return f"data:{mime};base64,{b64}"


def _maybe_read_mt5_image(mt5_dir: Path | None, ticker: str) -> str | None:
    if mt5_dir is None:
        return None
    if not mt5_dir.exists() or not mt5_dir.is_dir():
        return None

    t = ticker.strip().upper()
    prefix = t.lower()
    expected = f"{prefix}_d1.png"

    candidates: list[Path] = []
    try:
        for p in mt5_dir.iterdir():
            if not p.is_file():
                continue
            name = p.name.lower()
            if not name.endswith(".png"):
                continue
            if name == expected:
                candidates.append(p)
                continue
            if not name.startswith(prefix):
                continue
            candidates.append(p)
    except Exception:
        return None

    for p in sorted(candidates, key=lambda x: x.name.lower()):
        try:
            return _encode_image_base64(p)
        except Exception:
            continue
    return None


def _build_row(outputs_dir: Path, mt5_files_dir: Path | None, ticker: str) -> dict[str, Any]:
    t = ticker.strip().upper()

    insiders = _maybe_read_outputs_json(outputs_dir, f"{t.lower()}.insiders.json")
    statusinvest = _maybe_read_outputs_json(outputs_dir, f"{t.lower()}.statusinvest.json")
    acionistas = _maybe_read_outputs_json(outputs_dir, f"{t.lower()}.acionistas.json")

    adx = _maybe_read_json_from_dir(mt5_files_dir, f"{t.lower()}.adx.json")
    obv = _maybe_read_json_from_dir(mt5_files_dir, f"{t.lower()}.obv.json")
    peaks_valleys = _maybe_read_json_from_dir(mt5_files_dir, f"{t.lower()}.pico_vale.json")
    mt5_image = _maybe_read_mt5_image(mt5_files_dir, t)

    row: dict[str, Any] = {
        "ticker": t,
        "update_at": date.today().isoformat(),
        "data_insiders": insiders,
        "data_indicators": statusinvest,
        "data_shark": acionistas,
        "data_obv": obv,
        "data_adx": adx,
        "data_peaks_valleys": peaks_valleys,
        "image_mt5": mt5_image,
    }

    return row


def _build_common_row(outputs_dir: Path, common_id: int) -> dict[str, Any]:
    magic_formula = _maybe_read_outputs_json(outputs_dir, "magic_formula.json")
    volume = _maybe_read_outputs_json(outputs_dir, "volume.json")
    sharks = _maybe_read_outputs_json(outputs_dir, "sharks.json")

    row: dict[str, Any] = {
        "id": int(common_id),
        "data_magic_formula": magic_formula,
        "data_volume": volume,
        "data_sharks": sharks,
    }
    return row


def _upsert_rows(cfg: UploadConfig) -> None:
    config_data = _load_config(Path("config.yaml"))
    tickers = cfg.tickers
    if tickers is None:
        tickers = _get_tickers(config_data, None)
    if not tickers:
        tickers = _infer_tickers_from_outputs(cfg.outputs_dir)

    if not tickers:
        raise SystemExit(
            "No tickers found. Provide --ticker, set tickers in config.yaml, or ensure outputs/*.json exist"
        )

    rows = [_build_row(cfg.outputs_dir, cfg.mt5_files_dir, t) for t in tickers]

    common_row = _build_common_row(cfg.outputs_dir, cfg.common_id)

    if cfg.dry_run:
        print(f"DRY RUN: would upsert {len(rows)} rows into indicators")
        print(json.dumps(rows[:2], ensure_ascii=False, indent=2))
        print("DRY RUN: would upsert 1 row into indicators_common")
        print(json.dumps(common_row, ensure_ascii=False, indent=2))
        return

    supabase = create_client(cfg.supabase_url, cfg.supabase_key)

    resp = supabase.from_("indicators").upsert(rows, on_conflict="ticker").execute()
    if getattr(resp, "error", None):
        raise SystemExit(str(resp.error))

    data = getattr(resp, "data", None)
    if isinstance(data, list):
        print(f"Upserted {len(data)} rows")
    else:
        print("Upsert completed")

    resp2 = (
        supabase.from_("indicators_common")
        .upsert([common_row], on_conflict="id")
        .execute()
    )
    if getattr(resp2, "error", None):
        raise SystemExit(str(resp2.error))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--out", default=None)
    parser.add_argument("--mt5-dir", default=None)
    parser.add_argument("--common-id", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--ticker", action="append", default=None)
    args = parser.parse_args()

    load_dotenv()

    cfg_path = Path(args.config)
    config = _load_config(cfg_path)

    outputs_dir = Path(args.out or config.get("output_dir") or "outputs")

    mt5_files_dir_value = args.mt5_dir or config.get("mt5_files_dir")
    mt5_files_dir = Path(mt5_files_dir_value) if mt5_files_dir_value else None

    common_id_value = (
        args.common_id
        if args.common_id is not None
        else config.get("indicators_common_id")
    )
    if common_id_value is None:
        common_id_value = os.environ.get("INDICATORS_COMMON_ID")
    common_id = int(common_id_value) if common_id_value is not None else 1

    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise SystemExit("Missing SUPABASE_URL and/or SUPABASE_KEY env vars")

    up_cfg = UploadConfig(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        outputs_dir=outputs_dir,
        mt5_files_dir=mt5_files_dir,
        common_id=common_id,
        dry_run=bool(args.dry_run),
        tickers=_get_tickers(config, args.ticker),
    )

    _upsert_rows(up_cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
