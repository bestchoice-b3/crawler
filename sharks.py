import argparse
import json
import re
from pathlib import Path


_SUFFIX_STOPWORDS: set[str] = {
    "inc",
    "incorporated",
    "corp",
    "corporation",
    "co",
    "company",
    "ltd",
    "ltda",
    "plc",
    "llc",
    "lp",
    "l.p",
    "sa",
    "s/a",
    "holding",
    "holdings",
}


def _normalize_shark_name(name: str) -> str:
    s = (name or "").strip().lower()
    if not s:
        return ""

    s = re.sub(r"[\"'`´]", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ""

    tokens = [t for t in s.split(" ") if t]

    filtered: list[str] = []
    for t in tokens:
        if t in _SUFFIX_STOPWORDS:
            continue
        filtered.append(t)

    if not filtered:
        return tokens[0]

    return " ".join(filtered)


def build_sharks(out_dir: Path) -> list[dict]:
    accionistas_map: dict[str, set[str]] = {}
    display_name_counts: dict[str, dict[str, int]] = {}

    for p in sorted(out_dir.glob("*.acionistas.json")):
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue

        ticker = str(payload.get("ticker") or "").strip().upper()
        if not ticker:
            m = re.match(r"^([a-z0-9]+)\.acionistas\.json$", p.name, re.IGNORECASE)
            if m:
                ticker = m.group(1).strip().upper()

        items = payload.get("items")
        if not ticker or not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue
            name_raw = str(item.get("acionista") or "").strip()
            if not name_raw:
                continue

            key = _normalize_shark_name(name_raw)
            if not key:
                continue

            accionistas_map.setdefault(key, set()).add(ticker)
            display_name_counts.setdefault(key, {})
            display_name_counts[key][name_raw] = display_name_counts[key].get(name_raw, 0) + 1

    sharks: list[dict] = []
    for shark_key, tickers in accionistas_map.items():
        items = sorted(tickers)

        name_counts = display_name_counts.get(shark_key) or {}
        if name_counts:
            shark_name = sorted(name_counts.items(), key=lambda x: (-x[1], x[0]))[0][0]
        else:
            shark_name = shark_key

        sharks.append(
            {
                "shark_name": shark_name,
                "quantity": len(items),
                "items": items,
            }
        )

    sharks.sort(key=lambda x: (-int(x.get("quantity") or 0), str(x.get("shark_name") or "")))
    return sharks


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="outputs")
    args = parser.parse_args()

    out_dir = Path(args.out)
    sharks = build_sharks(out_dir)
    if not sharks:
        print("No sharks found")
        return 0

    out_path = out_dir / "sharks.json"
    out_path.write_text(
        json.dumps(sharks, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Wrote {out_path} ({len(sharks)} sharks)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
