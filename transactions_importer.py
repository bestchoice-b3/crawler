"""Import brokerage transaction Excel files into Supabase.

Duplicates are avoided by computing a deterministic hash for each row and
upserting on the unique ``row_hash`` column.
"""
from __future__ import annotations

import hashlib
import io
import os
from datetime import datetime
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# Map the column names we expect in the Excel file to snake_case DB fields.
COLUMN_MAP: dict[str, str] = {
    "Entrada/Saída": "entrada_saida",
    "Data": "data",
    "Movimentação": "movimentacao",
    "Produto": "produto",
    "Instituição": "instituicao",
    "Quantidade": "quantidade",
    "Preço unitário": "preco_unitario",
    "Valor da Operação": "valor_operacao",
}

# Columns used to build the duplicate-prevention hash.
_HASH_COLUMNS = [
    "entrada_saida",
    "data",
    "movimentacao",
    "produto",
    "instituicao",
    "quantidade",
    "preco_unitario",
    "valor_operacao",
]


def _normalize_text(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if text.lower() in ("", "nan", "<na>", "none"):
        return None
    return text


def _parse_date(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return pd.to_datetime(text, dayfirst=True).date().isoformat()
    except Exception:
        return None


def _parse_decimal(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("R$", "").replace("%", "").replace(" ", "")
    if text in ("", "-", "--"):
        return None
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _extract_ticker(product: str | None) -> str | None:
    if not product:
        return None
    first = product.split(" - ", 1)[0].strip()
    if not first:
        return None
    return first.upper()


def _row_hash(row: dict[str, Any]) -> str:
    parts: list[str] = []
    for col in _HASH_COLUMNS:
        value = row.get(col)
        if value is None:
            parts.append("")
        elif isinstance(value, float):
            parts.append(f"{value:.10g}")
        else:
            parts.append(str(value).strip().lower())
    return hashlib.md5("|".join(parts).encode("utf-8")).hexdigest()


def _parse_row(raw: pd.Series, filename: str) -> dict[str, Any] | None:
    entrada_saida = _normalize_text(raw.get("entrada_saida"))
    data = _parse_date(raw.get("data"))
    movimentacao = _normalize_text(raw.get("movimentacao"))
    produto = _normalize_text(raw.get("produto"))
    instituicao = _normalize_text(raw.get("instituicao"))
    quantidade = _parse_decimal(raw.get("quantidade"))
    preco_unitario = _parse_decimal(raw.get("preco_unitario"))
    valor_operacao = _parse_decimal(raw.get("valor_operacao"))

    if all(v is None for v in (entrada_saida, data, movimentacao, produto, instituicao)):
        return None

    row: dict[str, Any] = {
        "entrada_saida": entrada_saida,
        "data": data,
        "movimentacao": movimentacao,
        "produto": produto,
        "produto_ticker": _extract_ticker(produto),
        "instituicao": instituicao,
        "quantidade": quantidade,
        "preco_unitario": preco_unitario,
        "valor_operacao": valor_operacao,
        "source_file": filename,
    }
    row["row_hash"] = _row_hash(row)
    return row


def _clean_env(value: str | None) -> str:
    if not value:
        return ""
    return value.strip().strip("'\"\n\r")


def import_excel(content: bytes, filename: str) -> dict[str, Any]:
    """Read an Excel file and upsert its rows into the ``transactions`` table."""
    supabase_url = _clean_env(os.environ.get("SUPABASE_URL"))
    supabase_key = _clean_env(os.environ.get("SUPABASE_KEY"))
    if not supabase_url or not supabase_key:
        raise RuntimeError("Missing SUPABASE_URL and/or SUPABASE_KEY env vars")
    if not supabase_url.startswith(("http://", "https://")):
        raise RuntimeError(f"Invalid SUPABASE_URL: {supabase_url!r}")

    try:
        df = pd.read_excel(io.BytesIO(content), dtype=str, keep_default_na=True)
    except Exception as exc:
        raise ValueError(f"Could not read Excel file: {exc}") from exc

    rename_map = {}
    for col in df.columns:
        key = col.strip()
        if key in COLUMN_MAP:
            rename_map[col] = COLUMN_MAP[key]
    df = df.rename(columns=rename_map)

    required = list(COLUMN_MAP.values())
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    rows: list[dict[str, Any]] = []
    for _, raw in df.iterrows():
        parsed = _parse_row(raw, filename)
        if parsed:
            rows.append(parsed)

    if not rows:
        return {"filename": filename, "processed": 0, "imported": 0}

    seen: set[str] = set()
    unique_rows: list[dict[str, Any]] = []
    for row in rows:
        row_hash = row["row_hash"]
        if row_hash not in seen:
            seen.add(row_hash)
            unique_rows.append(row)
    rows = unique_rows

    supabase = create_client(supabase_url, supabase_key)

    imported = 0
    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        resp = supabase.from_("transactions").upsert(
            batch, on_conflict="row_hash"
        ).execute()
        if getattr(resp, "error", None):
            raise RuntimeError(str(resp.error))
        data = getattr(resp, "data", None)
        if isinstance(data, list):
            imported += len(data)

    return {"filename": filename, "processed": len(rows), "imported": imported}
