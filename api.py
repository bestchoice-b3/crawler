#!/usr/bin/env python
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import os

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from dotenv import load_dotenv

from scrapers.anbima_debentures import AnbimaDebenturesScraper
from scrapers.fundamentus_acionistas import FundamentusAcionistasScraper
from scrapers.fundamentus_insiders import FundamentusInsidersScraper
from scrapers.statusinvest_prices import StatusInvestPricesScraper
from transactions_importer import import_excel

load_dotenv()

app = FastAPI(title="StatusInvest Scraper API", version="1.0.0")

_cors_origins_env = os.getenv("CORS_ORIGINS", "")
CORS_ORIGINS = [
    origin.strip()
    for origin in _cors_origins_env.split(",")
    if origin.strip()
] or [
    "https://www.meuradarb3.com.br",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DEFAULT_STORAGE_STATE = str(PROJECT_ROOT / "statusinvest_storage_state.json")

_TRANSACTIONS_HTML = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Importar movimentações</title>
<style>
  body { font-family: Arial, sans-serif; margin: 40px; background: #f7f7f7; }
  .container { max-width: 600px; margin: auto; background: #fff; padding: 24px; border-radius: 8px; box-shadow: 0 2px 6px rgba(0,0,0,0.1); }
  h1 { margin-top: 0; }
  input[type="file"] { margin: 12px 0; }
  button { padding: 10px 18px; background: #2563eb; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
  button:hover { background: #1d4ed8; }
  #result { margin-top: 20px; padding: 12px; background: #f0f0f0; border-radius: 4px; white-space: pre-wrap; }
</style>
</head>
<body>
<div class="container">
  <h1>Importar movimentações</h1>
  <p>Selecione o arquivo Excel (.xlsx) exportado da corretora:</p>
  <form id="uploadForm">
    <input type="file" id="fileInput" accept=".xlsx,.xls" required />
    <br>
    <button type="submit">Importar para o Supabase</button>
  </form>
  <div id="result"></div>
</div>
<script>
document.getElementById('uploadForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const fileInput = document.getElementById('fileInput');
  const file = fileInput.files[0];
  if (!file) return;
  const result = document.getElementById('result');
  result.textContent = 'Importando...';
  const formData = new FormData();
  formData.append('file', file);
  try {
    const resp = await fetch('/transactions/import', { method: 'POST', body: formData });
    const data = await resp.json();
    if (!resp.ok) {
      result.textContent = 'Erro: ' + (data.detail || JSON.stringify(data));
    } else {
      result.textContent = 'Importado com sucesso!\nProcessadas: ' + data.processed + '\nInseridas/atualizadas: ' + data.imported;
    }
  } catch (err) {
    result.textContent = 'Erro na requisição: ' + err.message;
  }
});
</script>
</body>
</html>"""


@app.get("/transactions", response_class=HTMLResponse)
def transactions_page() -> str:
    return _TRANSACTIONS_HTML


@app.post("/transactions/import")
def transactions_import(file: UploadFile = File(...)) -> JSONResponse:
    if not file.filename or not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Only Excel files (.xlsx / .xls) are supported")
    try:
        content = file.file.read()
        result = import_excel(content, file.filename)
        return JSONResponse(content=result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/scrape/debentures/{ticker}")
def scrape_debentures(ticker: str) -> JSONResponse:
    t = ticker.strip().upper()
    if not t:
        raise HTTPException(status_code=400, detail="ticker is required")

    scraper = AnbimaDebenturesScraper(tickers=[t])
    items = scraper.scrape()

    if not items:
        raise HTTPException(status_code=404, detail=f"No data found for ticker '{t}'")

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "anbima_debentures",
        "ticker": t,
        "items_count": len(items),
        "items": items,
    }

    return JSONResponse(content=payload)


@app.get("/scrape/acionistas/{ticker}")
def scrape_acionistas(ticker: str, tipo: int = 1) -> JSONResponse:
    t = ticker.strip().upper()
    if not t:
        raise HTTPException(status_code=400, detail="ticker is required")

    if tipo not in {1, 2}:
        raise HTTPException(status_code=400, detail="tipo must be 1 or 2")

    scraper = FundamentusAcionistasScraper(tickers=[t], tipo=tipo)
    items = scraper.scrape()

    if not items:
        raise HTTPException(status_code=404, detail=f"No data found for ticker '{t}'")

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "fundamentus_acionistas",
        "ticker": t,
        "tipo": tipo,
        "items_count": len(items),
        "items": items,
    }

    return JSONResponse(content=payload)


@app.get("/scrape/insiders/{ticker}")
def scrape_insiders(ticker: str, tipo: int = 1) -> JSONResponse:
    t = ticker.strip().upper()
    if not t:
        raise HTTPException(status_code=400, detail="ticker is required")

    if tipo not in {1, 2}:
        raise HTTPException(status_code=400, detail="tipo must be 1 or 2")

    scraper = FundamentusInsidersScraper(tickers=[t], tipo=tipo)
    items = scraper.scrape()

    if not items:
        raise HTTPException(status_code=404, detail=f"No data found for ticker '{t}'")

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "fundamentus_insiders",
        "ticker": t,
        "tipo": tipo,
        "items_count": len(items),
        "items": items,
    }

    return JSONResponse(content=payload)


@app.get("/scrape/{ticker}")
def scrape_ticker(ticker: str) -> JSONResponse:
    t = ticker.strip().upper()
    if not t:
        raise HTTPException(status_code=400, detail="ticker is required")

    scraper = StatusInvestPricesScraper(
        tickers=[t],
        cookie=None,
        storage_state_path=DEFAULT_STORAGE_STATE,
        use_browser_fallback=True,
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
