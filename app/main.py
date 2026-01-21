# app/main.py
from typing import List, Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, asc, desc
from sqlalchemy.orm import Session

from .db import SessionLocal, ensure_schema, Item


app = FastAPI(title="NFL Card Scanner")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _startup() -> None:
    ensure_schema()


def _get_db() -> Session:
    return SessionLocal()


@app.get("/health")
def health() -> dict:
    return {"ok": True}


HOME_HTML = (
    "<!doctype html>"
    "<html>"
    "<head>"
    '<meta charset="utf-8">'
    "<title>NFL Card Scanner</title>"
    "<style>"
    "body{font-family:Arial,Helvetica,sans-serif;margin:20px}"
    "table{border-collapse:collapse;width:100%}"
    "th,td{border:1px solid #ddd;padding:8px;vertical-align:top}"
    "th{background:#f4f4f4;text-align:left}"
    "img{max-width:90px;height:auto}"
    ".muted{color:#666;font-size:12px}"
    ".pos{font-weight:700}"
    ".neg{font-weight:700}"
    "</style>"
    "</head>"
    "<body>"
    "<h2>Auctions ending soon</h2>"
    '<div class="muted">Sorted by nearest end time. Market value is estimated from similar fixed price listings.</div>'
    '<table id="t">'
    "<thead>"
    "<tr>"
    "<th>Card</th>"
    "<th>Total</th>"
    "<th>Market</th>"
    "<th>Profit</th>"
    "<th>Ends</th>"
    "<th>Query</th>"
    "</tr>"
    "</thead>"
    "<tbody></tbody>"
    "</table>"
    "<script>"
    "function money(n){ return Number(n || 0).toFixed(2)
