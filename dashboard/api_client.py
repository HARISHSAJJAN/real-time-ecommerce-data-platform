"""Thin HTTP client wrapping the FastAPI analytics endpoints, with short-lived caching."""
from __future__ import annotations

import logging
import os

import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("dashboard.api_client")

API_BASE_URL = os.getenv("STREAMLIT_API_BASE_URL", "http://localhost:8000")
REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "15"))


def _get(path: str, params: dict | None = None) -> dict:
    url = f"{API_BASE_URL}{path}"
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        logger.error("Request to %s failed: %s", url, exc)
        raise


@st.cache_data(ttl=REFRESH_SECONDS)
def fetch_health() -> dict:
    return _get("/health")


@st.cache_data(ttl=REFRESH_SECONDS)
def fetch_revenue(days: int = 14) -> dict:
    return _get("/metrics/revenue", params={"days": days})


@st.cache_data(ttl=REFRESH_SECONDS)
def fetch_events() -> dict:
    return _get("/metrics/events")


@st.cache_data(ttl=REFRESH_SECONDS)
def fetch_products(limit: int = 50) -> dict:
    return _get("/metrics/products", params={"limit": limit})


@st.cache_data(ttl=REFRESH_SECONDS)
def fetch_countries() -> dict:
    return _get("/metrics/countries")


@st.cache_data(ttl=REFRESH_SECONDS)
def fetch_devices() -> dict:
    return _get("/metrics/devices")


@st.cache_data(ttl=REFRESH_SECONDS)
def fetch_funnel() -> dict:
    return _get("/metrics/funnel")


@st.cache_data(ttl=REFRESH_SECONDS)
def fetch_top_products(limit: int = 10) -> dict:
    return _get("/metrics/top-products", params={"limit": limit})
