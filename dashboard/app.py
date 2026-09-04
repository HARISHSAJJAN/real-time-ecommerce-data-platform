"""Streamlit dashboard for the Real-Time E-Commerce Data Platform."""
import logging

import streamlit as st

from dashboard import api_client
from dashboard.components import behavior, funnel, overview, products, revenue

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("dashboard.app")

st.set_page_config(page_title="E-Commerce Analytics", layout="wide")

st.title("Real-Time E-Commerce Analytics")
st.caption(
    "Live business metrics computed by Kafka -> Spark Structured Streaming -> StarRocks, "
    "queried through Trino and served by the FastAPI analytics API."
)

with st.sidebar:
    st.header("Settings")
    days = st.slider("Revenue trend window (days)", min_value=1, max_value=90, value=14)
    top_n = st.slider("Top products to show", min_value=5, max_value=50, value=10)
    if st.button("Refresh now"):
        st.cache_data.clear()

try:
    health = api_client.fetch_health()
except Exception:
    st.error(
        f"Cannot reach the analytics API at {api_client.API_BASE_URL}. "
        "Make sure `docker compose up` is running and the API service is healthy."
    )
    st.stop()

if not health.get("trino_reachable", False):
    st.warning("The API is up but cannot reach Trino/StarRocks yet. Data may be incomplete.")

try:
    revenue_data = api_client.fetch_revenue(days=days)
    events_data = api_client.fetch_events()
    products_data = api_client.fetch_products()
    countries_data = api_client.fetch_countries()
    devices_data = api_client.fetch_devices()
    funnel_data = api_client.fetch_funnel()
    top_products_data = api_client.fetch_top_products(limit=top_n)
except Exception as exc:
    logger.exception("Failed to load dashboard data")
    st.error(f"Failed to load data from the API: {exc}")
    st.stop()

overview.render(revenue_data, events_data)
st.divider()
revenue.render(revenue_data, top_products_data)
st.divider()
products.render(products_data)
st.divider()
behavior.render(events_data, devices_data, countries_data)
st.divider()
funnel.render(funnel_data)

st.caption(f"Data refreshes automatically every {api_client.REFRESH_SECONDS} seconds.")
