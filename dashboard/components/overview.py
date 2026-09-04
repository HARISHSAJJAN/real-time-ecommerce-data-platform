"""Overview section: top-line KPIs."""
import streamlit as st


def render(revenue: dict, events: dict) -> None:
    st.subheader("Overview")
    summary = revenue["summary"]

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Revenue", f"${summary['total_revenue']:,.2f}")
    col2.metric("Total Purchases", f"{summary['total_purchases']:,}")
    col3.metric("Total Events", f"{events['total_events']:,}")
    col4.metric("Active Users", f"{events['active_users']:,}")
    col5.metric("Avg Order Value", f"${summary['average_order_value']:,.2f}")
