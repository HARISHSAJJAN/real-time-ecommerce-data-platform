"""Revenue section: trend over time, by country, by product."""
import pandas as pd
import plotly.express as px
import streamlit as st


def render(revenue: dict, top_products: dict) -> None:
    st.subheader("Revenue")

    daily_df = pd.DataFrame(revenue["daily"])
    country_df = pd.DataFrame(revenue["by_country"])
    products_df = pd.DataFrame(top_products["products"])

    col1, col2 = st.columns(2)

    with col1:
        st.caption("Revenue over time")
        if not daily_df.empty:
            daily_df = daily_df.sort_values("event_date")
            fig = px.line(daily_df, x="event_date", y="revenue", markers=True)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No revenue data yet - let the pipeline run for a few minutes.")

    with col2:
        st.caption("Revenue by country")
        if not country_df.empty:
            fig = px.bar(country_df.sort_values("revenue", ascending=True), x="revenue", y="country", orientation="h")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No country revenue data yet.")

    st.caption("Revenue by product (top products)")
    if not products_df.empty:
        fig = px.bar(products_df.sort_values("revenue", ascending=True), x="revenue", y="name", orientation="h")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No product revenue data yet.")
