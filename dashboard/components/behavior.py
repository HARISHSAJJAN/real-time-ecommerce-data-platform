"""User behavior section: events by type, device distribution, country distribution."""
import pandas as pd
import plotly.express as px
import streamlit as st


def render(events: dict, devices: dict, countries: dict) -> None:
    st.subheader("User Behavior")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.caption("Events by type")
        df = pd.DataFrame(events["by_type"])
        if not df.empty:
            fig = px.pie(df, names="event_type", values="event_count", hole=0.4)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No event data yet.")

    with col2:
        st.caption("Device distribution")
        df = pd.DataFrame(devices["devices"])
        if not df.empty:
            fig = px.pie(df, names="device_type", values="event_count", hole=0.4)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No device data yet.")

    with col3:
        st.caption("Country distribution")
        df = pd.DataFrame(countries["countries"])
        if not df.empty:
            fig = px.pie(df, names="country", values="event_count", hole=0.4)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No country data yet.")
