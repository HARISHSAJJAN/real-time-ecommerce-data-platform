"""Conversion funnel section: Product Views -> Add to Cart -> Purchase."""
import plotly.graph_objects as go
import streamlit as st


def render(funnel: dict) -> None:
    st.subheader("Conversion Funnel")

    stages = funnel["stages"]
    labels = [s["stage"].replace("_", " ").title() for s in stages]
    values = [s["count"] for s in stages]

    fig = go.Figure(
        go.Funnel(
            y=labels,
            x=values,
            textposition="inside",
            textinfo="value+percent initial",
        )
    )
    st.plotly_chart(fig, use_container_width=True)

    cols = st.columns(len(stages))
    for col, stage in zip(cols, stages):
        rate = stage.get("conversion_from_previous")
        label = f"{rate:.1%}" if rate is not None else "—"
        col.metric(f"{stage['stage'].replace('_', ' ').title()} conv.", label)

    st.caption(f"Overall view-to-purchase rate: **{funnel['view_to_purchase_rate']:.2%}**")
