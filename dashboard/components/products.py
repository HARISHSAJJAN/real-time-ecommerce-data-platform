"""Product performance section."""
import pandas as pd
import streamlit as st


def render(products: dict) -> None:
    st.subheader("Product Performance")
    df = pd.DataFrame(products["products"])
    if df.empty:
        st.info("No product performance data yet.")
        return

    df = df.sort_values("revenue", ascending=False)
    display_df = df[["name", "category", "views", "cart_additions", "purchases", "revenue", "conversion_rate"]]
    display_df = display_df.rename(
        columns={
            "name": "Product",
            "category": "Category",
            "views": "Views",
            "cart_additions": "Cart Adds",
            "purchases": "Purchases",
            "revenue": "Revenue ($)",
            "conversion_rate": "Conversion Rate",
        }
    )
    st.dataframe(
        display_df.style.format({"Revenue ($)": "{:,.2f}", "Conversion Rate": "{:.2%}"}),
        use_container_width=True,
        hide_index=True,
    )
