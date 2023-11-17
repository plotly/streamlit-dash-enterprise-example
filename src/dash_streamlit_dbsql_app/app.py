import streamlit as st
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
from pathlib import Path
from dash_streamlit_dbsql_app.data_provider import DataProvider
import os
import plotly.figure_factory as ff
import plotly.graph_objects as go
import numpy as np

# Load environment variables
env_path = Path(".") / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)


data_provider = DataProvider()


def set_mapbox_token():
    if os.getenv("MAPBOX_TOKEN"):
        px.set_mapbox_access_token(os.getenv("MAPBOX_TOKEN"))


def density_map(data):
    st.subheader("Selected categories heatmap")
    fig_map = px.density_mapbox(
        data_frame=data,
        lat="latitude",
        lon="longitude",
        hover_name="name",
        hover_data=['name', "fsq_category_labels"],
        zoom=10,
        radius=15,
        opacity=0.7,
    )
    mapbox_style = "open-street-map" if not os.getenv("MAPBOX_TOKEN") else "dark"
    fig_map.update_layout(mapbox_style=mapbox_style)
    fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    st.plotly_chart(fig_map)


def main():
    st.set_page_config(layout="wide")
    st.title("New York City FourSquare Data")

    set_mapbox_token()

    top_c1, top_c2 = st.columns(2, gap="small")

    selected_categories = []

    with top_c1:
        st.markdown(
            """ The dataset contains POI (points of interest like restaurants, coffee shops, museums, stadiums, etc.) for the city of New York across all categories.
            You can easily access this dataset on the Databricks Marketplace.
            """
        )
        st.subheader('Select Categories')
        categories = data_provider.get_all_categories()
        selected_categories = st.multiselect('Select Categories', categories, default=['Coffee Shop', 'Restaurant'])

    with top_c2:
        st.subheader('Top Categories')
        top_categories = data_provider.get_top_categories(n=20)
        top_categories.columns = ["Category", "Count"]
        st.dataframe(top_categories, use_container_width=True, hide_index=True)

    c1, c2 = st.columns(2)

    with c1:
        data = data_provider.get_selected_categories(selected_categories)
        if not data.empty:
            density_map(data)

    with c2:
        fig = go.Figure(
            data=[
                go.Sankey(
                    node=dict(
                        pad=15,
                        thickness=20,
                        line=dict(color="black", width=0.5),
                        label=["A1", "A2", "B1", "B2", "C1", "C2"],
                        color="blue",
                    ),
                    link=dict(
                        source=[0, 1, 0, 2, 3, 3],  # indices correspond to labels, eg A1, A2, A1, B1, ...
                        target=[2, 3, 3, 4, 4, 5],
                        value=[8, 4, 2, 8, 4, 2],
                    ),
                )
            ]
        )

        fig.update_layout(title_text="Basic Sankey Diagram", font_size=10)
        st.plotly_chart(fig)


if __name__ == "__main__":
    main()
