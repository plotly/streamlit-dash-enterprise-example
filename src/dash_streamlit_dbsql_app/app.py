import os
from pathlib import Path

import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from loguru import logger

from dash_streamlit_dbsql_app.data_provider import DataProvider

st.set_page_config(layout="wide")


def load_env():
    env_path = Path(".") / ".env"
    if env_path.exists():
        logger.info(f"Loading environment variables from {env_path}")
        load_dotenv(dotenv_path=env_path)
    else:
        logger.info(f"Skipping loading environment variables from {env_path}")


def set_mapbox_token():
    if os.getenv("MAPBOX_TOKEN"):
        px.set_mapbox_access_token(os.getenv("MAPBOX_TOKEN"))


def density_map(data):
    fig_map = px.density_mapbox(
        data_frame=data,
        lat="latitude",
        lon="longitude",
        hover_name="name",
        hover_data=["name", "fsq_category_labels"],
        zoom=10,
        radius=15,
        opacity=0.7,
    )
    mapbox_style = "open-street-map" if not os.getenv("MAPBOX_TOKEN") else "dark"
    fig_map.update_layout(mapbox_style=mapbox_style)
    fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    st.plotly_chart(fig_map)


def preconfigure():
    logger.info("Preconfiguring Streamlit")
    load_env()
    set_mapbox_token()
    logger.info("Streamlit preconfigured")


preconfigure()
data_provider = DataProvider()


@st.cache_data
def get_all_categories():
    return data_provider.get_all_categories()


@st.cache_data
def get_top_categories():
    return data_provider.get_top_categories(n=20)


logger.info("Caching data")
all_categories = get_all_categories()
top_categories = get_top_categories()
logger.info("Data cached")


def app():
    st.title("New York City FourSquare Data app")
    st.markdown(
        """ The dataset contains POI (points of interest like restaurants, coffee shops, museums, stadiums, etc.) for the city of New York across all categories.
                You can easily access this dataset on the Databricks Marketplace.
                """
    )

    top_c1, top_c2 = st.columns(2, gap="small")

    with top_c1:
        selected = st.multiselect(
            "Select Categories", all_categories, default=["Coffee Shop", "Restaurant"], key="categories"
        )
    with top_c2:
        st.subheader("Top Categories")
        tc = top_categories.copy()
        tc.columns = ["Category", "Count"]
        st.dataframe(tc, use_container_width=True, hide_index=True)

    c1, c2 = st.columns(2)

    with c1:
        if selected:
            st.subheader("Density Map for the selected categories")
            with st.spinner("Loading data..."):
                data = data_provider.get_selected_categories(selected)

            if not data.empty:
                density_map(data)

    with c2:
        if selected:
            st.subheader("Top Associated Categories")
            with st.spinner("Loading data..."):
                top_associated = data_provider.get_top_associated(selected, n=10)

            if not top_associated.empty:
                top_associated.columns = ["Category", "Count"]
                bar = px.bar(top_associated, x="Category", y="Count", title="Top Associated Categories")
                st.plotly_chart(bar, use_container_width=True)


def main():
    logger.info("Preparing data provider")
    logger.info("Data provider ready")
    app()


if __name__ == "__main__":
    main()
