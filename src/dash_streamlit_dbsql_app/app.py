import os
from pathlib import Path
import plotly.graph_objects as go

import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from loguru import logger
from urllib.request import urlopen
import json


from data_provider import DataProvider

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


@st.cache_data
def get_selected_categories(categories):
    return data_provider.get_selected_categories(categories)


@st.cache_data
def get_category_hierarchy():
    return data_provider.get_categories_hierarchy()


logger.info("Caching data")
all_categories = get_all_categories()
top_categories = get_top_categories()
logger.info("Data cached")


def app():
    st.title("New York City FourSquare Data app")

    st.markdown(
        """ The dataset contains POI (points of interest like restaurants, coffee shops, museums, stadiums, etc.) for the city of New York across all categories.
            You can easily access this dataset on the Databricks Marketplace.            """
    )
    st.markdown(
        """1. Use the selection tool to take a look at the distribution of different places across NYC, and their correlations with each other."""
    )

    st.markdown(
        """2. Then, down below, enter an NYC Zip Code to see the best rated places of the categories you have selected, and their provenance ratings (the extent to which they use locally sourced resources)."""
    )
    selected = st.multiselect(
        "Select Categories", all_categories, default=["Cocktail Bar", "Coffee Shop", "Deli"], key="categories"
    )

    c1, c2 = st.columns(2, gap="small")

    with c1:
        if selected:
            with st.spinner("Loading data..."):
                df_geo = data_provider.get_zip_codes(selected)
                df_geo = df_geo.dropna(subset=['postcode'])
                st.subheader("Zip Code View")
                with urlopen(
                    "https://raw.githubusercontent.com/fedhere/PUI2015_EC/master/mam1612_EC/nyc-zip-code-tabulation-areas-polygons.geojson"
                ) as response:
                    zipcodes = json.load(response)
                    fig_geo = px.choropleth_mapbox(
                        df_geo,
                        geojson=zipcodes,
                        locations='postcode',
                        color='total_places',
                        color_continuous_scale="haline",
                        featureidkey="properties.postalCode",
                        mapbox_style="carto-positron",
                        zoom=11,
                        center={"lat": 40.7484, "lon": -73.9857},  # Center the map on New York City
                        opacity=0.8,
                        hover_data={'postcode': True, 'total_places': True},
                    )
                    mapbox_style = "open-street-map" if not os.getenv("MAPBOX_TOKEN") else "dark"
                    fig_geo.update_layout(
                        mapbox_style=mapbox_style,
                        margin={"r": 0, "t": 0, "l": 0, "b": 0},
                        height=600,
                    )
                    st.plotly_chart(fig_geo)

    with c2:
        if selected:
            st.subheader("Category View")
            with st.spinner("Loading data..."):
                df = data_provider.get_categories_hierarchy(selected)
                categories = df['category'].unique().tolist()
                count_labels = [str(df["num_places_of_that_category"].sum())]

                # Extracting subcategories and ensuring uniqueness
                subcategories = set()
                for sublist in df['top_3_associated_categories_array']:
                    for item in sublist:
                        subcategories.add(item)
                subcategories = list(subcategories)

                # Nodes consist of a single node for count of places, followed by categories and subcategories
                nodes = count_labels + categories + subcategories

                # Create a mapping from node to a unique index
                node_to_index = {node: i for i, node in enumerate(nodes)}

                # Define the source, target, and value for the Sankey diagram
                source = []
                target = []
                value = []

                # Add links from count of places to categories
                count_index = node_to_index[count_labels[0]]
                for category in categories:
                    category_index = node_to_index[category]
                    category_total = df[df['category'] == category]['num_places_of_that_category'].iloc[0]
                    source.append(count_index)
                    target.append(category_index)
                    value.append(category_total)

                # Add links from categories to subcategories
                for _, row in df.iterrows():
                    category_index = node_to_index[row['category']]
                    for subcategory in row['top_3_associated_categories_array']:
                        subcategory_index = node_to_index[subcategory]
                        # Here we divide the category total evenly among the subcategories
                        # Modify this if you have a more precise way to allocate values
                        source.append(category_index)
                        target.append(subcategory_index)
                        value.append(row['num_places_of_that_category'] / len(row['top_3_associated_categories_array']))

                # Create the Sankey diagram
                fig = go.Figure(
                    data=[
                        go.Sankey(
                            node=dict(
                                pad=15,
                                thickness=20,
                                line=dict(color="black", width=0.5),
                                label=nodes,
                            ),
                            link=dict(
                                source=source,
                                target=target,
                                value=value,
                            ),
                        )
                    ]
                )

                # Responsive design
                fig.update_layout(autosize=True)
                # Display the Sankey diagram in Streamlit.
                st.plotly_chart(fig)

    zipcode_input = st.text_input('Enter your zip code:', '10001')
    with st.spinner("Loading data..."):
        df2 = data_provider.get_popular_places(selected, zipcode_input)

        # Create bar chart for popularity
        fig_pop = go.Figure()
        fig_pop.add_trace(
            go.Bar(x=df2['name'], y=df2['popularity'], marker_color='rgb(55, 83, 109)', name='Popularity')
        )

        # Create line chart for provenance rating
        fig_pop.add_trace(
            go.Scatter(
                x=df2['name'],
                y=df2['provenance_rating'],
                mode='lines+markers',
                line=dict(color='rgb(255, 164, 7)'),
                marker=dict(color='rgb(255, 243, 7)', size=7),
                name='Provenance Rating',
                yaxis='y2',
            )
        )

        # Update layout
        fig_pop.update_layout(
            title='Use Plotly to Zoom in and Investigate the Chart',
            xaxis_title='Restaurant',
            yaxis=dict(title='Popularity', range=[0, 1], titlefont=dict(color='rgb(55, 83, 109)')),
            yaxis2=dict(
                title='Provenance Rating',
                overlaying='y',
                side='right',
                range=[0, 5],
                titlefont=dict(color='rgb(26, 118, 255)'),
            ),
            legend=dict(y=1, x=0, bgcolor='rgba(255, 255, 255, 0.5)'),
            margin=dict(l=100, r=100, t=100, b=100),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(245, 245, 245, 1)',
            font=dict(size=12),
            height=600,
        )

        # Responsive design
        fig_pop.update_layout(autosize=True)
        st.subheader("Most popular places by popularity and provenance rating")
        st.plotly_chart(fig_pop, use_container_width=True)


def main():
    logger.info("Preparing data provider")
    logger.info("Data provider ready")
    app()


if __name__ == "__main__":
    main()
