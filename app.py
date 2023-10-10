import streamlit as st
import pandas as pd
import numpy as np
from databricks import sql
import os


def main():
    st.title("Streamlit App - Uber Pickups in NYC")
    st.caption("- _developed in a Dash Enterprise :green[Workspace] (web IDE)_")
    st.caption("- _deployed on :orange[Dash Enterprise]_")
    st.caption("- _data in :blue[Databricks]_")
    st.caption("- _data retrieved using :red[databricks-sql-connector]_")

    DATE_COLUMN = "date/time"
    DATA_URL = (
        "https://s3-us-west-2.amazonaws.com/"
        "streamlit-demo-data/uber-raw-data-sep14.csv.gz"
    )

    @st.cache_data
    def load_data(nrows):
        connection = sql.connect(
            server_hostname="plotly-dash-databricks-partner-portal.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/c2252c5e80ca7426",
            access_token="dapi5d33e43457091351f0d6a8ccf2242aab",
        )
        cursor3 = connection.cursor()
        cursor3.execute(f"SELECT * FROM uber_raw_data_sep_14")
        df = cursor3.fetchall_arrow()
        data = df.to_pandas()
        cursor3.close()
        connection.close()
        # data = pd.read_csv(DATA_URL, nrows=nrows)
        lowercase = lambda x: str(x).lower()
        data.rename(lowercase, axis="columns", inplace=True)
        data[DATE_COLUMN] = pd.to_datetime(data[DATE_COLUMN])
        return data

    data = load_data(10000)

    if st.checkbox("Show raw data"):
        st.subheader("Raw data")
        st.write(data)

    hour_to_filter = st.slider("hour", 0, 23, 17)
    filtered_data = data[data[DATE_COLUMN].dt.hour == hour_to_filter]

    st.subheader("Map of all pickups at %s:00" % hour_to_filter)
    st.map(filtered_data)

    st.subheader("Number of pickups by hour")
    hist_values = np.histogram(data[DATE_COLUMN].dt.hour, bins=24, range=(0, 24))[0]
    st.bar_chart(hist_values)

    # Some number in the range 0-23


if __name__ == "__main__":
    main()
