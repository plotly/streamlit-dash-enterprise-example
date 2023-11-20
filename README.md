# dash-streamlit-dbsql-app

This is an example application written in Streamlit that uses Databricks SQL endpoint to query data from a table created from Marketplace.

## Developer instructions

### Databricks-side setup

- Create a Databricks SQL endpoint. We recommend a Serverless endpoint of any size.
- Use the Databricks Marketplace to install the `Places - Free New York City Sample dataset` provided by Foursquare
- [Optional] Prepare a Mapbox token


### Local setup

- Clone the repo
- Install Python 3.10+ and [hatch](https://github.com/pypa/hatch)
- Create a file `.env` with environment variables as shown in `.env.example`
- Run:

```bash
hatch run sync
```
To install the dependencies.

To run the app:
```bash
streamlit run src/dash_streamlit_dbsql_app/app.py
```

## Deployment instructions

- Prepare a Docker container with relevant dependencies
- Provide the necessary env variables (described in the `.env.example` file).
- The entrypoint should look like:

```bash
streamlit run src/dash_streamlit_dbsql_app/app.py
```
