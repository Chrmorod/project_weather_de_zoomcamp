import os

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

st.set_page_config(page_title="Weather Dashboard", layout="wide")

db_url = os.getenv("STREAMLIT_DB_URL")
if not db_url:
    st.error("Missing STREAMLIT_DB_URL")
    st.stop()

conn = st.connection(
    "weather_db",
    type="sql",
    url=db_url,
)

st.title("Weather Dashboard Spain")


@st.cache_data(ttl=300)
def load_latest() -> pd.DataFrame:
    query = """
        SELECT
            city_name,
            country_code,
            api_timestamp,
            ROUND(temperature_c::numeric, 2) AS temperature_c,
            ROUND(feels_like_c::numeric, 2) AS feels_like_c,
            ROUND(humidity_pct::numeric, 2) AS humidity_pct,
            ROUND(pressure_hpa::numeric, 2) AS pressure_hpa,
            ROUND(wind_speed_ms::numeric, 2) AS wind_speed_ms,
            weather_main,
            weather_description,
            clouds_pct
        FROM marts.weather_latest_by_city
        ORDER BY city_name
    """
    return conn.query(query, ttl=300)


@st.cache_data(ttl=300)
def load_daily_summary() -> pd.DataFrame:
    query = """
        SELECT
            city_name,
            weather_date,
            ROUND(avg_temperature_c::numeric, 2) AS avg_temperature_c,
            ROUND(min_temperature_c::numeric, 2) AS min_temperature_c,
            ROUND(max_temperature_c::numeric, 2) AS max_temperature_c,
            ROUND(avg_humidity_pct::numeric, 2) AS avg_humidity_pct,
            ROUND(avg_pressure_hpa::numeric, 2) AS avg_pressure_hpa,
            ROUND(avg_wind_speed_ms::numeric, 2) AS avg_wind_speed_ms
        FROM marts.weather_daily_summary
        ORDER BY weather_date, city_name
    """
    return conn.query(query, ttl=300)


@st.cache_data(ttl=300)
def load_risk_map() -> pd.DataFrame:
    query = """
        SELECT
            city_name,
            lat,
            lon,
            ROUND(temperature_c::numeric, 2) AS temperature_c,
            ROUND(wind_speed_ms::numeric, 2) AS wind_speed_ms,
            risk_type,
            risk_level
        FROM marts.cities_at_risk
        WHERE lat IS NOT NULL
          AND lon IS NOT NULL
    """
    return conn.query(query, ttl=300)


latest_df = load_latest()
daily_df = load_daily_summary()

if latest_df.empty:
    st.warning("No hay datos en marts.weather_latest_by_city")
    st.stop()

if daily_df.empty:
    st.warning("No hay datos en marts.weather_daily_summary")
    st.stop()

latest_df["api_timestamp"] = pd.to_datetime(latest_df["api_timestamp"])
daily_df["weather_date"] = pd.to_datetime(daily_df["weather_date"])

cities = sorted(latest_df["city_name"].dropna().unique().tolist())
selected_city = st.selectbox("City", cities)

city_latest = latest_df[latest_df["city_name"] == selected_city].iloc[0]
city_daily = daily_df[daily_df["city_name"] == selected_city].copy()

if city_daily.empty:
    st.warning(f"No daily data available for {selected_city}")
    st.stop()

# KPIs
c1, c2, c3, c4 = st.columns(4)
c1.metric("Current Temperature", f"{city_latest['temperature_c']:.2f} °C")
c2.metric("Feels like", f"{city_latest['feels_like_c']:.2f} °C")
c3.metric("Humidity", f"{city_latest['humidity_pct']:.2f} %")
c4.metric("Wind Speed", f"{city_latest['wind_speed_ms']:.2f} m/s")

st.subheader(f"Latest available data: {selected_city}")
st.dataframe(
    latest_df[latest_df["city_name"] == selected_city],
    use_container_width=True,
    hide_index=True,
)

# 1) Daily average temperature line chart
st.subheader(f"Daily evolution of average temperature — {selected_city}")

line_chart = (
    alt.Chart(city_daily)
    .mark_line(point=True, color="#E4572E")
    .encode(
        x=alt.X(
            "yearmonthdate(weather_date):T",
            title="Date",
            axis=alt.Axis(format="%d-%m"),
        ),
        y=alt.Y(
            "avg_temperature_c:Q",
            title="Average Temperature (°C)",
        ),
        tooltip=[
            alt.Tooltip("city_name:N", title="City"),
            alt.Tooltip("yearmonthdate(weather_date):T", title="Date"),
            alt.Tooltip("avg_temperature_c:Q", title="Avg. Temperature", format=".2f"),
        ],
    )
    .properties(height=350)
)

st.altair_chart(line_chart, use_container_width=True)

# 2) Grouped bars of min / avg / max temperature by day
st.subheader(f"Grouped bars of minimum, average and maximum per day — {selected_city}")

temp_long = city_daily.melt(
    id_vars=["city_name", "weather_date"],
    value_vars=["min_temperature_c", "avg_temperature_c", "max_temperature_c"],
    var_name="metric",
    value_name="temperature_c",
)

metric_labels = {
    "min_temperature_c": "Minimum",
    "avg_temperature_c": "Average",
    "max_temperature_c": "Maximum",
}
temp_long["metric"] = temp_long["metric"].map(metric_labels)

grouped_chart = (
    alt.Chart(temp_long)
    .mark_bar()
    .encode(
        x=alt.X(
            "yearmonthdate(weather_date):T",
            title="Date",
            axis=alt.Axis(format="%d-%m"),
        ),
        y=alt.Y("temperature_c:Q", title="Temperature (°C)"),
        xOffset="metric:N",
        color=alt.Color(
            "metric:N",
            title="Metric",
            scale=alt.Scale(
                domain=["Minimum", "Average", "Maximum"],
                range=["#3B82F6", "#D16F51", "#EF4444"],
            ),
        ),
        tooltip=[
            alt.Tooltip("city_name:N", title="City"),
            alt.Tooltip("yearmonthdate(weather_date):T", title="Date"),
            alt.Tooltip("metric:N", title="Metric"),
            alt.Tooltip("temperature_c:Q", title="Temperature", format=".2f"),
        ],
    )
    .properties(height=380)
)

st.altair_chart(grouped_chart, use_container_width=True)

# 3) Horizontal ranking of cities
st.subheader("Horizontal ranking of cities")

ranking_metric = st.radio(
    "Ranking by",
    options=["Average Temperature", "Average Wind Speed"],
    horizontal=True,
)

ranking_df = (
    daily_df.groupby("city_name", as_index=False)
    .agg(
        avg_temperature_c=("avg_temperature_c", "mean"),
        avg_wind_speed_ms=("avg_wind_speed_ms", "mean"),
    )
)

ranking_df["avg_temperature_c"] = ranking_df["avg_temperature_c"].round(2)
ranking_df["avg_wind_speed_ms"] = ranking_df["avg_wind_speed_ms"].round(2)

metric_config = {
    "Average Temperature": {
        "col": "avg_temperature_c",
        "title": "Ranking of cities by average temperature (°C)",
        "color": "#E4572E",
    },
    "Average Wind Speed": {
        "col": "avg_wind_speed_ms",
        "title": "Ranking of cities by average wind speed (m/s)",
        "color": "#1D4ED8",
    },
}

ranking_col = metric_config[ranking_metric]["col"]
ranking_title = metric_config[ranking_metric]["title"]
ranking_color = metric_config[ranking_metric]["color"]

ranking_plot_df = ranking_df.sort_values(by=ranking_col, ascending=False).copy()

ranking_chart = (
    alt.Chart(ranking_plot_df)
    .mark_bar(color=ranking_color)
    .encode(
        x=alt.X(f"{ranking_col}:Q", title=ranking_title),
        y=alt.Y("city_name:N", sort="-x", title="City"),
        tooltip=[
            alt.Tooltip("city_name:N", title="City"),
            alt.Tooltip("avg_temperature_c:Q", title="Avg. Temperature", format=".2f"),
            alt.Tooltip("avg_wind_speed_ms:Q", title="Avg. Wind Speed", format=".2f"),
        ],
    )
    .properties(height=500)
)

st.altair_chart(ranking_chart, use_container_width=True)

st.subheader("Current comparison between cities")
st.dataframe(
    latest_df[
        [
            "city_name",
            "temperature_c",
            "feels_like_c",
            "humidity_pct",
            "pressure_hpa",
            "wind_speed_ms",
            "weather_main",
            "weather_description",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)

# 4) Risk map
st.subheader("Map of at-risk cities")

try:
    risk_df = load_risk_map()

    if risk_df.empty:
        st.info("No at-risk cities available with the current criteria.")
    else:
        risk_df["risk_level"] = risk_df["risk_level"].fillna(1).astype(int)

        risk_df["color_r"] = risk_df["risk_level"].apply(
            lambda x: 220 if x >= 3 else 255 if x == 2 else 100
        )
        risk_df["color_g"] = risk_df["risk_level"].apply(
            lambda x: 60 if x >= 3 else 160 if x == 2 else 180
        )
        risk_df["color_b"] = risk_df["risk_level"].apply(
            lambda x: 60 if x >= 3 else 60 if x == 2 else 200
        )
        risk_df["radius"] = risk_df["risk_level"].apply(
            lambda x: 50000 if x >= 3 else 35000 if x == 2 else 20000
        )

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=risk_df,
            get_position='[lon, lat]',
            get_radius="radius",
            get_fill_color='[color_r, color_g, color_b, 180]',
            pickable=True,
        )

        view_state = pdk.ViewState(
            latitude=40.2,
            longitude=-3.7,
            zoom=5.2,
            pitch=0,
        )

        tooltip = {
            "html": """
            <b>City:</b> {city_name} <br/>
            <b>Risk:</b> {risk_type} <br/>
            <b>Temperature:</b> {temperature_c} °C <br/>
            <b>Wind:</b> {wind_speed_ms} m/s
            """,
            "style": {"backgroundColor": "white", "color": "black"},
        }

        st.pydeck_chart(
            pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                tooltip=tooltip,
                map_style="light"
            )
        )
        
except Exception as e:
    st.info(
        "Risk map not available yet. Create marts.cities_at_risk with lat/lon, risk_type and risk_level."
    )
    st.caption(str(e))