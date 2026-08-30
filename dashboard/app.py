"""
Streamlit dashboard for the Heartbeat Monitoring pipeline.

Run:
    streamlit run app.py

Reads directly from PostgreSQL and auto-refreshes so you can watch live
data come in while producer.py / consumer.py are running.
"""
import sys
import os
import time

import pandas as pd
import psycopg2
import streamlit as st

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "python"))
import config  # noqa: E402

st.set_page_config(page_title="Heartbeat Monitor", layout="wide")
st.title("💓 Real-Time Customer Heart Beat Monitor")

REFRESH_SECONDS = 5


@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )


def load_data(minutes: int = 15) -> pd.DataFrame:
    conn = get_connection()
    query = """
        SELECT customer_id, ts, heart_rate, is_anomaly
        FROM heartbeats
        WHERE ts > now() - interval '%s minutes'
        ORDER BY ts ASC
    """
    return pd.read_sql(query, conn, params=(minutes,))


placeholder = st.empty()
minutes_window = st.sidebar.slider("Time window (minutes)", 1, 60, 15)
st.sidebar.caption(f"Auto-refreshes every {REFRESH_SECONDS}s")

while True:
    df = load_data(minutes_window)
    with placeholder.container():
        if df.empty:
            st.info("No data yet — make sure producer.py and consumer.py are running.")
        else:
            col1, col2, col3 = st.columns(3)
            col1.metric("Readings in window", len(df))
            col2.metric("Active customers", df["customer_id"].nunique())
            col3.metric("Anomalies", int(df["is_anomaly"].sum()))

            st.subheader("Heart rate over time")
            pivot = df.pivot_table(index="ts", columns="customer_id", values="heart_rate")
            st.line_chart(pivot)

            st.subheader("Recent anomalies")
            anomalies = df[df["is_anomaly"]].sort_values("ts", ascending=False).head(20)
            st.dataframe(anomalies, use_container_width=True)

            st.subheader("Raw recent readings")
            st.dataframe(df.sort_values("ts", ascending=False).head(50), use_container_width=True)

    time.sleep(REFRESH_SECONDS)
    st.rerun()
