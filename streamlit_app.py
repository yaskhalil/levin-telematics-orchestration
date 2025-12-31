import json
import os
from pathlib import Path

import pandas as pd
import psycopg2
import streamlit as st


def get_pg_conn():
    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    db = os.getenv("PGDATABASE", "postgres")
    user = os.getenv("PGUSER", "postgres")
    pwd = os.getenv("PGPASSWORD", "postgres")
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd)


def read_sql_df(query: str, params=None) -> pd.DataFrame:
    with get_pg_conn() as conn:
        return pd.read_sql_query(query, conn, params=params)


def list_artifact_dates() -> list[str]:
    root = Path("include/artifacts")
    if not root.exists():
        return []
    dates = []
    for p in root.glob("date=*"):
        if p.is_dir():
            dates.append(p.name.replace("date=", ""))
    return sorted(dates)


st.set_page_config(page_title="Levin Telematics Explorer", layout="wide")
st.title("Levin Telematics Explorer")

with st.sidebar:
    st.header("Connection")
    st.caption("Uses PG* env vars. Defaults: localhost:5432 postgres/postgres postgres")
    st.text_input("PGHOST", value=os.getenv("PGHOST", "localhost"), key="pghost")
    st.text_input("PGPORT", value=os.getenv("PGPORT", "5432"), key="pgport")
    st.text_input("PGDATABASE", value=os.getenv("PGDATABASE", "postgres"), key="pgdb")
    st.text_input("PGUSER", value=os.getenv("PGUSER", "postgres"), key="pguser")
    st.text_input("PGPASSWORD", value=os.getenv("PGPASSWORD", "postgres"), type="password", key="pgpass")

    # Update env vars for this process based on UI input
    os.environ["PGHOST"] = st.session_state["pghost"]
    os.environ["PGPORT"] = st.session_state["pgport"]
    os.environ["PGDATABASE"] = st.session_state["pgdb"]
    os.environ["PGUSER"] = st.session_state["pguser"]
    os.environ["PGPASSWORD"] = st.session_state["pgpass"]

    st.divider()
    artifact_dates = list_artifact_dates()
    selected_date = st.selectbox("Partition date (ds)", options=artifact_dates, index=len(artifact_dates) - 1 if artifact_dates else 0)


col1, col2, col3 = st.columns(3)

try:
    raw_count = read_sql_df(
        "SELECT COUNT(*)::bigint AS n FROM raw_telematics WHERE partition_date = %s",
        params=(selected_date,),
    )["n"].iloc[0] if selected_date else 0
    agg_count = read_sql_df(
        "SELECT COUNT(*)::bigint AS n FROM agg_vehicle_day WHERE partition_date = %s",
        params=(selected_date,),
    )["n"].iloc[0] if selected_date else 0

    with col1:
        st.metric("raw_telematics rows", int(raw_count))
    with col2:
        st.metric("agg_vehicle_day rows", int(agg_count))
    with col3:
        st.metric("Partition date", selected_date or "-")
except Exception as e:
    st.error(f"Postgres connection/query failed: {e}")


tabs = st.tabs(["Curated (agg_vehicle_day)", "Raw (raw_telematics)", "Data quality (artifacts)"])

with tabs[0]:
    st.subheader("agg_vehicle_day")
    if selected_date:
        df = read_sql_df(
            """
            SELECT *
            FROM agg_vehicle_day
            WHERE partition_date = %s
            ORDER BY num_events DESC
            LIMIT 500
            """,
            params=(selected_date,),
        )
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button(
            "Download as CSV",
            df.to_csv(index=False),
            file_name=f"agg_vehicle_day_{selected_date}.csv",
        )

        st.caption("Charts (top 500 vehicles by num_events)")
        st.area_chart(data=df, x="vehicle_id", y="num_events")
        st.area_chart(data=df, x="vehicle_id", y="avg_speed")
        st.area_chart(data=df, x="vehicle_id", y="avg_rpm")
        st.area_chart(data=df, x="vehicle_id", y="max_speed")
        st.area_chart(data=df, x="vehicle_id", y="max_rpm")
        st.area_chart(data=df, x="vehicle_id", y="max_engine_temp")

with tabs[1]:
    st.subheader("raw_telematics")
    if selected_date:
        df = read_sql_df(
            """
            SELECT event_time, partition_date, vehicle_id, device_id, trip_id,
                   speed, rpm, coolant_temp_c, dtc_count,
                   source_file
            FROM raw_telematics
            WHERE partition_date = %s
            ORDER BY event_time ASC
            LIMIT 500
            """,
            params=(selected_date,),
        )
        st.dataframe(df, use_container_width=True, hide_index=True)

with tabs[2]:
    st.subheader("Artifacts")
    if selected_date:
        art_dir = Path("include/artifacts") / f"date={selected_date}"
        metrics_path = art_dir / "metrics.json"
        summary_path = art_dir / "summary.md"

        if metrics_path.exists():
            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            st.json(metrics)
        else:
            st.info("No metrics.json found for this date.")

        if summary_path.exists():
            st.markdown(summary_path.read_text(encoding="utf-8"))
        else:
            st.info("No summary.md found for this date.")


