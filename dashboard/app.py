import time

import pandas as pd
import plotly.express as px
import streamlit as st
from cassandra.cluster import Cluster

st.set_page_config(page_title="NASA Log Analizi", layout="wide")

st.title("🚀 NASA Web Server Log Analizi (Real-Time)")


@st.cache_resource
def get_session():
    cluster = Cluster(["cassandra"])
    session = cluster.connect("nasa_logs")
    return session


session = get_session()

kpi1, kpi2, kpi3 = st.columns(3)
col1, col2 = st.columns(2)
col3 = st.container()

placeholder = st.empty()

while True:
    try:
        rows = session.execute(
            "SELECT endpoint, status_code, ip_address, log_time FROM logs_raw LIMIT 5000"
        )
        df = pd.DataFrame(list(rows))

        if not df.empty:
            total_requests = len(df)
            unique_ips = df["ip_address"].nunique()
            error_rate = len(df[df["status_code"] >= 400]) / total_requests * 100

            with kpi1:
                st.metric("Toplam İstek (Son 5000)", total_requests)
            with kpi2:
                st.metric("Tekil IP Sayısı", unique_ips)
            with kpi3:
                st.metric("Hata Oranı (%)", f"{error_rate:.2f}%")

            # grafikler

            top_endpoints = df["endpoint"].value_counts().head(10).reset_index()
            top_endpoints.columns = ["Endpoint", "Count"]
            fig_endpoint = px.bar(
                top_endpoints,
                x="Count",
                y="Endpoint",
                orientation="h",
                title="Top 10 Endpoints",
            )
            col1.plotly_chart(fig_endpoint, use_container_width=True)

            status_counts = df["status_code"].value_counts().reset_index()
            status_counts.columns = ["Status Code", "Count"]
            fig_status = px.pie(
                status_counts,
                values="Count",
                names="Status Code",
                title="HTTP Status Dağılımı",
            )
            col2.plotly_chart(fig_status, use_container_width=True)

            df["time_sec"] = pd.to_datetime(df["log_time"]).dt.floor("S")
            time_series = df.groupby("time_sec").size().reset_index(name="Requests")
            fig_time = px.line(
                time_series,
                x="time_sec",
                y="Requests",
                title="Saniye Bazlı Trafik Akışı",
            )
            with col3:
                st.plotly_chart(fig_time, use_container_width=True)

        else:
            st.warning("Henüz veri gelmedi veya Kafka/Spark çalışmıyor.")

        time.sleep(2)

        try:
            st.rerun()
        except AttributeError:
            st.experimental_rerun()

    except Exception as e:
        st.error(f"Bir hata oluştu: {e}")
        time.sleep(5)
