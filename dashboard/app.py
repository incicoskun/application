import time

import pandas as pd
import plotly.express as px
import streamlit as st
from cassandra.cluster import Cluster

# Sayfa Ayarları
st.set_page_config(page_title="NASA Log Analizi (Kappa Architecture)", layout="wide")

st.title("NASA Web Server Log Analizi")


# Cassandra Bağlantısı
@st.cache_resource
def get_session():
    cluster = Cluster(["cassandra"])
    session = cluster.connect("nasa_logs")
    return session


session = get_session()

# --- SAYFA DÜZENİ ---
kpi1, kpi2, kpi3 = st.columns(3)
row2_col1, row2_col2 = st.columns(2)
row3_col1, row3_col2 = st.columns(2)

placeholder = st.empty()

while True:
    try:
        # spark data pull
        rows_endpoint = session.execute(
            "SELECT endpoint, count FROM requests_by_endpoint"
        )
        df_endpoint = pd.DataFrame(list(rows_endpoint))

        rows_ip = session.execute("SELECT ip_address, count FROM requests_by_ip")
        df_ip = pd.DataFrame(list(rows_ip))

        rows_status = session.execute(
            "SELECT status_code, count FROM requests_by_status"
        )
        df_status = pd.DataFrame(list(rows_status))

        # raw log
        rows_raw = session.execute("SELECT log_time FROM logs_raw LIMIT 2000")
        df_raw = pd.DataFrame(list(rows_raw))

        # veri işleme kısımları
        if not df_endpoint.empty and not df_ip.empty:
            # endpoint
            endpoint_summary = (
                df_endpoint.groupby("endpoint")["count"].sum().reset_index()
            )
            top_endpoints = endpoint_summary.sort_values(
                by="count", ascending=False
            ).head(10)

            # ip
            ip_summary = df_ip.groupby("ip_address")["count"].sum().reset_index()
            top_ips = ip_summary.sort_values(by="count", ascending=False).head(10)

            # status
            status_summary = (
                df_status.groupby("status_code")["count"].sum().reset_index()
            )

            # kpi calc
            total_requests = endpoint_summary["count"].sum()
            unique_ips = len(ip_summary)

            # error_rate
            errors = status_summary[status_summary["status_code"] >= 400]["count"].sum()
            error_rate = (errors / total_requests * 100) if total_requests > 0 else 0

            # kpi
            with kpi1:
                st.metric("Toplam İstek (Spark)", f"{total_requests:,}")
            with kpi2:
                st.metric("Tekil IP Sayısı", unique_ips)
            with kpi3:
                st.metric("Hata Oranı", f"%{error_rate:.2f}")

            # top 10 endpoint graph
            top_endpoints.columns = ["Endpoint", "Count"]
            fig_endpoint = px.bar(
                top_endpoints,
                x="Count",
                y="Endpoint",
                orientation="h",
                title="Top 10 Sayfalar",
                color="Count",
                color_continuous_scale="Viridis",
            )
            fig_endpoint.update_layout(yaxis=dict(autorange="reversed"))
            row2_col1.plotly_chart(fig_endpoint, use_container_width=True)

            # top 10 ip graph
            top_ips.columns = ["IP Address", "Count"]
            fig_ip = px.bar(
                top_ips,
                x="Count",
                y="IP Address",
                orientation="h",
                title="Top 10 IP Adresi",
                color="Count",
                color_continuous_scale="Magma",
            )
            fig_ip.update_layout(yaxis=dict(autorange="reversed"))
            row2_col2.plotly_chart(fig_ip, use_container_width=True)

            # http graph
            fig_status = px.pie(
                status_summary,
                values="count",
                names="status_code",
                title="HTTP Status Dağılımı",
                hole=0.4,
            )
            row3_col1.plotly_chart(fig_status, use_container_width=True)

            # time raw log
            if not df_raw.empty:
                df_raw["time_sec"] = pd.to_datetime(df_raw["log_time"]).dt.floor("S")
                time_series = (
                    df_raw.groupby("time_sec").size().reset_index(name="Requests")
                )
                fig_time = px.line(
                    time_series,
                    x="time_sec",
                    y="Requests",
                    title="Canlı Trafik Akışı (Logs Raw)",
                    markers=True,
                )
                row3_col2.plotly_chart(fig_time, use_container_width=True)

        else:
            st.warning("Veri bekleniyor... Spark istatistik tabloları henüz dolmadı.")

        time.sleep(2)
        st.rerun()

    except Exception as e:
        st.error(f"Hata: {e}")
        time.sleep(5)
        st.rerun()
