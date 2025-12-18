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
        # ==========================================
        # 1. SPARK TARAFINDAN HESAPLANAN VERİLER
        # ==========================================

        # A) ENDPOINT İSTATİSTİKLERİ (requests_by_endpoint tablosundan)
        rows_endpoint = session.execute(
            "SELECT endpoint, count FROM requests_by_endpoint"
        )
        df_endpoint = pd.DataFrame(list(rows_endpoint))

        # B) IP İSTATİSTİKLERİ (requests_by_ip tablosundan)
        rows_ip = session.execute("SELECT ip_address, count FROM requests_by_ip")
        df_ip = pd.DataFrame(list(rows_ip))

        # C) STATUS İSTATİSTİKLERİ (requests_by_status tablosundan)
        rows_status = session.execute(
            "SELECT status_code, count FROM requests_by_status"
        )
        df_status = pd.DataFrame(list(rows_status))

        # ==========================================
        # 2. ZAMAN SERİSİ İÇİN HAM VERİ (logs_raw)
        # ==========================================
        rows_raw = session.execute("SELECT log_time FROM logs_raw LIMIT 2000")
        df_raw = pd.DataFrame(list(rows_raw))

        # ==========================================
        # 3. VERİ İŞLEME VE GÖRSELLEŞTİRME
        # ==========================================

        if not df_endpoint.empty and not df_ip.empty:
            # --- ENDPOINT İŞLEME ---
            endpoint_summary = (
                df_endpoint.groupby("endpoint")["count"].sum().reset_index()
            )
            top_endpoints = endpoint_summary.sort_values(
                by="count", ascending=False
            ).head(10)

            # --- IP İŞLEME ---
            ip_summary = df_ip.groupby("ip_address")["count"].sum().reset_index()
            top_ips = ip_summary.sort_values(by="count", ascending=False).head(10)

            # --- STATUS İŞLEME ---
            status_summary = (
                df_status.groupby("status_code")["count"].sum().reset_index()
            )

            # --- KPI HESAPLAMA ---
            total_requests = endpoint_summary["count"].sum()
            unique_ips = len(ip_summary)

            # Hata Oranı Hesabı
            errors = status_summary[status_summary["status_code"] >= 400]["count"].sum()
            error_rate = (errors / total_requests * 100) if total_requests > 0 else 0

            # --- KPI KARTLARI ---
            with kpi1:
                st.metric("Toplam İstek (Spark)", f"{total_requests:,}")
            with kpi2:
                st.metric("Tekil IP Sayısı", unique_ips)
            with kpi3:
                st.metric("Hata Oranı", f"%{error_rate:.2f}")

            # --- GRAFİK 1: TOP 10 ENDPOINTS (Spark Verisi) ---
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

            # --- GRAFİK 2: TOP 10 IP (Spark Verisi) ---
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

            # --- GRAFİK 3: HTTP STATUS (Spark Verisi) ---
            fig_status = px.pie(
                status_summary,
                values="count",
                names="status_code",
                title="HTTP Status Dağılımı",
                hole=0.4,
            )
            row3_col1.plotly_chart(fig_status, use_container_width=True)

            # --- GRAFİK 4: ZAMAN SERİSİ (Ham Veri) ---
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
