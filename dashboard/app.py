import time

import pandas as pd
import plotly.express as px
import streamlit as st
from cassandra.cluster import Cluster

# Sayfa Ayarları (Wide mode ile ekranı genişletiyoruz)
st.set_page_config(page_title="NASA Log Analizi", layout="wide")

st.title("NASA Web Server Log Analizi (Real-Time)")


# Cassandra Bağlantısı
@st.cache_resource
def get_session():
    cluster = Cluster(["cassandra"])  # Docker servis adı
    session = cluster.connect("nasa_logs")
    return session


session = get_session()

# --- SAYFA DÜZENİ (LAYOUT) ---
# 1. Satır: KPI Kartları
kpi1, kpi2, kpi3 = st.columns(3)

# 2. Satır: Grafikler (Top Endpoints ve Top IPs)
row2_col1, row2_col2 = st.columns(2)

# 3. Satır: Grafikler (Pie Chart ve Time Series)
row3_col1, row3_col2 = st.columns(2)

# Otomatik Yenileme Döngüsü
placeholder = st.empty()

while True:
    try:
        # Verileri Çek (Son 5000 satır)
        rows = session.execute(
            "SELECT endpoint, status_code, ip_address, log_time FROM logs_raw LIMIT 5000"
        )
        df = pd.DataFrame(list(rows))

        if not df.empty:
            # --- KPI HESAPLAMALARI ---
            total_requests = len(df)
            unique_ips = df["ip_address"].nunique()
            # Status code integer olduğu için hata oranını hesapla
            error_count = len(df[df["status_code"] >= 400])
            error_rate = (
                (error_count / total_requests * 100) if total_requests > 0 else 0
            )

            # KPI'ları Yazdır
            with kpi1:
                st.metric("Toplam İstek (Son 5000)", total_requests)
            with kpi2:
                st.metric("Tekil IP Sayısı", unique_ips)
            with kpi3:
                st.metric("Hata Oranı (4xx/5xx)", f"%{error_rate:.2f}")

            # --- GRAFİK 1: TOP 10 ENDPOINTS (SOL) ---
            top_endpoints = df["endpoint"].value_counts().head(10).reset_index()
            top_endpoints.columns = ["Endpoint", "Count"]
            fig_endpoint = px.bar(
                top_endpoints,
                x="Count",
                y="Endpoint",
                orientation="h",
                title="Top 10 Popüler Sayfalar (Endpoint)",
                color="Count",
                color_continuous_scale="Viridis",
            )
            fig_endpoint.update_layout(
                yaxis=dict(autorange="reversed")
            )  # En yükseği en üste al
            row2_col1.plotly_chart(fig_endpoint, use_container_width=True)

            # --- GRAFİK 2: TOP 10 IP ADRESLERİ (SAĞ - YENİ EKLENDİ) ---
            top_ips = df["ip_address"].value_counts().head(10).reset_index()
            top_ips.columns = ["IP Address", "Request Count"]
            fig_ip = px.bar(
                top_ips,
                x="Request Count",
                y="IP Address",
                orientation="h",
                title="Top 10 Aktif IP Adresi",
                color="Request Count",
                color_continuous_scale="Magma",
            )
            fig_ip.update_layout(yaxis=dict(autorange="reversed"))
            row2_col2.plotly_chart(fig_ip, use_container_width=True)

            # --- GRAFİK 3: HTTP STATUS DAĞILIMI (PASTA) ---
            status_counts = df["status_code"].value_counts().reset_index()
            status_counts.columns = ["Status Code", "Count"]
            fig_status = px.pie(
                status_counts,
                values="Count",
                names="Status Code",
                title="HTTP Durum Kodu Dağılımı",
                hole=0.4,
            )
            row3_col1.plotly_chart(fig_status, use_container_width=True)

            # --- GRAFİK 4: ZAMAN SERİSİ (ÇİZGİ) ---
            df["time_sec"] = pd.to_datetime(df["log_time"]).dt.floor("S")
            time_series = df.groupby("time_sec").size().reset_index(name="Requests")

            fig_time = px.line(
                time_series,
                x="time_sec",
                y="Requests",
                title="Saniye Bazlı Trafik Akışı",
                markers=True,
            )
            row3_col2.plotly_chart(fig_time, use_container_width=True)

        else:
            st.warning("Veri bekleniyor... (Producer ve Spark çalışıyor mu?)")

        # 2 Saniye bekle
        time.sleep(2)
        st.rerun()

    except Exception as e:
        # Hata olursa ekrana bas ama durma
        st.error(f"Bağlantı Hatası: {e}")
        time.sleep(5)
        st.rerun()
