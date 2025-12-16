import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import joblib
import os
import json
import numpy as np
import plotly.express as px  # <--- Pastikan ini ada di paling atas file

def calculate_quality_metrics(df, target_col='quantity'):
    """
    Menghitung Missing Values, Outliers, dan Simple Drift
    """
    metrics = {}
    
    # 1. Missing Values (Nilai Hilang)
    missing_count = df[target_col].isnull().sum()
    total_rows = len(df)
    missing_pct = (missing_count / total_rows) * 100
    metrics['missing_text'] = f"{missing_count} Baris ({missing_pct:.2f}%)"
    
    # 2. Outliers (Pencilan - Metode IQR)
    # Data penjualan sering ada lonjakan, kita anggap outlier jika > Q3 + 1.5*IQR
    Q1 = df[target_col].quantile(0.25)
    Q3 = df[target_col].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    outliers = df[(df[target_col] < lower_bound) | (df[target_col] > upper_bound)]
    outlier_count = len(outliers)
    outlier_pct = (outlier_count / total_rows) * 100
    metrics['outlier_text'] = f"{outlier_count} Baris ({outlier_pct:.2f}%)"
    
    # 3. Data Drift (Pergeseran Data Sederhana)
    # Membandingkan Rata-rata Penjualan Paruh Pertama vs Paruh Kedua Data
    split_idx = int(len(df) * 0.5)
    past_data = df.iloc[:split_idx][target_col]
    current_data = df.iloc[split_idx:][target_col]
    
    past_mean = past_data.mean()
    current_mean = current_data.mean()
    drift_pct = ((current_mean - past_mean) / past_mean) * 100
    
    metrics['drift_value'] = f"{drift_pct:.1f}%"
    metrics['drift_status'] = "Stabil" if abs(drift_pct) < 10 else "Terdeteksi Drift"
    
    return metrics

# --- KONFIGURASI HALAMAN ---
st.set_page_config(page_title="Apple Store Executive Dashboard", layout="wide")

# --- KONEKSI DATABASE ---
# Menggunakan nama service docker 'apple_store_postgres'
DB_HOST = 'apple_store_postgres'
DB_USER = 'admin'
DB_PASS = 'admin'
DB_NAME = 'apple_dwh'
CONN_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

# Path Model ML (Sesuai mounting volume di Docker)
MODEL_PATH = '/app/scripts/ml/models/warranty_risk_model.pkl'

# --- FUNGSI HELPER ---
@st.cache_data(ttl=600) # Cache data selama 10 menit biar cepat
def load_data(query):
    try:
        engine = create_engine(CONN_STRING)
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Gagal koneksi database: {e}")
        return pd.DataFrame()

def load_model():
    if os.path.exists(MODEL_PATH):
        return joblib.load(MODEL_PATH)
    return None

# --- JUDUL ---
st.title("Apple Store: Data Warehouse & AI Dashboard")
st.markdown("Dashboard ini terhubung langsung ke **Data Warehouse PostgreSQL** dan Model **Machine Learning**.")

# --- TABS ---
tab1, tab2 = st.tabs(["Business Intelligence", "AI Risk Prediction"])

# ==============================================================================
# TAB 1: BUSINESS INTELLIGENCE (Visualisasi Data DWH)
# ==============================================================================
with tab1:
    # 1. KPI CARDS
    st.subheader("Key Performance Indicators (KPI)")
    
    # Query Agregasi Cepat
    sql_kpi = """
    SELECT 
        SUM(total_amount) as total_revenue,
        SUM(quantity) as total_units,
        COUNT(DISTINCT sales_key) as total_transactions
    FROM dwh.fact_sales
    """
    df_kpi = load_data(sql_kpi)
    
    if not df_kpi.empty:
        col1, col2, col3 = st.columns(3)
        rev = df_kpi['total_revenue'][0]
        qty = df_kpi['total_units'][0]
        trx = df_kpi['total_transactions'][0]
        
        col1.metric("Total Pendapatan", f"${rev:,.0f}")
        col2.metric("Unit Terjual", f"{qty:,.0f}")
        col3.metric("Total Transaksi", f"{trx:,.0f}")
    
    st.divider()

    # 2. GRAFIK TREN & KATEGORI
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.subheader("Analisis Tren Penjualan")
        
        # --- FILTER INTERAKTIF ---
        filter_col1, filter_col2 = st.columns(2)
        
        with filter_col1:
            years_df = load_data("SELECT DISTINCT year FROM dwh.dim_date ORDER BY year DESC")
            years_list = years_df['year'].tolist() if not years_df.empty else [2023, 2024]
            selected_year = st.selectbox("Pilih Tahun", years_list, index=0, key='trend_year')
            
        with filter_col2:
            view_mode = st.selectbox("Mode Tampilan", ["Bulanan (Jan-Dec)", "Per Kuartal (Q1-Q4)"], key='trend_mode')

        # --- LOGIKA QUERY & SORTING ---
        
        # Inisialisasi variabel agar tidak NameError
        x_axis = "" 
        orders_map = {} 

        if view_mode == "Bulanan (Jan-Dec)":
            # 1. Query (Tetap sama)
            sql_trend = f"""
                SELECT 
                    month_name, 
                    SUM(total_revenue) as revenue,
                    SUM(total_items_sold) as units
                FROM marts.view_monthly_sales
                WHERE year = {selected_year}
                GROUP BY month_name
            """
            x_axis = "month_name"
            
            # Definisi Urutan Bulan
            month_order = [
                'January', 'February', 'March', 'April', 'May', 'June', 
                'July', 'August', 'September', 'October', 'November', 'December'
            ]
            orders_map = {x_axis: month_order}
        
        # PENTING: Tentukan aturan urutan untuk Plotly
        # Jika mode bulanan, pakai month_order. Jika kuartal, pakai default (karena Q1, Q2 sudah urut abjad)
        orders_map = {}
        if view_mode == "Bulanan (Jan-Dec)":
            orders_map = {x_axis: month_order}
            
        else: # Mode Kuartal
            sql_trend = f"""
                SELECT 
                    quarter, 
                    SUM(total_revenue) as revenue,
                    SUM(total_items_sold) as units
                FROM marts.view_monthly_sales
                WHERE year = {selected_year}
                GROUP BY quarter
                ORDER BY quarter ASC
            """
            x_axis = "quarter_label" # Kita akan buat kolom baru ini di bawah
            
            # Urutan Kuartal
            quarter_order = ['Q1', 'Q2', 'Q3', 'Q4']
            orders_map = {x_axis: quarter_order}

        # --- LOAD DATA ---
        df_trend = load_data(sql_trend)
        
        if not df_trend.empty:
            # Pre-processing Data Frame
            if view_mode == "Bulanan (Jan-Dec)":
                # Bersihkan spasi jika ada
                # Bersihkan spasi
                df_trend['month_name'] = df_trend['month_name'].astype(str).str.strip()
                
                # Bikin kolom sementara untuk sorting angka (1-12)
                # Kita pakai dictionary comprehension untuk mapping nama bulan ke angka
                month_map = {name: i for i, name in enumerate(month_order)}
                
                # Map nama bulan ke angka, lalu sort
                df_trend['sort_key'] = df_trend['month_name'].map(month_map)
                df_trend = df_trend.sort_values('sort_key')
            else:
                # Bikin label Q1, Q2..
                df_trend['quarter_label'] = "Q" + df_trend['quarter'].astype(str)

            # --- RENDER CHART ---
            # category_orders adalah KUNCI agar Plotly menurut
            # --- RENDER CHART (Update bagian ini) ---
            fig_trend = px.line(
                df_trend, 
                x=x_axis, 
                y='revenue', 
                markers=True,
                title=f"Tren {view_mode} - Tahun {selected_year}",
                labels={'revenue': 'Total Pendapatan ($)', x_axis: 'Periode'},
                
                # === TAMBAHKAN BARIS SAKTI INI ===
                category_orders=orders_map 
            )
            
            # Tambahkan Dual Axis (Unit Terjual)
            # fig_trend.add_scatter(
            #     x=df_trend[x_axis], 
            #     y=df_trend['units'], 
            #     mode='lines+markers', 
            #     name='Unit Terjual', 
            #     yaxis='y2',
            #     line=dict(dash='dot', color='orange') # Garis putus-putus biar beda
            # )
            
            # Layout Sumbu Ganda
            fig_trend.update_layout(
                yaxis=dict(title="Pendapatan ($)"),
                yaxis2=dict(title="Unit Terjual", overlaying='y', side='right'),
                legend=dict(x=0, y=1.1, orientation='h'),
                hovermode="x unified"
            )
            
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.warning(f"Tidak ada data transaksi untuk tahun {selected_year}.")

    with col_right:
        st.subheader(f"Penjualan per Kategori ({selected_year})")
        
        # --- QUERY VIA DATA MART (ROLAP) ---
        # Kita ambil dari view yang sama, tapi di-group by category
        sql_cat = f"""
            SELECT 
                category_name, 
                SUM(total_revenue) as revenue
            FROM marts.view_monthly_sales
            WHERE year = {selected_year}  -- Filter tahun agar sinkron dengan grafik sebelah
            GROUP BY category_name
            ORDER BY revenue DESC
        """
        
        df_cat = load_data(sql_cat)
        
        if not df_cat.empty:
            # Render Pie Chart
            fig_pie = px.pie(
                df_cat, 
                values='revenue', 
                names='category_name', 
                hole=0.4,
                color_discrete_sequence=px.colors.sequential.RdBu
            )
            
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("Data kategori tidak tersedia.")

# ... (Kode KPI Card di atas tetap sama) ...
    st.divider()

    st.divider()

    # --- BAGIAN 3: DETAIL PRODUK PER KATEGORI (SUMBER: DATA MART) ---
    st.subheader("📱 Detail Penjualan Produk per Kategori")
    
    # 1. Ambil daftar kategori dari Data Mart
    # (Menggunakan DISTINCT agar kita hanya mengambil kategori yang pernah terjual)
    df_cats = load_data("SELECT DISTINCT category_name FROM marts.view_product_sales_yearly")
    cat_list = df_cats['category_name'].tolist()
    
    # 2. Filter Kategori
    # key='mart_cat_select' ditambahkan agar tidak error duplicate ID
    selected_cat = st.selectbox("Pilih Kategori Produk:", ["Semua Kategori"] + cat_list, key='mart_cat_select')
    
    # 3. Query Dinamis ke Data Mart (ROLAP)
    # Kita melakukan SUM() lagi untuk menggabungkan data dari semua tahun
    if selected_cat == "Semua Kategori":
        sql_product = """
            SELECT 
                product_name, 
                SUM(total_units) as total_units,
                SUM(total_revenue) as total_revenue
            FROM marts.view_product_sales_yearly
            GROUP BY product_name
            ORDER BY total_units DESC
            LIMIT 10
        """
    else:
        sql_product = f"""
            SELECT 
                product_name, 
                SUM(total_units) as total_units,
                SUM(total_revenue) as total_revenue
            FROM marts.view_product_sales_yearly
            WHERE category_name = '{selected_cat}'
            GROUP BY product_name
            ORDER BY total_units DESC
            LIMIT 10
        """
    
    df_products = load_data(sql_product)
    
    col_prod1, col_prod2 = st.columns([2, 1])
    
    with col_prod1:
        # Bar Chart Horizontal
        if not df_products.empty:
            fig_prod = px.bar(
                df_products, 
                x='total_units', 
                y='product_name', 
                orientation='h',
                title=f"Top 10 Produk Terlaris ({selected_cat}) - All Time",
                labels={'total_units': 'Unit Terjual', 'product_name': 'Nama Produk'},
                color='total_units',
                color_continuous_scale='Blues',
                text_auto='.2s'
            )
            # Layout bersih
            fig_prod.update_layout(
                yaxis={'categoryorder':'total ascending'},
                showlegend=False,
            )
            st.plotly_chart(fig_prod, use_container_width=True)
            
    with col_prod2:
        # Tampilkan Tabel Data
        if not df_products.empty:
            st.write(f"**Data Detail ({selected_cat})**")
            st.dataframe(
                df_products, 
                column_config={
                    "product_name": "Produk",
                    "total_units": "Qty",
                    "total_revenue": st.column_config.NumberColumn("Omzet", format="$%d")
                },
                hide_index=True, 
                height=400,
                use_container_width=True
            )

    st.divider()

    # --- BAGIAN 4: ANALISIS KLAIM GARANSI (DATA MART VERSION) ---
    st.subheader("Analisis Risiko & Klaim Garansi")
    
    # 1. Query Data LANGSUNG dari Mart
    sql_warranty = """
        SELECT 
            category_name,
            product_name,
            spec_chipset,
            spec_ram,
            spec_battery,
            total_warranty_claims
        FROM marts.view_product_risk_analysis
        ORDER BY total_warranty_claims DESC
    """
    
    df_warranty = load_data(sql_warranty)
    
    if not df_warranty.empty:
        # --- FILTER DI ATAS ---
        # Ambil daftar kategori unik
        cat_list = df_warranty['category_name'].unique().tolist()
        # Tambahkan opsi 'Semua Kategori' di awal list
        cat_list.insert(0, "Semua Kategori")
        
        selected_cat_warranty = st.selectbox(
            "Pilih Kategori Produk untuk Analisis Detail:", 
            cat_list,
            key='warranty_cat_select'
        )

        # --- LOGIKA FILTER ---
        if selected_cat_warranty == "Semua Kategori":
            # Jika pilih semua, pakai data utuh
            df_filtered = df_warranty.copy()
            chart_title = "Top 10 Produk Bermasalah (Global)"
        else:
            # Jika pilih kategori tertentu, filter datanya
            df_filtered = df_warranty[df_warranty['category_name'] == selected_cat_warranty].copy()
            chart_title = f"Top 10 Produk Bermasalah: {selected_cat_warranty}"
        
        # --- LAYOUT: KIRI (GRAFIK) | KANAN (TABEL) ---
        col_grafik, col_tabel = st.columns([1.5, 1]) 
        
        with col_grafik:
            st.markdown(f"##### {chart_title}")
            
            # Ambil Top 10 produk dengan klaim terbanyak dari data yang sudah difilter
            df_chart_prod = df_filtered.sort_values('total_warranty_claims', ascending=False).head(10)
            
            if not df_chart_prod.empty:
                fig_prod_risk = px.bar(
                    df_chart_prod,
                    x='total_warranty_claims',
                    y='product_name',
                    orientation='h',
                    labels={'total_warranty_claims': 'Jumlah Klaim', 'product_name': ''},
                    color='total_warranty_claims',
                    color_continuous_scale='Reds',
                    text_auto=True
                )
                fig_prod_risk.update_layout(yaxis={'categoryorder':'total ascending'}, margin=dict(l=0, r=0, t=0, b=0))
                st.plotly_chart(fig_prod_risk, use_container_width=True)
            else:
                st.info("Tidak ada data klaim untuk kategori ini.")

        with col_tabel:
            st.markdown(f"##### Spesifikasi Teknis (Top 10)")
            
            # Tampilkan tabel spesifikasi yang sesuai dengan grafik di samping
            st.dataframe(
                df_chart_prod[['product_name', 'spec_chipset', 'spec_ram', 'spec_battery']],
                column_config={
                    "product_name": "Nama Produk",
                    "spec_chipset": "Chipset",
                    "spec_ram": "RAM",
                    "spec_battery": "Baterai"
                },
                hide_index=True,
                use_container_width=True,
                height=400
            )
            
    else:
        st.info("Belum ada data klaim garansi di Data Mart.")

# ... (Pastikan kode ini menjorok ke dalam / indent di bawah if selected == "Dashboard Utama":) ...

    st.markdown("---")
    st.subheader("🌍 Sebaran Penjualan Global (Geospatial Analysis)")

    try:
        # --- 1. LANGSUNG QUERY DATA DI SINI ---
        map_query = """
        SELECT 
            ds.country,
            COUNT(DISTINCT fs.sales_key) as total_transactions,
            SUM(fs.quantity) as total_items_sold,
            SUM(fs.quantity * fs.unit_price) as total_revenue
        FROM dwh.fact_sales fs
        JOIN dwh.dim_store ds ON fs.store_key = ds.store_key
        GROUP BY ds.country
        ORDER BY total_revenue DESC;
        """
        
        # Eksekusi query langsung ke DataFrame
        df_map = load_data(map_query)
        
        # --- 2. TAMPILKAN VISUALISASI ---
        # Bagi layar: Kiri (Peta Besar), Kanan (Tabel Kecil)
        col_map, col_data = st.columns([2, 1])
        
        with col_map:
            # 1. Render Map Choropleth (Data Penjualan)
            fig_map = px.choropleth(
                df_map,
                locations="country",
                locationmode='country names',
                color="total_revenue",
                hover_name="country",
                hover_data={
                    "total_revenue": ":,.0f",
                    "total_transactions": ":,.0f",
                    "country": False
                },
                color_continuous_scale=px.colors.sequential.Plasma,
            )
            
            # 2. UPDATE PENTING: Menampilkan Negara Non-Apple Store
            fig_map.update_geos(
                visible=True,           # Tampilkan peta dasar
                showcountries=True,     # Tampilkan garis batas negara
                countrycolor="white",   # Warna garis batas
                showland=True,          # Tampilkan daratan (land)
                landcolor="#f0f0f0",    # Warna negara yang TIDAK ada datanya (Abu-abu muda)
                showocean=True,         # Opsional: Tampilkan laut
                oceancolor="#eefaff",   # Opsional: Warna laut (Biru muda banget)
                projection_type='natural earth' # Proyeksi lebih realistis (bukan datar)
            )

            # 3. Rapikan Margin
            fig_map.update_layout(
                margin={"r":0,"t":0,"l":0,"b":0},
                height=400,
                # Pindahkan legend warna ke bawah agar tidak menutupi peta
                coloraxis_colorbar=dict(
                    title="Revenue ($)",
                    orientation="h",
                    yanchor="bottom", y=0,
                    xanchor="center", x=0.5,
                    thickness=15
                )
            )
            
            st.plotly_chart(fig_map, use_container_width=True)
            
        with col_data:
            st.caption("📋 Top Negara by Revenue")
            # Tampilkan tabel data ringkas di samping peta
            st.dataframe(
                df_map[['country', 'total_revenue']].style.format({"total_revenue": "{:,.0f}"}),
                hide_index=True,
                use_container_width=True,
                height=400
            )
            
    except Exception as e:
        st.error(f"Gagal memuat peta geospatial: {e}")

# ==============================================================================
# TAB 2: MACHINE LEARNING INSIGHTS (MONITORING ONLY)
# ==============================================================================

with tab2:
# ... (setelah bagian Product Scanner di Tab 2) ...

# ... (kode sebelumnya sama) ...
    
    st.markdown("---")
    st.header("Laporan Performa Model Forecasting (XGBoost)")
    
    # Path File Metrics Forecast
    FORECAST_METRICS = '/app/scripts/ml/models/metrics_forecast.json'
    if not os.path.exists(FORECAST_METRICS): FORECAST_METRICS = './scripts/ml/models/metrics_forecast.json'
    
    if os.path.exists(FORECAST_METRICS):
        with open(FORECAST_METRICS, 'r') as f:
            f_metrics = json.load(f)
            
        # 1. KPI Utama
        st.info(f"Model Forecasting Terakhir Dilatih: **{f_metrics['last_updated']}**")
        
        # Tampilkan Rata-rata Akurasi Global (Jika ada, atau hitung dari detail)
        # Kita hitung rata-rata akurasi dari semua kategori
        accuracies = [v.get('accuracy_pct', 0) for k, v in f_metrics['categories'].items()]
        avg_acc = sum(accuracies) / len(accuracies) if accuracies else 0
        
        m1, m2 = st.columns(2)
        m1.metric("Rata-rata Kesalahan (MAE Global)", f"{f_metrics['overall']['avg_mae']:.1f} Unit")
        m2.metric("Rata-rata Akurasi Estimasi", f"{avg_acc:.1f}%", help="100% - MAPE (Rata-rata persentase error)")

        
# 2. Persiapan Data untuk Tabel & Grafik
        cat_data = []
        for cat, val in f_metrics['categories'].items():
            cat_data.append({
                'Kategori': cat,
                'Rata-rata Jual (Unit)': val['avg_sales'],
                'Error (MAE)': val['mae'],
                'Error (RMSE)': val.get('rmse', 0),
                'MAPE (%)': val.get('mape_pct', 0), # <--- DATA BARU
                'Akurasi (%)': val.get('accuracy_pct', 0), 
            })
            
        df_forecast_perf = pd.DataFrame(cat_data).sort_values('Akurasi (%)', ascending=False)
        


        # 3. Visualisasi
        col_f1, col_f2 = st.columns([1.5, 1]) # Tabel lebih lebar dikit
        
        with col_f1:
            st.subheader("Perbandingan Akurasi per Kategori")
            fig_acc = px.bar(
                df_forecast_perf, 
                x='Akurasi (%)', 
                y='Kategori', 
                orientation='h',
                title="Tingkat Akurasi (Semakin Panjang Semakin Baik)",
                text_auto=True,
                color='Akurasi (%)',
                color_continuous_scale='Greens' 
            )
            fig_acc.update_layout(yaxis={'categoryorder':'total ascending'}, showlegend=False)
            st.plotly_chart(fig_acc, use_container_width=True)
            
        with col_f2:
            st.subheader("Detail Evaluasi Lengkap")
            # Tampilkan Tabel Lengkap dengan MAPE
            st.dataframe(
                df_forecast_perf,
                column_config={
                    "Akurasi (%)": st.column_config.ProgressColumn(
                        "Akurasi (1-MAPE)", 
                        format="%.1f%%", 
                        min_value=0, 
                        max_value=100
                    ),
                    "MAPE (%)": st.column_config.NumberColumn(
                        "MAPE", 
                        format="%.1f%%",
                        help="Mean Absolute Percentage Error (Persentase kesalahan rata-rata)"
                    ),
                    "Rata-rata Jual (Unit)": st.column_config.NumberColumn(format="%.1f"),
                    "Error (MAE)": st.column_config.NumberColumn(format="%.1f"),
                    "Error (RMSE)": st.column_config.NumberColumn(format="%.1f")
                },
                hide_index=True,
                use_container_width=True,
                height=400
            )
            
    else:
        st.warning("File metrics_forecast.json belum tersedia. Jalankan script train_cat_forecast.py.")

    st.divider()
    st.subheader("Prediksi Penjualan Masa Depan Per Kategori")

    # Load Data Future
    FUTURE_FILE = '/app/scripts/ml/future_sales_forecast.csv' 
    if not os.path.exists(FUTURE_FILE): FUTURE_FILE = './scripts/ml/future_sales_forecast.csv'
    
    if os.path.exists(FUTURE_FILE):
        df_future = pd.read_csv(FUTURE_FILE)
        
        # PENTING: Pastikan kolom tanggal dikenali sebagai datetime
        df_future['date'] = pd.to_datetime(df_future['date'])
        
        # --- INTERACTIVE CONTROLS ---
        col_f1, col_f2 = st.columns(2)
        
        with col_f1:
            # Pilihan Kategori
            cat_options = df_future['category'].unique()
            selected_cat_fc = st.multiselect("Pilih Kategori:", cat_options, default=['Smartphone', 'Laptop'], key='fc_cat')
            
        with col_f2:
            # Pilihan Durasi Waktu
            time_options = ["7 Hari", "1 Bulan", "3 Bulan", "6 Bulan"]
            selected_time = st.selectbox("Pilih Rentang Waktu:", time_options, index=1, key='fc_time') # Default 1 Bulan

        # --- LOGIKA FILTER WAKTU ---
        # Tentukan berapa hari yang mau ditampilkan
        days_to_show = 7
        if selected_time == "1 Bulan": days_to_show = 30
        elif selected_time == "3 Bulan": days_to_show = 90
        elif selected_time == "6 Bulan": days_to_show = 180
        
        # Ambil tanggal awal (hari pertama prediksi)
        start_date = df_future['date'].min()
        # Hitung tanggal akhir batas potong
        end_date = start_date + pd.Timedelta(days=days_to_show)
        
        # Filter Data (Potong sesuai tanggal)
        df_chart_fc = df_future[
            (df_future['category'].isin(selected_cat_fc)) & 
            (df_future['date'] <= end_date)
        ]
        
        if not df_chart_fc.empty:
            # Render Grafik
            fig_fc = px.line(
                df_chart_fc, 
                x='date', 
                y='predicted_units', 
                color='category',
                markers=True, # Tampilkan titik agar jelas per hari
                title=f"Forecast Penjualan: {selected_time} ke Depan",
                labels={'predicted_units': 'Estimasi Unit', 'date': 'Tanggal Prediksi'}
            )
            st.plotly_chart(fig_fc, use_container_width=True)
            
            # Tampilkan Tabel Ringkasan Total
            with st.expander("Lihat Ringkasan Angka"):
                # Pivot agar enak dibaca (Tanggal ke bawah, Kategori ke samping)
                df_pivot_fc = df_chart_fc.pivot(index='date', columns='category', values='predicted_units')
                
                # Tambahkan baris Total di bawah (Opsional, biar keren)
                st.dataframe(df_pivot_fc.style.format("{:.0f}"))
                
                total_pred = df_chart_fc.groupby('category')['predicted_units'].sum()
                st.caption(f"Total estimasi penjualan selama {selected_time}:")
                st.write(total_pred)
                
        else:
            st.info("Silakan pilih kategori produk.")
    else:
        st.warning("File prediksi masa depan belum tersedia. Jalankan script 'predict_future_sales.py'.")