# Gunakan image Python yang ringan
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install library sistem yang diperlukan (jika ada) dan dependencies Python
# Kita install pandas, streamlit, sqlalchemy, psycopg2, plotly (untuk grafik bagus), dan scikit-learn
RUN pip install --no-cache-dir \
    streamlit \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    plotly \
    scikit-learn \
    joblib \
    matplotlib \
    seaborn

# Expose port default Streamlit
EXPOSE 8501

# Perintah untuk menjalankan aplikasi
# Kita asumsikan folder 'scripts' akan di-mount ke '/app/scripts'
CMD ["streamlit", "run", "/app/scripts/dashboard/app.py", "--server.address=0.0.0.0"]