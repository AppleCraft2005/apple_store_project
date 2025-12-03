from fastapi import FastAPI, HTTPException
import json
import os
import uvicorn

app = FastAPI(
    title="Apple Store Specs Internal API",
    description="Microservice untuk menyediakan data spesifikasi produk Apple",
    version="1.0.0"
)

# --- LOAD DATA ---
# Logika Path Cerdas:
# Jika di Docker (kita mount ke /source_data), pathnya absolut
if os.path.exists('/source_data/master_specs.json'):
    DATA_PATH = '/source_data/master_specs.json'
else:
    # Jika di Laptop (Relative path dari folder script)
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_PATH = os.path.join(BASE_DIR, 'source_data', 'master_specs.json')

print(f"--> Loading database from: {DATA_PATH}")

try:
    with open(DATA_PATH, 'r') as f:
        SPECS_DB = json.load(f)
    print(f"--> Database loaded successfully: {len(SPECS_DB)} products.")
except Exception as e:
    print(f"--> ERROR loading JSON: {e}")
    SPECS_DB = {}

# --- ENDPOINTS ---

@app.get("/")
def health_check():
    """Cek apakah server hidup"""
    return {"status": "active", "service": "Apple Specs API", "docs_url": "http://localhost:8000/docs"}

@app.get("/api/spec")
def get_product_specification(name: str):
    """
    Mengambil spesifikasi produk berdasarkan nama lengkap.
    Contoh query: ?name=Apple iPhone 14
    """
    # Cari di database (Case sensitive match dari key JSON)
    product_data = SPECS_DB.get(name)
    
    if product_data:
        # Simulasi struktur respons API profesional
        return {
            "status": "success",
            "timestamp": "now",
            "data": product_data
        }
    else:
        # Return 404 jika produk tidak ada di JSON kita
        raise HTTPException(status_code=404, detail=f"Product '{name}' not found in internal database.")

if __name__ == "__main__":
    # Jalankan server di port 8000
    print("--> Starting Server on Port 8000...")
    uvicorn.run(app, host="0.0.0.0", port=8000)