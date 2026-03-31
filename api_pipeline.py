import requests
import pandas as pd
import sqlite3
from datetime import datetime

# STEP 1: Extract (API request)
url = "https://api.exchangerate.host/latest"
response = requests.get(url)
data = response.json()

# STEP 2: Transform
base = data['base']
date = data['date']
rates = data['rates']

# Extract a few currencies
usd = rates['USD']
eur = rates['EUR']
gbp = rates['GBP']
zar = rates['ZAR']

# Create DataFrame
df = pd.DataFrame([{
    "base_currency": base,
    "date": date,
    "usd_rate": usd,
    "eur_rate": eur,
    "gbp_rate": gbp,
    "zar_rate": zar,
    "timestamp": datetime.now()
}])

# STEP 3: Load (SQLite)
conn = sqlite3.connect("api_pipeline.db")

df.to_sql("exchange_rates", conn, if_exists="append", index=False)

print("API pipeline executed successfully!")