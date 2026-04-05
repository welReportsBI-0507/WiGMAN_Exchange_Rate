import os
import time
import pandas as pd
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

MASTER_FILE = "Exchange Rate.xlsx"
TAIL_DAYS = 15

# ================= LOAD DATA =================
if os.path.exists(MASTER_FILE):
    df = pd.read_excel(MASTER_FILE)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date'])
else:
    df = pd.DataFrame()

last_date = df['Date'].max() if not df.empty else datetime.now() - timedelta(days=30)

from_date = (last_date + timedelta(days=1)).strftime("%d/%m/%Y")
to_date = datetime.now().strftime("%d/%m/%Y")

# ================= SELENIUM SETUP =================
options = Options()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(options=options)

driver.get("https://www.rbi.org.in/scripts/referenceratearchive.aspx")
time.sleep(3)

# Fill dates
driver.execute_script(f"""
document.getElementById('txtFromDate').value = '{from_date}';
document.getElementById('txtToDate').value = '{to_date}';
""")

driver.find_element(By.ID, "btnSubmit").click()
time.sleep(3)

html = driver.page_source
driver.quit()

# ================= PARSE =================
tables = pd.read_html(html)

if not tables:
    print("No data → fill only")
    exit()

new_df = tables[0]
new_df.columns = new_df.iloc[0]
new_df = new_df.iloc[1:].reset_index(drop=True)

new_df['Date'] = pd.to_datetime(
    new_df['Date'].astype(str).str.strip(),
    format="%d/%m/%Y",
    errors='coerce'
)

new_df = new_df.dropna(subset=['Date'])

# ================= APPEND =================
combined_df = pd.concat([df, new_df], ignore_index=True)
combined_df = combined_df.drop_duplicates(subset=['Date'], keep='last')

# ================= TAIL FILL =================
combined_df = combined_df.sort_values('Date')

cutoff = combined_df['Date'].max() - pd.Timedelta(days=TAIL_DAYS)

old = combined_df[combined_df['Date'] < cutoff]
recent = combined_df[combined_df['Date'] >= cutoff]

recent = recent.set_index('Date')

full_range = pd.date_range(
    start=recent.index.min(),
    end=recent.index.max(),
    freq='D'
)

recent = recent.reindex(full_range).ffill()
recent = recent.reset_index().rename(columns={'index': 'Date'})

combined_df = pd.concat([old, recent], ignore_index=True)

# ================= SAVE =================
combined_df['Date'] = combined_df['Date'].dt.strftime('%d/%m/%Y')
combined_df.to_excel(MASTER_FILE, index=False)

print("DONE")
