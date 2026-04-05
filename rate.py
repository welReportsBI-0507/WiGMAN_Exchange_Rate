import os
import pandas as pd
import requests
from datetime import datetime, timedelta

# ================= CONFIG =================
MASTER_FILE = "Exchange Rate.xlsx"
TAIL_DAYS = 15
RBI_URL = "https://www.rbi.org.in/scripts/referenceratearchive.aspx"
# ==========================================


# =========================================================
# STEP 1: LOAD MASTER FILE
# =========================================================

if os.path.exists(MASTER_FILE):
    df = pd.read_excel(MASTER_FILE)

    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date'])

    print("✅ Master file loaded")

else:
    print("⚠️ No master file found, starting fresh")
    df = pd.DataFrame()


# =========================================================
# STEP 2: GET DATE RANGE
# =========================================================

if not df.empty:
    last_date = df['Date'].max()
else:
    last_date = datetime.now() - timedelta(days=30)

from_date_dt = last_date + timedelta(days=1)
to_date_dt = datetime.now()

print(f"Fetching data from {from_date_dt.date()} → {to_date_dt.date()}")

if from_date_dt > to_date_dt:
    print("✅ Already up to date")
    exit()

from_date = from_date_dt.strftime("%d/%m/%Y")
to_date = to_date_dt.strftime("%d/%m/%Y")


# =========================================================
# STEP 3: FETCH DATA FROM RBI (NO SELENIUM)
# =========================================================

payload = {
    "hdnFromDate": from_date,
    "hdnToDate": to_date,
    "btnSubmit": "Go"
}

headers = {
    "User-Agent": "Mozilla/5.0"
}

response = requests.post(RBI_URL, data=payload, headers=headers)

tables = pd.read_html(response.text)

# If no table → weekend / holiday
if not tables:
    print("⚠️ No data returned (weekend/holiday)")

    if not df.empty:
        combined_df = df.copy().sort_values('Date')

        cutoff_date = combined_df['Date'].max() - pd.Timedelta(days=TAIL_DAYS)

        old_data = combined_df[combined_df['Date'] < cutoff_date]
        recent_data = combined_df[combined_df['Date'] >= cutoff_date]

        recent_data = recent_data.set_index('Date')

        recent_range = pd.date_range(
            start=recent_data.index.min(),
            end=recent_data.index.max(),
            freq='D'
        )

        recent_data = recent_data.reindex(recent_range).ffill()
        recent_data = recent_data.reset_index().rename(columns={'index': 'Date'})

        combined_df = pd.concat([old_data, recent_data], ignore_index=True)

        combined_df['Date'] = combined_df['Date'].dt.strftime('%d/%m/%Y')
        combined_df.to_excel(MASTER_FILE, index=False)

        print("✅ Gap filled only")

    exit()


# =========================================================
# STEP 4: CLEAN RBI DATA
# =========================================================

new_df = tables[0]

# Fix header
new_df.columns = new_df.iloc[0]
new_df = new_df.iloc[1:].reset_index(drop=True)

# Clean columns
new_df.columns = [str(col).strip() for col in new_df.columns]

# Convert Date
new_df['Date'] = pd.to_datetime(
    new_df['Date'].astype(str).str.strip(),
    format="%d/%m/%Y",
    errors='coerce'
)

new_df = new_df.dropna(subset=['Date'])

print("Rows fetched:", len(new_df))


# =========================================================
# STEP 5: HANDLE EMPTY DATA
# =========================================================

if new_df.empty:
    print("⚠️ No new rows → filling gap only")

    combined_df = df.copy().sort_values('Date')

    cutoff_date = combined_df['Date'].max() - pd.Timedelta(days=TAIL_DAYS)

    old_data = combined_df[combined_df['Date'] < cutoff_date]
    recent_data = combined_df[combined_df['Date'] >= cutoff_date]

    recent_data = recent_data.set_index('Date')

    recent_range = pd.date_range(
        start=recent_data.index.min(),
        end=recent_data.index.max(),
        freq='D'
    )

    recent_data = recent_data.reindex(recent_range).ffill()
    recent_data = recent_data.reset_index().rename(columns={'index': 'Date'})

    combined_df = pd.concat([old_data, recent_data], ignore_index=True)

    combined_df['Date'] = combined_df['Date'].dt.strftime('%d/%m/%Y')
    combined_df.to_excel(MASTER_FILE, index=False)

    print("✅ Gap filled")

    exit()


# =========================================================
# STEP 6: APPEND DATA
# =========================================================

combined_df = pd.concat([df, new_df], ignore_index=True)
combined_df = combined_df.drop_duplicates(subset=['Date'], keep='last')

print("✅ Data appended")


# =========================================================
# STEP 7: TAIL-ONLY FILL
# =========================================================

combined_df = combined_df.sort_values('Date')

cutoff_date = combined_df['Date'].max() - pd.Timedelta(days=TAIL_DAYS)

old_data = combined_df[combined_df['Date'] < cutoff_date]
recent_data = combined_df[combined_df['Date'] >= cutoff_date]

recent_data = recent_data.set_index('Date')

recent_range = pd.date_range(
    start=recent_data.index.min(),
    end=recent_data.index.max(),
    freq='D'
)

recent_data = recent_data.reindex(recent_range).ffill()
recent_data = recent_data.reset_index().rename(columns={'index': 'Date'})

combined_df = pd.concat([old_data, recent_data], ignore_index=True)

print("✅ Recent gaps filled")


# =========================================================
# STEP 8: SAVE
# =========================================================

combined_df['Date'] = combined_df['Date'].dt.strftime('%d/%m/%Y')

combined_df.to_excel(MASTER_FILE, index=False)

print("🎉 DONE: GitHub pipeline complete")
