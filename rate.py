import os
import pandas as pd
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright

MASTER_FILE = "Exchange Rate.xlsx"
TAIL_DAYS = 15


# ================= LOAD =================
if os.path.exists(MASTER_FILE):
    df = pd.read_excel(MASTER_FILE)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date'])
else:
    df = pd.DataFrame()

last_date = df['Date'].max() if not df.empty else datetime.now() - timedelta(days=30)

from_date = (last_date + timedelta(days=1)).strftime("%d/%m/%Y")
to_date = datetime.now().strftime("%d/%m/%Y")

print(f"Fetching {from_date} → {to_date}")


# ================= FETCH RBI =================
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    page.goto("https://www.rbi.org.in/scripts/referenceratearchive.aspx")

    page.fill("#txtFromDate", from_date)
    page.fill("#txtToDate", to_date)

    page.click("#btnSubmit")

    try:
        page.wait_for_selector("table", timeout=5000)
        html = page.content()
    except:
        print("⚠️ No data (weekend/holiday)")
        browser.close()
        html = None

    browser.close()


# ================= PARSE =================
if html:
    tables = pd.read_html(html)
else:
    tables = []

if not tables:
    print("No new data → fill only")

    if not df.empty:
        combined_df = df.copy().sort_values('Date')

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

        combined_df['Date'] = combined_df['Date'].dt.strftime('%d/%m/%Y')
        combined_df.to_excel(MASTER_FILE, index=False)

        print("✅ Gap filled")

    exit()


# ================= CLEAN =================
new_df = tables[0]

new_df.columns = new_df.iloc[0]
new_df = new_df.iloc[1:].reset_index(drop=True)

new_df.columns = [str(col).strip() for col in new_df.columns]

new_df['Date'] = pd.to_datetime(
    new_df['Date'].astype(str).str.strip(),
    format="%d/%m/%Y",
    errors='coerce'
)

new_df = new_df.dropna(subset=['Date'])

print("Rows fetched:", len(new_df))


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

print("🎉 DONE")
