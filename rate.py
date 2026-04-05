from playwright.sync_api import sync_playwright
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import os

# ================= CONFIG =================
MASTER_FILE = "Exchange Rate.xlsx"
TAIL_DAYS = 15

# ================= LOAD MASTER =================
if os.path.exists(MASTER_FILE):
    df = pd.read_excel(MASTER_FILE)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date'])
    print("Master file loaded")
else:
    df = pd.DataFrame()
    print("No master file found, starting fresh")

# ================= DATE RANGE =================
if not df.empty:
    last_date = df['Date'].max()
else:
    last_date = datetime.now() - timedelta(days=30)

from_date = (last_date + timedelta(days=1)).strftime("%d/%m/%Y")
to_date = datetime.now().strftime("%d/%m/%Y")

print("Fetching from", from_date, "to", to_date)


# ================= FETCH RBI =================
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    page.goto("https://www.rbi.org.in/scripts/referenceratearchive.aspx")

    page.evaluate("""
    (args) => {
        let from = document.getElementById('txtFromDate');
        let to = document.getElementById('txtToDate');

        from.removeAttribute('readonly');
        to.removeAttribute('readonly');

        from.value = args.fromDate;
        to.value = args.toDate;

        from.dispatchEvent(new Event('change'));
        to.dispatchEvent(new Event('change'));
    }
    """, {
        "fromDate": from_date,
        "toDate": to_date
    })

    page.click("#btnSubmit")
    page.wait_for_timeout(5000)

    html = page.content()
    browser.close()


# ================= PARSE =================
tables = pd.read_html(StringIO(html))

if len(tables) < 3:
    print("No new data (weekend/holiday)")
    new_df = pd.DataFrame()
else:
    new_df = tables[2].copy()

    new_df.columns = new_df.iloc[0]
    new_df = new_df.iloc[1:].reset_index(drop=True)
    new_df.columns = [str(col).strip() for col in new_df.columns]

    date_series = new_df.iloc[:, 0]

    new_df['Date'] = pd.to_datetime(
        date_series.astype(str).str.strip(),
        format="%d/%m/%Y",
        errors='coerce'
    )

    new_df = new_df.dropna(subset=['Date'])

    print("Rows fetched:", len(new_df))


# ================= APPEND =================
combined_df = pd.concat([df, new_df], ignore_index=True)
combined_df = combined_df.drop_duplicates(subset=['Date'], keep='last')


# ================= CARRY FORWARD (TAIL ONLY) =================
if not combined_df.empty:
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

    print("Carry forward applied")


# ================= SAVE =================
combined_df['Date'] = combined_df['Date'].dt.strftime('%d/%m/%Y')
combined_df.to_excel(MASTER_FILE, index=False)

print("Done - file updated")
