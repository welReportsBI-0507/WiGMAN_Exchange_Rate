from playwright.sync_api import sync_playwright
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import os

# ================= CONFIG =================
MASTER_FILE = "Exchange Rate.csv"


# ================= LOAD INPUT CSV =================
if os.path.exists(MASTER_FILE):
    df = pd.read_csv(MASTER_FILE)

    if not df.empty and "Date" in df.columns:
        df["Date"] = pd.to_datetime(
            df["Date"],
            dayfirst=True,
            errors="coerce"
        )
        df = df.dropna(subset=["Date"])
    else:
        df = pd.DataFrame()

    print("Input CSV loaded")
else:
    df = pd.DataFrame()
    print("No CSV found, starting fresh")


# ================= DATE RANGE =================
if not df.empty:
    last_date = df["Date"].max()
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

new_df = pd.DataFrame()

for table in tables:
    temp = table.copy()

    if temp.empty:
        continue

    possible_headers = [
        str(c).strip().lower()
        for c in temp.iloc[0].tolist()
    ]

    if "date" in possible_headers:
        temp.columns = temp.iloc[0]
        temp = temp.iloc[1:].reset_index(drop=True)
        temp.columns = [str(c).strip() for c in temp.columns]

        if "Date" in temp.columns:
            new_df = temp
            break

if not new_df.empty:
    new_df = new_df[new_df["Date"].astype(str).str.strip() != "Date"]

    new_df["Date"] = pd.to_datetime(
        new_df["Date"].astype(str).str.strip(),
        format="%d/%m/%Y",
        errors="coerce"
    )

    new_df = new_df.dropna(subset=["Date"])

    print("Rows fetched:", len(new_df))

else:
    print("No new data table found")
    new_df = pd.DataFrame()


# ================= APPEND =================
if df.empty:
    combined_df = new_df.copy()
else:
    combined_df = pd.concat([df, new_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=["Date"], keep="last")


# ================= FULL CARRY FORWARD =================
if not combined_df.empty:
    combined_df = combined_df.sort_values("Date")

    combined_df = combined_df.set_index("Date")

    full_range = pd.date_range(
        start=combined_df.index.min(),
        end=combined_df.index.max(),
        freq="D"
    )

    combined_df = combined_df.reindex(full_range).ffill()

    combined_df = combined_df.reset_index().rename(columns={"index": "Date"})

    print("Full carry forward applied")


# ================= SAVE =================
combined_df["Date"] = combined_df["Date"].dt.strftime("%d/%m/%Y")

combined_df.to_csv(MASTER_FILE, index=False)

print("Done - CSV updated")
