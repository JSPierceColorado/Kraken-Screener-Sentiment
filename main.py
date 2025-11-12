import os
import json
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2 import service_account
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from alpaca.data.historical.news import NewsClient
from alpaca.data.requests import NewsRequest


SHEET_NAME = "Active-Investing"
WORKSHEET_NAME = "Kraken-Screener"

# Columns we will use (P onward). A–O remain untouched.
HEADER_COLUMNS = [
    "VADER_Compound",     # P
    "Articles_Analyzed",  # Q
    "Last_Updated_UTC",   # R
]


def get_gspread_worksheet():
    google_creds_json = os.environ["GOOGLE_CREDS_JSON"]
    creds_info = json.loads(google_creds_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = service_account.Credentials.from_service_account_info(
        creds_info, scopes=scopes
    )
    client = gspread.authorize(credentials)

    sheet = client.open(SHEET_NAME)
    return sheet.worksheet(WORKSHEET_NAME)


def ensure_headers(worksheet):
    start_col_index = 16  # P
    end_col_index = start_col_index + len(HEADER_COLUMNS) - 1  # R

    def col_letter(idx):
        letters = ""
        while idx:
            idx, rem = divmod(idx - 1, 26)
            letters = chr(65 + rem) + letters
        return letters

    start_letter = col_letter(start_col_index)
    end_letter = col_letter(end_col_index)
    header_range = f"{start_letter}1:{end_letter}1"

    worksheet.update(header_range, [HEADER_COLUMNS])


def get_tickers(worksheet):
    col_a = worksheet.col_values(1)
    if not col_a:
        return []
    return [v.strip().upper() for v in col_a[1:] if v.strip()]  # skip header


def get_news_client():
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")

    if api_key and secret_key:
        return NewsClient(api_key=api_key, secret_key=secret_key)
    return NewsClient()


def compute_sentiment(news_client, analyzer, ticker, days_back=7):
    now_utc = datetime.now(timezone.utc)
    start_time = now_utc - timedelta(days=days_back)

    request = NewsRequest(
        symbols=ticker,
        start=start_time,
        limit=50,
    )

    try:
        news = news_client.get_news(request)
    except Exception as e:
        print(f"Error fetching news for {ticker}: {e}")
        return None, 0

    df = getattr(news, "df", None)
    if df is None or df.empty:
        return None, 0

    compounds = []

    for row in df.itertuples(index=False):
        headline = getattr(row, "headline", "") or ""
        summary = getattr(row, "summary", "") or ""
        text = (headline + ". " + summary).strip()
        if not text:
            continue

        score = analyzer.polarity_scores(text)["compound"]
        compounds.append(score)

    if not compounds:
        return None, 0

    avg_compound = sum(compounds) / len(compounds)
    return round(avg_compound, 4), len(compounds)


def main():
    worksheet = get_gspread_worksheet()
    ensure_headers(worksheet)

    tickers = get_tickers(worksheet)
    if not tickers:
        print("No tickers found.")
        return

    news_client = get_news_client()
    analyzer = SentimentIntensityAnalyzer()

    rows = []

    for ticker in tickers:
        compound, count = compute_sentiment(news_client, analyzer, ticker)
        timestamp = datetime.now(timezone.utc).isoformat()

        if compound is None:
            rows.append(["", 0, timestamp])
        else:
            rows.append([compound, count, timestamp])

    # Write to P2:R(last_row)
    start_row = 2
    end_row = start_row + len(rows) - 1

    def col_letter(idx):
        letters = ""
        while idx:
            idx, rem = divmod(idx - 1, 26)
            letters = chr(65 + rem) + letters
        return letters

    start_col = 16  # P
    end_col = start_col + len(HEADER_COLUMNS) - 1  # R

    start_letter = col_letter(start_col)
    end_letter = col_letter(end_col)

    update_range = f"{start_letter}{start_row}:{end_letter}{end_row}"

    worksheet.update(update_range, rows, value_input_option="USER_ENTERED")

    print("Sentiment update complete.")


if __name__ == "__main__":
    main()
