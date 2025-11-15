import os
import json
import time
from datetime import datetime, timezone

import gspread
from google.oauth2 import service_account
import requests


SHEET_NAME = "Active-Investing"
WORKSHEET_NAME = "Kraken-Screener"

# Columns we will use (P onward). A–O remain untouched.
# NOTE: Name is kept for backwards compatibility, but the value now comes
# from CryptoNews sentiment, not VADER.
HEADER_COLUMNS = [
    "VADER_Compound",     # P (now: CryptoNews-based sentiment score)
    "Articles_Analyzed",  # Q
    "Last_Updated_UTC",   # R
]

# How far back to look for sentiment via CryptoNews.
# CryptoNews supports shortcuts like: last7days, last30days, last60days, etc.
CRYPTO_NEWS_DATE_RANGE = os.getenv("CRYPTO_NEWS_DATE_RANGE", "last30days")

# Throttle between requests so we don't spam the API
SECONDS_BETWEEN_REQUESTS = 1.2


def col_letter(idx: int) -> str:
    """Convert 1-based column index to Excel-style letter."""
    letters = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def get_gspread_worksheet() -> gspread.Worksheet:
    """Authorize with Google and return the Kraken-Screener worksheet."""
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
    worksheet = sheet.worksheet(WORKSHEET_NAME)
    return worksheet


def ensure_headers(worksheet: gspread.Worksheet):
    """Ensure our headers exist from column P onward."""
    start_col_index = 16  # P
    end_col_index = start_col_index + len(HEADER_COLUMNS) - 1  # R

    start_letter = col_letter(start_col_index)
    end_letter = col_letter(end_col_index)
    header_range = f"{start_letter}1:{end_letter}1"

    worksheet.update(header_range, [HEADER_COLUMNS])


def get_tickers(worksheet: gspread.Worksheet) -> list[str]:
    """Read tickers from column A, ignoring row 1."""
    col_a = worksheet.col_values(1)
    if not col_a:
        return []
    return [v.strip().upper() for v in col_a[1:] if v.strip()]


def normalize_ticker_for_news(ticker: str) -> str:
    """
    Normalize Kraken-style tickers for CryptoNews.
    Example:
      'BTC/USD' -> 'BTC'
      'ETH-USDT' -> 'ETH'
    """
    t = ticker.upper()
    if "/" in t:
        t = t.split("/")[0]
    if "-" in t:
        t = t.split("-")[0]
    return t


def compute_cryptonews_sentiment_for_ticker(
    session: requests.Session,
    ticker: str,
    api_token: str,
    date_range: str = CRYPTO_NEWS_DATE_RANGE,
):
    """
    Use CryptoNews /api/v1/stat endpoint to get sentiment counts and
    turn them into a single sentiment score.

    Endpoint pattern (based on docs/examples):
      https://cryptonews-api.com/api/v1/stat
        ?tickers=BTC
        &date=last7days
        &page=1
        &token=YOUR_TOKEN

    Response structure (simplified from docs + published examples):
      {
        "data": {
          "2024-01-15": {
            "BTC": {
              "Neutral": <int>,
              "Positive": <int>,
              "Negative": <int>
            }
          },
          "2024-01-16": { ... },
          ...
        },
        "total_pages": <int>,
        ...
      }

    We:
      - aggregate Neutral/Positive/Negative across all dates & pages
      - compute sentiment = (Positive - Negative) / (Positive + Negative + Neutral)
      - return (sentiment_score, total_articles)
    """
    symbol_for_news = normalize_ticker_for_news(ticker)

    base_url = "https://cryptonews-api.com/api/v1/stat"

    total_positive = 0
    total_negative = 0
    total_neutral = 0

    page = 1
    max_pages_safe_guard = 50  # hard safety cap

    while page <= max_pages_safe_guard:
        params = {
            "tickers": symbol_for_news,
            "date": date_range,
            "page": page,
            "token": api_token,
        }

        try:
            resp = session.get(base_url, params=params, timeout=10)
            # If unauthorized or rate-limited or similar, bail out
            if resp.status_code != 200:
                print(
                    f"CryptoNews request failed for {ticker} "
                    f"(page {page}) with status {resp.status_code}: {resp.text[:200]}"
                )
                break

            payload = resp.json()
        except Exception as e:
            print(f"Error fetching CryptoNews sentiment for {ticker}: {e}")
            break

        data = payload.get("data") or {}
        if not data:
            # No data for this ticker / date range
            break

        for date_str, per_date in data.items():
            coin_data = per_date.get(symbol_for_news)
            if not coin_data:
                continue

            neutral = coin_data.get("Neutral", 0) or 0
            positive = coin_data.get("Positive", 0) or 0
            negative = coin_data.get("Negative", 0) or 0

            total_neutral += neutral
            total_positive += positive
            total_negative += negative

        total_pages = payload.get("total_pages", 1)
        if page >= total_pages:
            break

        page += 1
        # Small delay between pages to be polite
        time.sleep(0.2)

    total_articles = total_neutral + total_positive + total_negative
    if total_articles == 0:
        print(f"No CryptoNews sentiment data for {ticker} (date={date_range}).")
        return None, 0

    # Map counts into a single score ~[-1, 1]
    sentiment_score = (total_positive - total_negative) / float(total_articles)
    sentiment_score = round(sentiment_score, 4)

    print(
        f"Ticker {ticker} (CryptoNews): "
        f"{total_articles} articles (P={total_positive}, "
        f"N={total_negative}, Neu={total_neutral}), "
        f"score={sentiment_score}"
    )

    return sentiment_score, total_articles


def main():
    worksheet = get_gspread_worksheet()
    ensure_headers(worksheet)

    tickers = get_tickers(worksheet)
    if not tickers:
        print("No tickers found.")
        return

    cryptonews_token = os.getenv("CRYPTONEWS_API_TOKEN")
    if not cryptonews_token:
        raise RuntimeError("CRYPTONEWS_API_TOKEN env var missing.")

    session = requests.Session()
    rows_to_write = []

    for idx, ticker in enumerate(tickers, start=2):
        print(f"Processing row {idx}: {ticker}")

        sentiment, count = compute_cryptonews_sentiment_for_ticker(
            session=session,
            ticker=ticker,
            api_token=cryptonews_token,
        )

        timestamp = datetime.now(timezone.utc).isoformat()

        if sentiment is None:
            rows_to_write.append(["", 0, timestamp])
        else:
            rows_to_write.append([sentiment, count, timestamp])

        time.sleep(SECONDS_BETWEEN_REQUESTS)

    # Write P2:Rn
    start_row = 2
    end_row = start_row + len(rows_to_write) - 1

    start_col_letter = col_letter(16)  # P
    end_col_letter = col_letter(16 + len(HEADER_COLUMNS) - 1)  # R
    update_range = f"{start_col_letter}{start_row}:{end_col_letter}{end_row}"

    worksheet.update(update_range, rows_to_write, value_input_option="USER_ENTERED")

    print("CryptoNews sentiment update complete.")


if __name__ == "__main__":
    main()
