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
    """
    Ensure our headers exist in row 1 from column P onward.
    Does NOT touch columns A–O.
    """
    start_col_index = 16  # P
    end_col_index = start_col_index + len(HEADER_COLUMNS) - 1  # R

    start_letter = col_letter(start_col_index)
    end_letter = col_letter(end_col_index)
    header_range = f"{start_letter}1:{end_letter}1"

    worksheet.update(header_range, [HEADER_COLUMNS])


def get_tickers(worksheet: gspread.Worksheet) -> list[str]:
    """
    Read tickers from column A, starting at row 2.
    Row 1 is treated as header and ignored.
    """
    col_a = worksheet.col_values(1)  # column A
    if not col_a:
        return []

    # Skip row 1 (header)
    tickers = [value.strip().upper() for value in col_a[1:] if value.strip()]
    return tickers


def get_news_client() -> NewsClient:
    """
    Create Alpaca news client using env vars if available.
    """
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")

    if api_key and secret_key:
        return NewsClient(api_key=api_key, secret_key=secret_key)
    else:
        return NewsClient()


def compute_sentiment_for_ticker(
    news_client: NewsClient,
    analyzer: SentimentIntensityAnalyzer,
    ticker: str,
    days_back: int = 7,
):
    """
    Fetch recent news for `ticker` and compute VADER sentiment.

    Returns:
        (compound_avg, article_count)
        or (None, 0) if no usable news.
    """
    now_utc = datetime.now(timezone.utc)
    start_time = now_utc - timedelta(days=days_back)

    request = NewsRequest(
        symbols=ticker,
        start=start_time,
        limit=50,  # per-ticker cap
    )

    try:
        news = news_client.get_news(request)
    except Exception as e:
        print(f"Error fetching news for {ticker}: {e}")
        return None, 0

    compounds = []

    # `news` is iterable; each item usually has headline & summary
    for article in news:
        # Be defensive: article can be model or dict depending on version
        if isinstance(article, dict):
            headline = article.get("headline", "") or ""
            summary = article.get("summary", "") or ""
        else:
            headline = getattr(article, "headline", "") or ""
            summary = getattr(article, "summary", "") or ""

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
        print("No tickers found in column A.")
        return

    news_client = get_news_client()
    analyzer = SentimentIntensityAnalyzer()

    rows_to_write = []

    for ticker in tickers:
        if not ticker:
            rows_to_write.append(["", 0, datetime.now(timezone.utc).isoformat()])
            continue

        compound, count = compute_sentiment_for_ticker(news_client, analyzer, ticker)
        timestamp = datetime.now(timezone.utc).isoformat()

        if compound is None:
            rows_to_write.append(["", 0, timestamp])
        else:
            rows_to_write.append([compound, count, timestamp])

    # Determine range: from P2 down to R(last_row)
    start_row = 2
    end_row = start_row + len(rows_to_write) - 1

    start_col_index = 16  # P
    end_col_index = start_col_index + len(HEADER_COLUMNS) - 1  # R

    start_col_letter = col_letter(start_col_index)
    end_col_letter = col_letter(end_col_index)
    update_range = f"{start_col_letter}{start_row}:{end_col_letter}{end_row}"

    worksheet.update(update_range, rows_to_write, value_input_option="USER_ENTERED")

    print("Sentiment update complete.")


if __name__ == "__main__":
    main()
