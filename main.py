import os
import json
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2 import service_account
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import requests


SHEET_NAME = "Active-Investing"
WORKSHEET_NAME = "Kraken-Screener"

# Columns we will use (P onward). A–O remain untouched.
HEADER_COLUMNS = [
    "VADER_Compound",     # P
    "Articles_Analyzed",  # Q
    "Last_Updated_UTC",   # R
]

# How far back to look for news per ticker (in days)
NEWS_LOOKBACK_DAYS = 30


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


def normalize_ticker_for_news(ticker: str) -> str:
    """
    Optionally normalize symbols (if you use stuff like BTC/USD).
    For Finnhub company-news this usually wants stock symbols like AAPL, TSLA.
    """
    t = ticker.upper()
    # If you have crypto pairs, try stripping separators so BTC/USD -> BTC
    if "/" in t:
        t = t.split("/")[0]
    if "-" in t:
        t = t.split("-")[0]
    return t


def compute_sentiment_for_ticker(
    analyzer: SentimentIntensityAnalyzer,
    ticker: str,
    api_key: str,
    days_back: int = NEWS_LOOKBACK_DAYS,
):
    """
    Fetch recent company news from Finnhub for `ticker` and compute VADER sentiment.

    Returns:
        (compound_avg, article_count)
        or (None, 0) if no usable news.
    """
    today = datetime.utcnow().date()
    from_date = (today - timedelta(days=days_back)).strftime("%Y-%m-%d")
    to_date = today.strftime("%Y-%m-%d")

    symbol_for_news = normalize_ticker_for_news(ticker)

    params = {
        "symbol": symbol_for_news,
        "from": from_date,
        "to": to_date,
        "token": api_key,
    }

    try:
        resp = requests.get(
            "https://finnhub.io/api/v1/company-news",
            params=params,
            timeout=10,
        )
        resp.raise_for_status()
        articles = resp.json()
    except Exception as e:
        print(f"Error fetching Finnhub news for {ticker} ({symbol_for_news}): {e}")
        return None, 0

    if not isinstance(articles, list) or not articles:
        print(
            f"No Finnhub company news for {ticker} ({symbol_for_news}) "
            f"from {from_date} to {to_date}."
        )
        return None, 0

    compounds = []

    for article in articles:
        headline = (article.get("headline") or "").strip()
        summary = (article.get("summary") or "").strip()
        text = (headline + ". " + summary).strip()
        if not text:
            continue

        score = analyzer.polarity_scores(text)["compound"]
        compounds.append(score)

    count = len(compounds)
    if count == 0:
        print(
            f"Finnhub returned {len(articles)} articles for {ticker}, "
            f"but none had usable text."
        )
        return None, 0

    avg_compound = sum(compounds) / count
    print(
        f"Ticker {ticker} ({symbol_for_news}): {count} articles, "
        f"avg compound={avg_compound:.4f}"
    )
    return round(avg_compound, 4), count


def main():
    worksheet = get_gspread_worksheet()
    ensure_headers(worksheet)

    tickers = get_tickers(worksheet)
    if not tickers:
        print("No tickers found in column A.")
        return

    finnhub_api_key = os.getenv("FINNHUB_API_KEY")
    if not finnhub_api_key:
        raise RuntimeError("FINNHUB_API_KEY environment variable is not set.")

    analyzer = SentimentIntensityAnalyzer()

    rows_to_write = []

    for ticker in tickers:
        if not ticker:
            rows_to_write.append(["", 0, datetime.now(timezone.utc).isoformat()])
            continue

        compound, count = compute_sentiment_for_ticker(
            analyzer=analyzer,
            ticker=ticker,
            api_key=finnhub_api_key,
        )
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
