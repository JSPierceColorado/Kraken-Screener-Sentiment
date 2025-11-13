import os
import json
import time
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

# Throttle between requests so we don't spam Finnhub
SECONDS_BETWEEN_REQUESTS = 1.2

# Env-var-controlled max articles (default 20)
MAX_ARTICLES_PER_TICKER = int(os.getenv("MAX_ARTICLES_PER_TICKER", "20"))


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
    """Normalize tickers for Finnhub."""
    t = ticker.upper()
    if "/" in t:
        t = t.split("/")[0]
    if "-" in t:
        t = t.split("-")[0]
    return t


def compute_sentiment_for_ticker(
    session: requests.Session,
    analyzer: SentimentIntensityAnalyzer,
    ticker: str,
    api_key: str,
    days_back: int = NEWS_LOOKBACK_DAYS,
):
    """
    Fetch recent company news from Finnhub and compute VADER sentiment.
    Applies MAX_ARTICLES_PER_TICKER limit.
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

    url = "https://finnhub.io/api/v1/company-news"

    # Allow up to 1 retry for 429 rate limits
    for attempt in range(2):
        try:
            resp = session.get(url, params=params, timeout=10)

            if resp.status_code == 429:
                reset_header = resp.headers.get("X-RateLimit-Reset")
                if reset_header:
                    try:
                        reset_ts = int(reset_header)
                        now_ts = int(time.time())
                        wait_secs = max(0, reset_ts - now_ts + 1)
                    except ValueError:
                        wait_secs = 60
                else:
                    wait_secs = 60

                print(
                    f"429 rate limit for {ticker} — sleeping {wait_secs}s then retrying..."
                )
                time.sleep(wait_secs)
                continue

            resp.raise_for_status()
            articles = resp.json()
            break

        except Exception as e:
            print(f"Error fetching Finnhub news for {ticker}: {e}")
            return None, 0
    else:
        print(f"Failed to fetch Finnhub news for {ticker} after retries.")
        return None, 0

    # No articles
    if not isinstance(articles, list) or not articles:
        print(f"No news returned for {ticker}.")
        return None, 0

    # Apply article limit
    articles = articles[:MAX_ARTICLES_PER_TICKER]

    compounds = []
    for article in articles:
        headline = (article.get("headline") or "").strip()
        summary = (article.get("summary") or "").strip()
        text = (headline + ". " + summary).strip()
        if not text:
            continue

        score = analyzer.polarity_scores(text)["compound"]
        compounds.append(score)

    if not compounds:
        return None, 0

    avg_compound = round(sum(compounds) / len(compounds), 4)

    print(
        f"Ticker {ticker}: {len(compounds)} articles analyzed "
        f"(max={MAX_ARTICLES_PER_TICKER}), avg={avg_compound}"
    )

    return avg_compound, len(compounds)


def main():
    worksheet = get_gspread_worksheet()
    ensure_headers(worksheet)

    tickers = get_tickers(worksheet)
    if not tickers:
        print("No tickers found.")
        return

    finnhub_api_key = os.getenv("FINNHUB_API_KEY")
    if not finnhub_api_key:
        raise RuntimeError("FINNHUB_API_KEY env var missing.")

    analyzer = SentimentIntensityAnalyzer()
    session = requests.Session()

    rows_to_write = []

    for idx, ticker in enumerate(tickers, start=2):
        print(f"Processing row {idx}: {ticker}")

        compound, count = compute_sentiment_for_ticker(
            session=session,
            analyzer=analyzer,
            ticker=ticker,
            api_key=finnhub_api_key,
        )

        timestamp = datetime.now(timezone.utc).isoformat()

        if compound is None:
            rows_to_write.append(["", 0, timestamp])
        else:
            rows_to_write.append([compound, count, timestamp])

        time.sleep(SECONDS_BETWEEN_REQUESTS)

    # Write P2:Rn
    start_row = 2
    end_row = start_row + len(rows_to_write) - 1

    start_col_letter = col_letter(16)  # P
    end_col_letter = col_letter(16 + len(HEADER_COLUMNS) - 1)  # R
    update_range = f"{start_col_letter}{start_row}:{end_col_letter}{end_row}"

    worksheet.update(update_range, rows_to_write, value_input_option="USER_ENTERED")

    print("Sentiment update complete.")


if __name__ == "__main__":
    main()
