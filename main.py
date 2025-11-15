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
# Note: "VADER_Compound" is now a combined VADER score from Finnhub + CryptoNews.
HEADER_COLUMNS = [
    "VADER_Compound",     # P
    "Articles_Analyzed",  # Q
    "Last_Updated_UTC",   # R
]

# How far back to look for news per ticker (in days) for Finnhub
NEWS_LOOKBACK_DAYS = int(os.getenv("NEWS_LOOKBACK_DAYS", "30"))

# Throttle between tickers so we don't spam APIs
SECONDS_BETWEEN_REQUESTS = float(os.getenv("SECONDS_BETWEEN_REQUESTS", "1.2"))

# Env-var-controlled max articles (default 20) for Finnhub
MAX_ARTICLES_PER_TICKER = int(os.getenv("MAX_ARTICLES_PER_TICKER", "20"))

# CryptoNews config
CRYPTONEWS_API_TOKEN_ENV = "CRYPTONEWS_API_TOKEN"
CRYPTONEWS_BASE_URL = os.getenv(
    "CRYPTONEWS_BASE_URL",
    "https://cryptonews-api.com/api/v1",
)
CRYPTONEWS_MAX_ITEMS_PER_TICKER = int(
    os.getenv("CRYPTONEWS_MAX_ITEMS_PER_TICKER", "20")
)


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
    Normalize Kraken-style tickers for news APIs.
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


# ------------------------ Finnhub sentiment ------------------------


def compute_finnhub_sentiment_for_ticker(
    session: requests.Session,
    analyzer: SentimentIntensityAnalyzer,
    ticker: str,
    api_key: str,
    days_back: int = NEWS_LOOKBACK_DAYS,
):
    """
    Fetch recent company/asset news from Finnhub and compute VADER sentiment.
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
                    f"[Finnhub] 429 rate limit for {ticker} — "
                    f"sleeping {wait_secs}s then retrying..."
                )
                time.sleep(wait_secs)
                continue

            resp.raise_for_status()
            articles = resp.json()
            break

        except Exception as e:
            print(f"[Finnhub] Error fetching news for {ticker}: {e}")
            return None, 0
    else:
        print(f"[Finnhub] Failed to fetch news for {ticker} after retries.")
        return None, 0

    # No articles
    if not isinstance(articles, list) or not articles:
        print(f"[Finnhub] No news returned for {ticker}.")
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
        print(f"[Finnhub] No usable text for sentiment for {ticker}.")
        return None, 0

    avg_compound = round(sum(compounds) / len(compounds), 4)

    print(
        f"[Finnhub] {ticker}: {len(compounds)} articles analyzed "
        f"(max={MAX_ARTICLES_PER_TICKER}), avg={avg_compound}"
    )

    return avg_compound, len(compounds)


# ------------------------ CryptoNews sentiment ------------------------


def compute_cryptonews_sentiment_for_ticker(
    session: requests.Session,
    analyzer: SentimentIntensityAnalyzer,
    ticker: str,
    api_token: str,
    max_items: int = CRYPTONEWS_MAX_ITEMS_PER_TICKER,
):
    """
    Fetch recent ticker news from CryptoNews and compute VADER sentiment.

    Endpoint pattern (per docs/homepage):
      GET https://cryptonews-api.com/api/v1
          ?tickers=BTC
          &items=10
          &token=YOUR_TOKEN

    Response structure (based on public examples):
      {
        "data": [
          {
            "title": "...",
            "text": "...",
            "content": "...",
            ...
          },
          ...
        ],
        ...
      }

    We:
      - Pull up to `max_items` articles via &items=
      - Build text from title + text/content
      - Compute VADER compound per article
      - Return (average_compound, count)
    """
    symbol_for_news = normalize_ticker_for_news(ticker)

    params = {
        "tickers": symbol_for_news,
        "items": max_items,
        "token": api_token,
    }

    try:
        resp = session.get(CRYPTONEWS_BASE_URL, params=params, timeout=10)
    except Exception as e:
        print(f"[CryptoNews] Error fetching news for {ticker}: {e}")
        return None, 0

    if resp.status_code != 200:
        # Common case you saw before was 403, often plan-related
        print(
            f"[CryptoNews] Request failed for {ticker} with status "
            f"{resp.status_code}: {resp.text[:200]}"
        )
        return None, 0

    try:
        payload = resp.json()
    except Exception as e:
        print(f"[CryptoNews] Failed to parse JSON for {ticker}: {e}")
        return None, 0

    articles = payload.get("data") or []
    if not isinstance(articles, list) or not articles:
        print(f"[CryptoNews] No news returned for {ticker}.")
        return None, 0

    compounds = []
    for article in articles:
        # Be defensive about field names; adjust if needed once you inspect the payload.
        title = (
            article.get("title")
            or article.get("news_title")
            or ""
        ).strip()

        body = (
            article.get("text")
            or article.get("content")
            or ""
        ).strip()

        text = (title + ". " + body).strip()
        if not text:
            continue

        score = analyzer.polarity_scores(text)["compound"]
        compounds.append(score)

    if not compounds:
        print(f"[CryptoNews] No usable text for sentiment for {ticker}.")
        return None, 0

    avg_compound = round(sum(compounds) / len(compounds), 4)

    print(
        f"[CryptoNews] {ticker}: {len(compounds)} articles analyzed "
        f"(max={max_items}), avg={avg_compound}"
    )

    return avg_compound, len(compounds)


# ------------------------ Combined sentiment ------------------------


def compute_combined_sentiment_for_ticker(
    session: requests.Session,
    analyzer: SentimentIntensityAnalyzer,
    ticker: str,
    finnhub_api_key: str,
    cryptonews_token: str,
):
    """
    Compute a combined sentiment score per ticker using:
      - Finnhub company-news
      - CryptoNews ticker news

    Combination logic:
      - Get (avg_finnhub, n_finnhub)
      - Get (avg_crypto, n_crypto)
      - If both available, weighted average by article count:
          combined = (avg_finnhub * n_finnhub + avg_crypto * n_crypto) / (n_finnhub + n_crypto)
      - If only one is available, use that.
      - If neither, return (None, 0).
    """
    # Finnhub
    finnhub_score, finnhub_count = compute_finnhub_sentiment_for_ticker(
        session=session,
        analyzer=analyzer,
        ticker=ticker,
        api_key=finnhub_api_key,
    )

    # CryptoNews
    cryptonews_score, cryptonews_count = compute_cryptonews_sentiment_for_ticker(
        session=session,
        analyzer=analyzer,
        ticker=ticker,
        api_token=cryptonews_token,
    )

    total_count = 0
    weighted_sum = 0.0

    if finnhub_score is not None and finnhub_count > 0:
        total_count += finnhub_count
        weighted_sum += finnhub_score * finnhub_count

    if cryptonews_score is not None and cryptonews_count > 0:
        total_count += cryptonews_count
        weighted_sum += cryptonews_score * cryptonews_count

    if total_count == 0:
        print(f"[Combined] No sentiment data for {ticker}.")
        return None, 0

    combined_score = round(weighted_sum / total_count, 4)

    print(
        f"[Combined] {ticker}: total_articles={total_count}, "
        f"combined_score={combined_score} "
        f"(Finnhub n={finnhub_count or 0}, CryptoNews n={cryptonews_count or 0})"
    )

    return combined_score, total_count


# ------------------------ main ------------------------


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

    cryptonews_token = os.getenv(CRYPTONEWS_API_TOKEN_ENV)
    if not cryptonews_token:
        raise RuntimeError(f"{CRYPTONEWS_API_TOKEN_ENV} env var missing.")

    analyzer = SentimentIntensityAnalyzer()
    session = requests.Session()

    rows_to_write = []

    for idx, ticker in enumerate(tickers, start=2):
        print(f"Processing row {idx}: {ticker}")

        compound, count = compute_combined_sentiment_for_ticker(
            session=session,
            analyzer=analyzer,
            ticker=ticker,
            finnhub_api_key=finnhub_api_key,
            cryptonews_token=cryptonews_token,
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
