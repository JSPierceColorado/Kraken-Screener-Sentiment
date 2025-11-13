# Kraken Screener: Finnhub News Sentiment Analyzer

This script reads cryptocurrency or asset tickers from a Google Sheet (column A), fetches recent company/news articles from **Finnhub**, analyzes them using **VADER sentiment analysis**, and writes the results back into the sheet.

It is built to run as a periodic job (cron, server, GitHub Action, etc.) and maintain an updated sentiment column set for your **Kraken-Screener** worksheet.

---

## What This Script Does

For each ticker in your Google Sheet, the script:

1. Normalizes the ticker (removing suffixes like `/USD` or `-USD`).
2. Fetches up to `MAX_ARTICLES_PER_TICKER` recent articles from **Finnhub**.
3. Computes VADER compound sentiment scores.
4. Writes results to columns **P–R**:

   * **P** – VADER_Compound (average compound score)
   * **Q** – Articles_Analyzed
   * **R** – Last_Updated_UTC (timestamp)

If no valid articles or no sentiment can be computed, the cells are filled with blanks/zeroes.

---

## Google Sheets Layout

The script expects:

* Spreadsheet: **`Active-Investing`**
* Worksheet/tab: **`Kraken-Screener`**
* **Column A**: ticker symbols (row 1 = header, row 2+ = tickers)
* Script writes into:

  * **Column P (`VADER_Compound`)**
  * **Column Q (`Articles_Analyzed`)**
  * **Column R (`Last_Updated_UTC`)**

Columns **A–O** remain untouched.

The script will automatically ensure that the header row for P–R is present.

---

## Environment Variables

The script is controlled with the following environment variables:

| Variable                  | Required | Description                                                        |
| ------------------------- | -------- | ------------------------------------------------------------------ |
| `GOOGLE_CREDS_JSON`       | Yes      | Your full Google service account JSON **as a single-line string**. |
| `FINNHUB_API_KEY`         | Yes      | API key for Finnhub (required for company-news endpoint).          |
| `MAX_ARTICLES_PER_TICKER` | No       | Max number of articles to analyze per ticker (default: 20).        |

---

## News Fetching Logic

The function `compute_sentiment_for_ticker()` performs the heavy lifting:

### Lookback Window

* The script fetches news from Finnhub for the past **30 days** (`NEWS_LOOKBACK_DAYS`).

### Rate Limiting

* Sleeps **1.2 seconds** between tickers to avoid excessive request frequency.
* Handles **429 rate-limit responses** using Finnhub's `X-RateLimit-Reset` header.
* Retries once if a 429 occurs.

### Sentiment Calculation

* Each article's `headline` + `summary` is passed to VADER.
* Compound scores are averaged.
* Returns `(avg_compound, article_count)`.

---

## Installation

1. **Install dependencies**:

```bash
pip install gspread google-auth requests vaderSentiment
```

2. **Prepare your Google Service Account**:

   * Enable Google Sheets API
   * Enable Google Drive API
   * Share the target sheet with the service account email (Editor access)

3. **Set environment variables**:

```bash
export GOOGLE_CREDS_JSON='{"type":"service_account", ... }'
export FINNHUB_API_KEY="your_finnhub_key"
export MAX_ARTICLES_PER_TICKER=20
```

---

## Running the Script

Execute directly:

```bash
python kraken_sentiment.py
```

Logs will show:

```
Processing row 2: BTC
Ticker BTC: 12 articles analyzed (max=20), avg=0.214
Processing row 3: ETH
...
Sentiment update complete.
```

---

## How It Writes to the Sheet

The script builds a block like:

```
P2:R{n}
```

…where `n` is the last ticker row.

Rows are written in batch, improving performance and avoiding update quotas.

---

## Customization Ideas

* Add sentiment categorization (e.g., Positive / Neutral / Negative).
* Store raw article headlines in an adjacent tab.
* Expand to Kraken OHLC data or other APIs.
* Add Google Cloud Scheduler or GitHub Actions automation.

---

## License

Add your preferred license here (MIT, Apache 2.0, etc.).
