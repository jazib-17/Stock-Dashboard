"""
build_stock_dataset.py

Builds a wide stock dataset for a dashboard, using yfinance.

Pipeline:
  1. Screen global + US exchanges for candidate companies (yf.screen).
  2. Convert each candidate's market cap to USD immediately (Yahoo
     reports market cap in the LISTING's local currency, not USD --
     comparing raw values across currencies would badly distort both
     ranking and de-duplication).
  3. Remove duplicate listings of the same company across exchanges,
     preferring the exchange where the company actually resides.
  4. Take the top N companies by USD market cap (N is configurable).
  5. Download 5 years of price history in one batched call and derive
     returns + technical/risk indicators from it.
  6. Pull fundamentals (valuation, growth, quality, dividends, analyst
     targets) per ticker via yf.Ticker().info, threaded for speed.
  7. Optionally convert price/fundamental columns to USD using
     historical FX rates too (toggle with CONVERT_TO_USD below) --
     this is separate from step 2, which only fixes ranking/dedup.

Run this daily; it always re-screens and re-downloads fresh data.
"""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import yfinance as yf
from dateutil.relativedelta import relativedelta
from yfinance import EquityQuery


# ============================================================
# SETTINGS
# ============================================================

NUMBER_OF_STOCKS = 2000          # How many companies to keep, top-N by USD market cap
MIN_MARKET_CAP = 100_000_000    # $100 million (applied AFTER USD conversion)
MIN_PRICE = 5                   # Exclude penny stocks

CONVERT_TO_USD = True           # Add *_USD columns for price/fundamentals using historical FX
INCLUDE_FUNDAMENTALS = True     # Set False to skip the slower per-ticker .info calls

FUNDAMENTALS_WORKERS = 3        # Threads for per-ticker fundamentals lookups
COUNTRY_CHECK_WORKERS = 10      # Threads for resolving "home" exchange on multi-listed names

OUTPUT_UNIVERSE_FILE = "stock_universe.csv"
OUTPUT_FILE = "stock_snapshot.csv"

# Candidate pool sizes scale with how many stocks we ultimately want,
# so bumping NUMBER_OF_STOCKS doesn't starve the screener. Kept
# generous since Yahoo's own MIN_MARKET_CAP filter is applied in each
# listing's native currency (can't fix that server-side), so we need
# extra headroom to still end up with NUMBER_OF_STOCKS after USD
# re-filtering below.
GLOBAL_CANDIDATES = max(3000,4000)
US_CANDIDATES = max(2000, NUMBER_OF_STOCKS * 1)


# ============================================================
# EXCHANGES
# ============================================================

EXCHANGES = [

    # United States
    "NMS", "NYQ", "ASE",

    # Canada
    "TOR", "VAN", "CNQ", "NEO",

    # United Kingdom
    "LSE",

    # Germany
    "GER", "FRA",

    # France
    "PAR",

    # Netherlands
    "AMS",

    # Switzerland
    "EBS",

    # Spain
    "MAD",

    # Italy
    "MIL",

    # Denmark
    "CPH",

    # Sweden
    "STO",

    # Norway
    "OSL",

    # Finland
    "HEL",

    # Belgium
    "BRU",

    # Japan
    "JPX",

    # Hong Kong
    "HKG",

    # Australia
    "ASX",

    # South Korea
    "KOE", "KSC",

    # Taiwan
    "TAI", "TWO",

    # India
    "NSI", "BSE",

    # Singapore
    "SES",

    # Brazil
    "SAO",

    # Mexico
    "MEX",

    # South Africa
    "JNB",

    # New Zealand
    "NZE",
]

US_EXCHANGES = ["NMS", "NYQ", "ASE"]


# ============================================================
# EXCHANGE -> COUNTRY / CURRENCY
#
# We derive Country and Currency from the exchange code ourselves
# instead of trusting Yahoo's screener "region" field, which is
# unreliable. This is also how we know which currency each listing's
# market cap/price is quoted in, for USD conversion.
# ============================================================

EXCHANGE_COUNTRY = {

    "NMS": "United States", "NYQ": "United States", "ASE": "United States",

    "TOR": "Canada", "VAN": "Canada", "CNQ": "Canada", "NEO": "Canada",

    "LSE": "United Kingdom",

    "GER": "Germany", "FRA": "Germany",

    "PAR": "France",

    "AMS": "Netherlands",

    "EBS": "Switzerland",

    "MAD": "Spain",

    "MIL": "Italy",

    "CPH": "Denmark",

    "STO": "Sweden",

    "OSL": "Norway",

    "HEL": "Finland",

    "BRU": "Belgium",

    "JPX": "Japan",

    "HKG": "Hong Kong",

    "ASX": "Australia",

    "KOE": "South Korea", "KSC": "South Korea",

    "TAI": "Taiwan", "TWO": "Taiwan",

    "NSI": "India", "BSE": "India",

    "SES": "Singapore",

    "SAO": "Brazil",

    "MEX": "Mexico",

    "JNB": "South Africa",

    "NZE": "New Zealand",
}

EXCHANGE_CURRENCY = {

    "NMS": "USD", "NYQ": "USD", "ASE": "USD",

    "TOR": "CAD", "VAN": "CAD", "CNQ": "CAD", "NEO": "CAD",

    "LSE": "GBP",

    "GER": "EUR", "FRA": "EUR",

    "PAR": "EUR",

    "AMS": "EUR",

    "EBS": "CHF",

    "MAD": "EUR",

    "MIL": "EUR",

    "CPH": "DKK",

    "STO": "SEK",

    "OSL": "NOK",

    "HEL": "EUR",

    "BRU": "EUR",

    "JPX": "JPY",

    "HKG": "HKD",

    "ASX": "AUD",

    "KOE": "KRW", "KSC": "KRW",

    "TAI": "TWD", "TWO": "TWD",

    "NSI": "INR", "BSE": "INR",

    "SES": "SGD",

    "SAO": "BRL",

    "MEX": "MXN",

    "JNB": "ZAR",

    "NZE": "NZD",
}

# Currency -> Yahoo FX ticker
FX_TICKERS = {
    "CAD": "CADUSD=X", "GBP": "GBPUSD=X", "EUR": "EURUSD=X", "CHF": "CHFUSD=X",
    "DKK": "DKKUSD=X", "SEK": "SEKUSD=X", "NOK": "NOKUSD=X", "JPY": "JPYUSD=X",
    "HKD": "HKDUSD=X", "AUD": "AUDUSD=X", "KRW": "KRWUSD=X", "TWD": "TWDUSD=X",
    "INR": "INRUSD=X", "SGD": "SGDUSD=X", "BRL": "BRLUSD=X", "MXN": "MXNUSD=X",
    "ZAR": "ZARUSD=X", "NZD": "NZDUSD=X",
}


# ============================================================
# SMALL HELPERS
# ============================================================

def normalize_company_name(name):
    """Collapse a company name down to something comparable across
    exchanges, e.g. 'Shopify Inc' and 'Shopify Inc.' -> 'SHOPIFY'."""

    if pd.isna(name):
        return ""

    name = str(name).upper()

    name = re.sub(r"\bCLASS\s+[A-Z]\b", "", name)
    name = re.sub(r"\bCL\s*[A-Z]\b", "", name)

    suffixes = [
        " INCORPORATED", " INC", " CORPORATION", " CORP", " COMPANY", " CO",
        " LIMITED", " LTD", " PLC", " LLC", " HOLDINGS", " HOLDING",
        " SA", " NV", " SE", " AG",
    ]

    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[: -len(suffix)]

    name = re.sub(r"[^A-Z0-9\s]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()

    return name


def unix_to_date(ts):
    if ts is None or pd.isna(ts):
        return None
    try:
        return datetime.utcfromtimestamp(int(ts)).date()
    except (ValueError, OSError, TypeError):
        return None


def safe_div(a, b):
    if a is None or b is None or pd.isna(a) or pd.isna(b) or b == 0:
        return None
    return a / b


def get_screen_results(query, number):
    results = []

    for offset in range(0, number, 250):

        print(f"    Requesting results {offset + 1}-{offset + 250}...")

        try:
            result = yf.screen(
                query, offset=offset, size=250,
                sortField="intradaymarketcap", sortAsc=False,
            )
            quotes = result.get("quotes", [])
            results.extend(quotes)

            if len(quotes) < 250:
                break

        except Exception as e:
            print(f"    Error at offset {offset}: {e}")
            break

    return results


def fetch_company_country(ticker):
    """Real headquarters country from yfinance, used only to break
    ties between multiple listings of the same company."""

    try:
        info = yf.Ticker(ticker).info
        return ticker, info.get("country")
    except Exception:
        return ticker, None


def get_spot_fx_rates(currencies):
    """Latest available FX rate to USD for each currency, in one
    batched call. Used to make market cap comparable across markets
    BEFORE any ranking or dedup decisions are made."""

    rates = {"USD": 1.0}

    currencies = [c for c in currencies if c != "USD" and c in FX_TICKERS]
    if not currencies:
        return rates

    fx_tickers = [FX_TICKERS[c] for c in currencies]

    print("\n========================================")
    print("FETCHING SPOT FX RATES")
    print("========================================")

    fx_data = yf.download(
        fx_tickers,
        period="5d",
        interval="1d",
        auto_adjust=True,
        group_by="ticker",
        threads=True,
        progress=False,
    )

    for currency in currencies:
        try:
            fx_ticker = FX_TICKERS[currency]
            closes = fx_data[fx_ticker]["Close"].dropna()
            rates[currency] = float(closes.iloc[-1]) if not closes.empty else None
        except Exception as e:
            print(f"    Could not get spot rate for {currency}: {e}")
            rates[currency] = None

    return rates


# ============================================================
# 1. SCREEN FOR CANDIDATES (GLOBAL + US)
# ============================================================

print("\n========================================")
print("FINDING GLOBAL STOCKS")
print("========================================")

global_query = EquityQuery(
    "and",
    [
        EquityQuery("is-in", ["exchange"] + EXCHANGES),
        EquityQuery("gte", ["intradaymarketcap", MIN_MARKET_CAP]),
        EquityQuery("gte", ["intradayprice", MIN_PRICE]),
    ],
)

global_stocks = get_screen_results(global_query, GLOBAL_CANDIDATES)
print(f"\nGlobal candidates found: {len(global_stocks)}")

print("\n========================================")
print("FINDING US-LISTED STOCKS")
print("========================================")

us_query = EquityQuery(
    "and",
    [
        EquityQuery("is-in", ["exchange", "NMS", "NYQ", "ASE"]),
        EquityQuery("gte", ["intradaymarketcap", MIN_MARKET_CAP]),
        EquityQuery("gte", ["intradayprice", MIN_PRICE]),
    ],
)

us_stocks = get_screen_results(us_query, US_CANDIDATES)
print(f"\nUS candidates found: {len(us_stocks)}")


# ============================================================
# 2. BUILD COMPANY UNIVERSE
# ============================================================

print("\n========================================")
print("BUILDING COMPANY UNIVERSE")
print("========================================")

companies = pd.DataFrame(global_stocks + us_stocks)
companies = companies.drop_duplicates(subset="symbol", keep="first")

print(f"Unique tickers before company deduplication: {len(companies)}")

companies["exchange"] = companies["exchange"].astype(str).str.upper()

companies["CompanyIdentifier"] = companies["longName"].fillna(companies["shortName"])
companies["CompanyKey"] = companies["CompanyIdentifier"].apply(normalize_company_name)
companies = companies[companies["CompanyKey"] != ""]


# ============================================================
# 3. CONVERT MARKET CAP TO USD BEFORE RANKING OR DEDUPING
#
# Yahoo reports market cap in each listing's own currency. Comparing
# those raw numbers directly (e.g. a JPY market cap vs a USD one)
# would badly distort both "who is bigger" (dedup tiebreak) and
# "who's in the top N" (final selection) -- a mid-cap company on a
# high-nominal-value currency can dwarf a real mega-cap in raw terms.
# ============================================================

companies["Currency"] = companies["exchange"].map(EXCHANGE_CURRENCY)

unmapped_exchanges = companies.loc[companies["Currency"].isna(), "exchange"].unique()
if len(unmapped_exchanges) > 0:
    print(f"\nWARNING: Unmapped exchanges (add to EXCHANGE_CURRENCY): {list(unmapped_exchanges)}")

spot_rates = get_spot_fx_rates(companies["Currency"].dropna().unique().tolist())

companies["MarketCapUSD"] = companies.apply(
    lambda r: (r["marketCap"] * spot_rates.get(r["Currency"]))
    if pd.notna(r["marketCap"]) and spot_rates.get(r["Currency"]) is not None
    else None,
    axis=1,
)

# Re-apply the market cap floor in USD terms, now that it's comparable.
before_usd_filter = len(companies)
companies = companies[companies["MarketCapUSD"].notna() & (companies["MarketCapUSD"] >= MIN_MARKET_CAP)]
print(f"\nDropped {before_usd_filter - len(companies)} candidates below ${MIN_MARKET_CAP:,} once converted to USD")


# ============================================================
# 4. RESOLVE THE "HOME" LISTING FOR MULTI-LISTED COMPANIES
#
# Only companies with more than one listing need this check. For
# each such group, we trust the HQ country reported by the US-listed
# ticker's info (Yahoo tends to populate that reliably, since it's
# usually the more liquid/primary quote in its system) rather than
# the info of a secondary foreign listing, which is frequently
# sparse or wrong. If we can't get a confident answer, we default to
# keeping the US listing -- never the reverse -- so an API hiccup or
# bad data on a secondary listing can't bump a real US company (like
# Amazon) in favor of a thin cross-listing (like AMZN.SW).
# ============================================================

print("\n========================================")
print("RESOLVING HOME EXCHANGE FOR MULTI-LISTED COMPANIES")
print("========================================")

key_counts = companies["CompanyKey"].value_counts()
multi_listed_keys = set(key_counts[key_counts > 1].index)

print(f"Companies with more than one listing: {len(multi_listed_keys)}")

# Only need a country check for groups that actually contain a US
# listing (that's the only case where "prefer US by default" could
# be wrong). Pick one representative US ticker per group -- the
# largest by USD market cap.
groups_needing_check = {}
for key in multi_listed_keys:
    group = companies[companies["CompanyKey"] == key]
    us_rows = group[group["exchange"].isin(US_EXCHANGES)]
    if not us_rows.empty:
        rep_ticker = us_rows.sort_values("MarketCapUSD", ascending=False).iloc[0]["symbol"]
        groups_needing_check[key] = rep_ticker

print(f"Groups needing a country check: {len(groups_needing_check)}")

hq_country_by_key = {}

if groups_needing_check:
    tickers_to_check = list(groups_needing_check.values())
    with ThreadPoolExecutor(max_workers=COUNTRY_CHECK_WORKERS) as executor:
        futures = [executor.submit(fetch_company_country, t) for t in tickers_to_check]
        country_by_ticker = {}
        for i, future in enumerate(as_completed(futures), 1):
            ticker, country = future.result()
            country_by_ticker[ticker] = country
            if i % 50 == 0 or i == len(futures):
                print(f"    Checked {i}/{len(futures)}")

    hq_country_by_key = {
        key: country_by_ticker.get(rep_ticker)
        for key, rep_ticker in groups_needing_check.items()
    }


def pick_home_listing(group):
    """Given all listings for one company, return the row to keep."""

    key = group.name
    us_rows = group[group["exchange"].isin(US_EXCHANGES)]
    non_us_rows = group[~group["exchange"].isin(US_EXCHANGES)]

    if us_rows.empty:
        # No US listing at all -- just take the biggest by USD market cap.
        return group.sort_values("MarketCapUSD", ascending=False).iloc[0]

    hq_country = hq_country_by_key.get(key)

    # Unknown HQ country (lookup failed, or this wasn't a checked
    # group) -- safest default is to keep the US listing.
    if not hq_country or hq_country == "United States":
        return us_rows.sort_values("MarketCapUSD", ascending=False).iloc[0]

    # HQ is confidently somewhere else -- prefer a listing whose
    # exchange is actually in that country, if one exists.
    home_matches = non_us_rows[non_us_rows["exchange"].map(EXCHANGE_COUNTRY) == hq_country]
    if not home_matches.empty:
        return home_matches.sort_values("MarketCapUSD", ascending=False).iloc[0]

    # HQ is foreign but we don't have a listing on its home exchange
    # in our candidate set -- fall back to the biggest listing overall.
    return group.sort_values("MarketCapUSD", ascending=False).iloc[0]


before = len(companies)
companies = (
    companies.groupby("CompanyKey", group_keys=False)
    .apply(pick_home_listing)
    .reset_index(drop=True)
)
after = len(companies)

print(f"\nDuplicate listings removed: {before - after}")
print(f"Unique companies remaining: {after}")


# ============================================================
# 5. TAKE TOP N BY USD MARKET CAP
# ============================================================

companies = companies.sort_values("MarketCapUSD", ascending=False)
companies = companies.head(NUMBER_OF_STOCKS).reset_index(drop=True)

print(f"\nFinal stock universe: {len(companies)} unique companies")


# ============================================================
# 6. RENAME + FINALIZE IDENTIFICATION COLUMNS
# ============================================================

companies = companies.rename(
    columns={
        "symbol": "Ticker",
        "shortName": "Company",
        "exchange": "Exchange",
        "marketCap": "MarketCap",
    }
)

# Country comes from OUR exchange map, not Yahoo's "region" -- this
# is what fixes country showing USA for everything, and now that
# dedup prefers the real home exchange, it's usually accurate too.
companies["Country"] = companies["Exchange"].map(EXCHANGE_COUNTRY)

print("\n========================================")
print("SELECTED STOCKS (sample)")
print("========================================")
print(
    companies[["Ticker", "Company", "Exchange", "Country", "MarketCap", "MarketCapUSD"]]
    .head(20)
    .to_string(index=False)
)

companies.to_csv(OUTPUT_UNIVERSE_FILE, index=False)
print(f"\nSaved stock universe to {OUTPUT_UNIVERSE_FILE}")


# ============================================================
# 7. DOWNLOAD PRICE HISTORY (ONE BATCHED CALL)
# ============================================================

tickers = companies["Ticker"].tolist()
today = datetime.today()

# 5 years back plus a week of buffer for weekends/holidays around
# the exact anniversary dates we'll look up below.
start_date = today - relativedelta(years=5, days=7)

print("\n========================================")
print("DOWNLOADING PRICE HISTORY")
print("========================================")
print(f"Stocks: {len(tickers)}")
print(f"From:   {start_date.date()}")
print(f"To:     {today.date()}")

price_data = yf.download(
    tickers,
    start=start_date.strftime("%Y-%m-%d"),
    end=(today + relativedelta(days=1)).strftime("%Y-%m-%d"),
    interval="1d",
    auto_adjust=True,
    group_by="ticker",
    threads=True,
    progress=True,
)


# ============================================================
# 8. TECHNICAL / RISK / ACTIVITY INDICATORS FROM PRICE HISTORY
# ============================================================

PERIODS = [
    ("1W", relativedelta(weeks=1)),
    ("1M", relativedelta(months=1)),
    ("3M", relativedelta(months=3)),
    ("6M", relativedelta(months=6)),
    ("1Y", relativedelta(years=1)),
    ("3Y", relativedelta(years=3)),
    ("5Y", relativedelta(years=5)),
]


def compute_period_returns(stock_data, latest_date, current_price):
    row = {}

    for period, offset in PERIODS:
        target_date = latest_date - offset
        available = stock_data.loc[stock_data.index <= target_date]

        if available.empty:
            row[f"Date_{period}"] = None
            row[f"Price_{period}"] = None
            row[f"Return_{period}"] = None
            continue

        historical_date = available.index[-1]
        historical_price = float(available.iloc[-1]["Close"])

        row[f"Date_{period}"] = historical_date.date()
        row[f"Price_{period}"] = historical_price
        row[f"Return_{period}"] = ((current_price / historical_price) - 1) * 100

    # YTD (from the last trading day on/before Jan 1 of the current year)
    jan_1 = pd.Timestamp(year=latest_date.year, month=1, day=1)
    ytd_available = stock_data.loc[stock_data.index <= jan_1]

    if ytd_available.empty:
        row["Date_YTD"] = None
        row["Price_YTD"] = None
        row["Return_YTD"] = None
    else:
        ytd_date = ytd_available.index[-1]
        ytd_price = float(ytd_available.iloc[-1]["Close"])
        row["Date_YTD"] = ytd_date.date()
        row["Price_YTD"] = ytd_price
        row["Return_YTD"] = ((current_price / ytd_price) - 1) * 100

    return row


def compute_technicals(stock_data):
    close = stock_data["Close"]
    high = stock_data["High"] if "High" in stock_data else close
    low = stock_data["Low"] if "Low" in stock_data else close
    volume = stock_data["Volume"] if "Volume" in stock_data else pd.Series(dtype=float)

    current_price = float(close.iloc[-1])
    row = {}

    sma20 = close.rolling(20).mean().iloc[-1] if len(close) >= 20 else None
    sma50 = close.rolling(50).mean().iloc[-1] if len(close) >= 50 else None
    sma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else None
    ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1] if len(close) >= 20 else None

    row["SMA20"] = float(sma20) if pd.notna(sma20) else None
    row["SMA50"] = float(sma50) if pd.notna(sma50) else None
    row["SMA200"] = float(sma200) if pd.notna(sma200) else None
    row["EMA20"] = float(ema20) if pd.notna(ema20) else None

    row["PriceVsSMA50"] = ((current_price / row["SMA50"]) - 1) * 100 if row["SMA50"] else None
    row["PriceVsSMA200"] = ((current_price / row["SMA200"]) - 1) * 100 if row["SMA200"] else None

    # RSI14
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    row["RSI14"] = float(rsi.iloc[-1]) if len(close) >= 15 and pd.notna(rsi.iloc[-1]) else None

    # 52-week high/low
    window = min(len(close), 252)
    high_52w = float(high.tail(window).max())
    low_52w = float(low.tail(window).min())
    row["FiftyTwoWeekHigh"] = high_52w
    row["FiftyTwoWeekLow"] = low_52w
    row["FiftyTwoWRangePosition"] = (
        ((current_price - low_52w) / (high_52w - low_52w)) * 100 if high_52w != low_52w else None
    )
    row["DistanceFrom52WHigh"] = ((current_price / high_52w) - 1) * 100 if high_52w else None

    # Volatility (annualized, %)
    daily_returns = close.pct_change().dropna()
    row["Volatility30D"] = (
        float(daily_returns.tail(30).std() * (252 ** 0.5) * 100) if len(daily_returns) >= 30 else None
    )
    row["Volatility90D"] = (
        float(daily_returns.tail(90).std() * (252 ** 0.5) * 100) if len(daily_returns) >= 90 else None
    )

    # Max drawdown, trailing 1Y
    trailing = close.tail(min(len(close), 252))
    running_max = trailing.cummax()
    drawdown = (trailing / running_max) - 1
    row["MaxDrawdown1Y"] = float(drawdown.min() * 100) if not drawdown.empty else None

    # ATR14
    prev_close = close.shift(1)
    true_range = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    atr14 = true_range.rolling(14).mean().iloc[-1]
    row["ATR14"] = float(atr14) if pd.notna(atr14) else None

    # Activity
    if not volume.empty:
        row["Volume"] = float(volume.iloc[-1])
        avg_vol_20 = volume.rolling(20).mean().iloc[-1] if len(volume) >= 20 else None
        row["AvgVolume20D"] = float(avg_vol_20) if pd.notna(avg_vol_20) else None
        row["VolumeRatio"] = safe_div(row["Volume"], row["AvgVolume20D"])
    else:
        row["Volume"] = None
        row["AvgVolume20D"] = None
        row["VolumeRatio"] = None

    return row


print("\n========================================")
print("PROCESSING PRICE HISTORY")
print("========================================")

price_rows = []
failed = []

for i, ticker in enumerate(tickers, 1):

    print(f"[{i}/{len(tickers)}] {ticker}")

    try:
        stock_data = price_data[ticker].copy()
        stock_data = stock_data.dropna(subset=["Close"])

        if stock_data.empty:
            print("    No price data found")
            failed.append(ticker)
            continue

        stock_data.index = pd.to_datetime(stock_data.index).tz_localize(None)
        stock_data = stock_data.sort_index()

        latest_date = stock_data.index[-1]
        current_price = float(stock_data.loc[latest_date, "Close"])
        previous_close = (
            float(stock_data.iloc[-2]["Close"]) if len(stock_data) >= 2 else None
        )

        row = {
            "Ticker": ticker,
            "CurrentDate": latest_date.date(),
            "CurrentPrice": current_price,
            "PreviousClose": previous_close,
            "DailyReturn": (
                ((current_price / previous_close) - 1) * 100 if previous_close else None
            ),
        }

        row.update(compute_period_returns(stock_data, latest_date, current_price))
        row.update(compute_technicals(stock_data))

        price_rows.append(row)

    except Exception as e:
        print(f"    ERROR: {e}")
        failed.append(ticker)

price_df = pd.DataFrame(price_rows)

print(f"\nPrice history OK: {len(price_df)}")
print(f"Price history failed: {len(failed)}")


# ============================================================
# 9. FUNDAMENTALS (VALUATION / GROWTH / QUALITY / DIVIDENDS / ANALYSTS)
# ============================================================

import time
import random

MAX_RETRIES = 5
BASE_DELAY = 5  # seconds

def fetch_fundamentals(ticker):
    for attempt in range(MAX_RETRIES):
        try:
            info = yf.Ticker(ticker).info

            if not info or len(info) <= 1:
                raise ValueError("Empty info response (likely rate limited)")

            # small polite delay so we don't immediately hammer the next ticker
            time.sleep(0.25 + random.uniform(0, 0.25))
            break

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                return {"Ticker": ticker, "_fundamentals_error": str(e)}
            wait = BASE_DELAY * (2 ** attempt) + random.uniform(0, 2)
            print(f"    {ticker}: retry {attempt + 1}/{MAX_RETRIES} after {wait:.1f}s ({e})")
            time.sleep(wait)

    def g(key):
        return info.get(key)
    # ... rest of your function unchanged

    def g(key):
        return info.get(key)

    market_cap = g("marketCap")
    free_cf = g("freeCashflow")
    target_mean = g("targetMeanPrice")
    current = g("currentPrice") or g("regularMarketPrice")

    return {
        "Ticker": ticker,

        "Sector": g("sector"),
        "Industry": g("industry"),

        # Valuation
        "EnterpriseValue": g("enterpriseValue"),
        "PE": g("trailingPE"),
        "ForwardPE": g("forwardPE"),
        "PEG": g("trailingPegRatio") or g("pegRatio"),
        "PriceToBook": g("priceToBook"),
        "PriceToSales": g("priceToSalesTrailing12Months"),
        "EVToRevenue": g("enterpriseToRevenue"),
        "EVToEBITDA": g("enterpriseToEbitda"),
        "FCFYield": safe_div(free_cf, market_cap) and safe_div(free_cf, market_cap) * 100,

        # Growth
        "RevenueGrowth": g("revenueGrowth"),
        "EarningsGrowth": g("earningsGrowth"),
        "QuarterlyEarningsGrowth": g("earningsQuarterlyGrowth"),

        # Quality
        "ProfitMargin": g("profitMargins"),
        "OperatingMargin": g("operatingMargins"),
        "GrossMargin": g("grossMargins"),
        "ROE": g("returnOnEquity"),
        "ROA": g("returnOnAssets"),
        "DebtToEquity": g("debtToEquity"),
        "CurrentRatio": g("currentRatio"),
        "FreeCashFlow": free_cf,
        "OperatingCashFlow": g("operatingCashflow"),

        # Risk
        "Beta": g("beta"),

        # Dividends
        "DividendYield": g("dividendYield"),
        "DividendRate": g("dividendRate"),
        "PayoutRatio": g("payoutRatio"),
        "ExDividendDate": unix_to_date(g("exDividendDate")),

        # Earnings / expectations
        "NextEarningsDate": unix_to_date(g("earningsTimestampStart") or g("earningsTimestamp")),
        "TargetMeanPrice": target_mean,
        "TargetHighPrice": g("targetHighPrice"),
        "TargetLowPrice": g("targetLowPrice"),
        "AnalystCount": g("numberOfAnalystOpinions"),
        "UpsideToTarget": ((target_mean / current - 1) * 100) if target_mean and current else None,
    }


fundamentals_df = pd.DataFrame()

if INCLUDE_FUNDAMENTALS:

    print("\n========================================")
    print("DOWNLOADING FUNDAMENTALS")
    print("========================================")

    fundamentals_rows = []

    with ThreadPoolExecutor(max_workers=FUNDAMENTALS_WORKERS) as executor:
        futures = {executor.submit(fetch_fundamentals, t): t for t in tickers}

        for i, future in enumerate(as_completed(futures), 1):
            fundamentals_rows.append(future.result())
            if i % 50 == 0 or i == len(futures):
                print(f"    Fetched {i}/{len(futures)}")

    fundamentals_df = pd.DataFrame(fundamentals_rows)


# ============================================================
# 10. COMBINE EVERYTHING
# ============================================================

print("\n========================================")
print("COMBINING DATA")
print("========================================")

df = companies[
    ["Ticker", "Company", "Exchange", "Country", "Currency", "MarketCap", "MarketCapUSD"]
].copy()
df = df.merge(price_df, on="Ticker", how="left")

if not fundamentals_df.empty:
    # MarketCap from fundamentals is a live-er number; keep it separately.
    fundamentals_df = fundamentals_df.rename(columns={"MarketCap": "MarketCapLive"})
    df = df.merge(fundamentals_df, on="Ticker", how="left")


# ============================================================
# 11. OPTIONAL: CONVERT PRICE / FUNDAMENTAL COLUMNS TO USD
#
# Step 3 above already fixed ranking/dedup by converting market cap
# to USD internally. This step is separate: it adds *_USD columns
# for every price and dollar-denominated fundamental field, using
# historical FX rates aligned to each period's actual date, so you
# have both the native-currency and USD view side by side.
# ============================================================

def convert_to_usd(df):
    print("\n========================================")
    print("CONVERTING TO USD")
    print("========================================")

    non_usd_currencies = [
        c for c in df["Currency"].dropna().unique() if c != "USD" and c in FX_TICKERS
    ]

    if not non_usd_currencies:
        print("Nothing to convert -- everything is already USD.")
        return df

    date_columns = [c for c in df.columns if c.startswith("Date_")] + ["CurrentDate"]
    all_dates = []
    for column in date_columns:
        if column in df.columns:
            all_dates.extend(pd.to_datetime(df[column], errors="coerce").dropna().tolist())

    if all_dates:
        fx_start = min(all_dates) - pd.Timedelta(days=7)
        fx_end = max(all_dates) + pd.Timedelta(days=2)
    else:
        fx_start = pd.Timestamp.today() - pd.Timedelta(days=14)
        fx_end = pd.Timestamp.today() + pd.Timedelta(days=2)

    fx_tickers = [FX_TICKERS[c] for c in non_usd_currencies]

    fx_data = yf.download(
        fx_tickers, start=fx_start.strftime("%Y-%m-%d"), end=fx_end.strftime("%Y-%m-%d"),
        interval="1d", auto_adjust=True, group_by="ticker", threads=True, progress=True,
    )

    def get_fx_rate(currency, on_date):
        if currency == "USD":
            return 1.0
        if currency not in FX_TICKERS or pd.isna(on_date):
            return None
        try:
            fx_ticker = FX_TICKERS[currency]
            rates = fx_data[fx_ticker]["Close"].dropna()
            rates.index = pd.to_datetime(rates.index).tz_localize(None)
            on_date = pd.Timestamp(on_date)
            available = rates.loc[rates.index <= on_date]
            return float(available.iloc[-1]) if not available.empty else None
        except Exception:
            return None

    # Spot rate (latest available) per currency, used for "as of today"
    # fundamentals like MarketCap/DividendRate/analyst targets.
    spot_rate = {c: get_fx_rate(c, pd.Timestamp.today()) for c in non_usd_currencies}
    spot_rate["USD"] = 1.0

    price_periods = ["Current", "1W", "1M", "3M", "6M", "YTD", "1Y", "3Y", "5Y"]
    spot_fields = [
        "MarketCap", "MarketCapLive", "EnterpriseValue", "FreeCashFlow",
        "OperatingCashFlow", "DividendRate", "TargetMeanPrice",
        "TargetHighPrice", "TargetLowPrice",
    ]

    for i, row in df.iterrows():
        currency = row.get("Currency")
        if pd.isna(currency):
            continue
        currency = str(currency)

        for period in price_periods:
            price_col = "CurrentPrice" if period == "Current" else f"Price_{period}"
            date_col = "CurrentDate" if period == "Current" else f"Date_{period}"
            usd_col = "CurrentPriceUSD" if period == "Current" else f"Price_{period}_USD"

            if price_col not in df.columns or date_col not in df.columns:
                continue

            price = row.get(price_col)
            on_date = row.get(date_col)

            if pd.isna(price) or pd.isna(on_date):
                df.at[i, usd_col] = None
                continue

            rate = get_fx_rate(currency, on_date)
            df.at[i, usd_col] = float(price) * rate if rate is not None else None

        rate_today = spot_rate.get(currency)
        for field in spot_fields:
            if field not in df.columns:
                continue
            value = row.get(field)
            usd_col = f"{field}_USD"
            if pd.isna(value) or rate_today is None:
                df.at[i, usd_col] = None
            else:
                df.at[i, usd_col] = float(value) * rate_today

    for period in ["1W", "1M", "3M", "6M", "YTD", "1Y", "3Y", "5Y"]:
        hist_col = f"Price_{period}_USD"
        ret_col = f"Return_{period}_USD"
        if hist_col in df.columns and "CurrentPriceUSD" in df.columns:
            df[ret_col] = ((df["CurrentPriceUSD"] / df[hist_col]) - 1) * 100

    return df


if CONVERT_TO_USD:
    df = convert_to_usd(df)


# ============================================================
# 12. COLUMN ORDER
# ============================================================

column_order = [

    # Identification
    "Ticker", "Company", "Sector", "Industry", "Country", "Exchange", "Currency",

    # Price
    "CurrentPrice", "CurrentPriceUSD", "CurrentDate",
    "PreviousClose", "DailyReturn",

    "Date_1W", "Price_1W", "Price_1W_USD", "Return_1W", "Return_1W_USD",
    "Date_1M", "Price_1M", "Price_1M_USD", "Return_1M", "Return_1M_USD",
    "Date_3M", "Price_3M", "Price_3M_USD", "Return_3M", "Return_3M_USD",
    "Date_6M", "Price_6M", "Price_6M_USD", "Return_6M", "Return_6M_USD",
    "Date_YTD", "Price_YTD", "Price_YTD_USD", "Return_YTD", "Return_YTD_USD",
    "Date_1Y", "Price_1Y", "Price_1Y_USD", "Return_1Y", "Return_1Y_USD",
    "Date_3Y", "Price_3Y", "Price_3Y_USD", "Return_3Y", "Return_3Y_USD",
    "Date_5Y", "Price_5Y", "Price_5Y_USD", "Return_5Y", "Return_5Y_USD",

    # Technical
    "SMA20", "SMA50", "SMA200", "EMA20", "RSI14",
    "PriceVsSMA50", "PriceVsSMA200",
    "FiftyTwoWeekHigh", "FiftyTwoWeekLow", "FiftyTwoWRangePosition", "DistanceFrom52WHigh",

    # Risk
    "Beta", "Volatility30D", "Volatility90D", "MaxDrawdown1Y", "ATR14",

    # Valuation
    "MarketCap", "MarketCapUSD", "MarketCapLive", "MarketCapLive_USD",
    "EnterpriseValue", "EnterpriseValue_USD",
    "PE", "ForwardPE", "PEG", "PriceToBook", "PriceToSales",
    "EVToRevenue", "EVToEBITDA", "FCFYield",

    # Growth
    "RevenueGrowth", "EarningsGrowth", "QuarterlyEarningsGrowth",

    # Quality
    "ProfitMargin", "OperatingMargin", "GrossMargin", "ROE", "ROA",
    "DebtToEquity", "CurrentRatio",
    "FreeCashFlow", "FreeCashFlow_USD", "OperatingCashFlow", "OperatingCashFlow_USD",

    # Dividend
    "DividendYield", "DividendRate", "DividendRate_USD", "PayoutRatio", "ExDividendDate",

    # Activity
    "Volume", "AvgVolume20D", "VolumeRatio",

    # Earnings / expectations
    "NextEarningsDate",
    "TargetMeanPrice", "TargetMeanPrice_USD",
    "TargetHighPrice", "TargetHighPrice_USD",
    "TargetLowPrice", "TargetLowPrice_USD",
    "AnalystCount", "UpsideToTarget",
]

column_order = [c for c in column_order if c in df.columns]
# Keep any extra columns (e.g. error columns) at the end instead of dropping them
remaining = [c for c in df.columns if c not in column_order]
df = df[column_order + remaining]


# ============================================================
# 13. SAVE
# ============================================================

print("\n========================================")
print("DONE")
print("========================================")
print(f"Companies in final dataset: {len(df)}")
if failed:
    print(f"Failed price lookups ({len(failed)}): {', '.join(failed[:25])}" + (" ..." if len(failed) > 25 else ""))

df.to_csv(OUTPUT_FILE, index=False)
print(f"\nSaved to {OUTPUT_FILE}")