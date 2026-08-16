import yfinance as yf
import pandas as pd
import re

from datetime import datetime
from dateutil.relativedelta import relativedelta
from yfinance import EquityQuery


# ============================================================
# SETTINGS
# ============================================================

NUMBER_OF_STOCKS = 1500

# Get more candidates than we need so duplicates
# don't reduce the final number of companies.
GLOBAL_CANDIDATES = 3000
US_CANDIDATES = 2000

MIN_MARKET_CAP = 100_000_000   # $100 million
MIN_PRICE = 5                  # Exclude penny stocks


# ============================================================
# EXCHANGES
# ============================================================

EXCHANGES = [

    # United States
    "NMS",      # NASDAQ
    "NYQ",      # NYSE
    "ASE",      # NYSE American

    # Canada
    "TOR",
    "VAN",
    "CNQ",
    "NEO",

    # United Kingdom
    "LSE",

    # Germany
    "GER",
    "FRA",

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
    "KOE",
    "KSC",

    # Taiwan
    "TAI",
    "TWO",

    # India
    "NSI",
    "BSE",

    # Singapore
    "SES",

    # Brazil
    "SAO",

    # Mexico
    "MEX",

    # South Africa
    "JNB",

    # New Zealand
    "NZE"
]


# ============================================================
# US EXCHANGES
# ============================================================

US_EXCHANGES = [
    "NMS",
    "NYQ",
    "ASE"
]


# ============================================================
# FUNCTION: GET SCREENER RESULTS
# ============================================================

def get_screen_results(query, number):

    results = []

    for offset in range(0, number, 250):

        print(
            f"    Requesting results "
            f"{offset + 1}-{offset + 250}..."
        )

        try:

            result = yf.screen(
                query,
                offset=offset,
                size=250,
                sortField="intradaymarketcap",
                sortAsc=False
            )

            quotes = result.get(
                "quotes",
                []
            )

            results.extend(quotes)

            # If Yahoo returned fewer than 250,
            # there are no more results.
            if len(quotes) < 250:
                break

        except Exception as e:

            print(
                f"    Error at offset {offset}: {e}"
            )

            break

    return results


# ============================================================
# FUNCTION: NORMALIZE COMPANY NAME
# ============================================================

def normalize_company_name(name):

    if pd.isna(name):
        return ""

    name = str(name).upper()

    # --------------------------------------------------------
    # Remove share classes
    # --------------------------------------------------------

    name = re.sub(
        r"\bCLASS\s+[A-Z]\b",
        "",
        name
    )

    name = re.sub(
        r"\bCL\s*[A-Z]\b",
        "",
        name
    )

    # --------------------------------------------------------
    # Remove common corporate suffixes
    # --------------------------------------------------------

    suffixes = [
        " INCORPORATED",
        " INC",
        " CORPORATION",
        " CORP",
        " COMPANY",
        " CO",
        " LIMITED",
        " LTD",
        " PLC",
        " LLC",
        " HOLDINGS",
        " HOLDING",
        " SA",
        " NV",
        " SE",
        " AG"
    ]

    for suffix in suffixes:

        if name.endswith(suffix):
            name = name[:-len(suffix)]

    # --------------------------------------------------------
    # Remove punctuation
    # --------------------------------------------------------

    name = re.sub(
        r"[^A-Z0-9\s]",
        " ",
        name
    )

    # --------------------------------------------------------
    # Normalize whitespace
    # --------------------------------------------------------

    name = re.sub(
        r"\s+",
        " ",
        name
    ).strip()

    return name


# ============================================================
# 1. GET GLOBAL CANDIDATES
# ============================================================

print("\n========================================")
print("FINDING GLOBAL STOCKS")
print("========================================")

global_query = EquityQuery(
    "and",
    [

        # Only selected exchanges
        EquityQuery(
            "is-in",
            ["exchange"] + EXCHANGES
        ),

        # Minimum market cap
        EquityQuery(
            "gte",
            [
                "intradaymarketcap",
                MIN_MARKET_CAP
            ]
        ),

        # Minimum share price
        EquityQuery(
            "gte",
            [
                "intradayprice",
                MIN_PRICE
            ]
        )
    ]
)


global_stocks = get_screen_results(
    global_query,
    GLOBAL_CANDIDATES
)


print(
    f"\nGlobal candidates found: "
    f"{len(global_stocks)}"
)


# ============================================================
# 2. GET US-LISTED CANDIDATES
# ============================================================

print("\n========================================")
print("FINDING US-LISTED STOCKS")
print("========================================")

us_query = EquityQuery(
    "and",
    [

        # Only major US exchanges
        EquityQuery(
            "is-in",
            [
                "exchange",
                "NMS",
                "NYQ",
                "ASE"
            ]
        ),

        # Minimum market cap
        EquityQuery(
            "gte",
            [
                "intradaymarketcap",
                MIN_MARKET_CAP
            ]
        ),

        # Minimum price
        EquityQuery(
            "gte",
            [
                "intradayprice",
                MIN_PRICE
            ]
        )
    ]
)


us_stocks = get_screen_results(
    us_query,
    US_CANDIDATES
)


print(
    f"\nUS candidates found: "
    f"{len(us_stocks)}"
)


# ============================================================
# 3. COMBINE GLOBAL + US RESULTS
# ============================================================

print("\n========================================")
print("BUILDING COMPANY UNIVERSE")
print("========================================")

companies = pd.DataFrame(
    global_stocks + us_stocks
)


# Remove exact duplicate tickers
companies = companies.drop_duplicates(
    subset="symbol",
    keep="first"
)


print(
    f"Unique tickers before company "
    f"deduplication: {len(companies)}"
)


# ============================================================
# 4. CREATE COMPANY IDENTIFIER
# ============================================================

# Prefer Yahoo's long company name.
# Fall back to shortName if longName isn't available.

companies["CompanyIdentifier"] = (
    companies["longName"]
    .fillna(
        companies["shortName"]
    )
)


companies["CompanyKey"] = (
    companies["CompanyIdentifier"]
    .apply(
        normalize_company_name
    )
)


# Remove anything without a company name
companies = companies[
    companies["CompanyKey"] != ""
]


# ============================================================
# 5. MARK US LISTINGS
# ============================================================

companies["IsUS"] = (
    companies["exchange"].isin(
        US_EXCHANGES
    )
)


# ============================================================
# 6. PRIORITIZE US LISTINGS
# ============================================================

# This is the important part.
#
# For the same company:
#
# TXN       -> US -> True
# TXN.MX    -> Mexico -> False
#
# Because IsUS is sorted descending,
# TXN will be selected.
#
# Same thing for:
#
# MSFT / MSFT.MX
# SHOP / SHOP.TO
# TM / 7203.T
# etc.

companies = companies.sort_values(
    by=[
        "CompanyKey",
        "IsUS",
        "marketCap"
    ],
    ascending=[
        True,
        False,
        False
    ]
)


# ============================================================
# 7. KEEP ONE LISTING PER COMPANY
# ============================================================

before = len(companies)


companies = companies.drop_duplicates(
    subset="CompanyKey",
    keep="first"
)


after = len(companies)


print(
    f"Duplicate listings removed: "
    f"{before - after}"
)

print(
    f"Unique companies remaining: "
    f"{after}"
)


# ============================================================
# 8. SORT UNIQUE COMPANIES BY MARKET CAP
# ============================================================

companies = companies.sort_values(
    "marketCap",
    ascending=False
)


# ============================================================
# 9. TAKE TOP 500
# ============================================================

companies = companies.head(
    NUMBER_OF_STOCKS
).reset_index(
    drop=True
)


print(
    f"\nFinal stock universe: "
    f"{len(companies)} unique companies"
)


# ============================================================
# 10. RENAME COLUMNS
# ============================================================

companies = companies.rename(
    columns={
        "symbol": "Ticker",
        "shortName": "Company",
        "exchange": "Exchange",
        "region": "Country",
        "marketCap": "MarketCap",
        "regularMarketPrice": "ScreenPrice"
    }
)


# ============================================================
# 11. SHOW SELECTED STOCKS
# ============================================================

print("\n========================================")
print("SELECTED STOCKS")
print("========================================")

print(
    companies[
        [
            "Ticker",
            "Company",
            "Exchange",
            "Country",
            "MarketCap"
        ]
    ].to_string(
        index=False
    )
)


# ============================================================
# 12. SAVE STOCK UNIVERSE
# ============================================================

companies.to_csv(
    "stock_universe.csv",
    index=False
)


print(
    "\nSaved stock universe to "
    "stock_universe.csv"
)


# ============================================================
# 13. PREPARE PRICE DOWNLOAD
# ============================================================

stocks = companies[
    "Ticker"
].tolist()


today = datetime.today()


# Download 7 extra days before the 5-year target.
#
# This gives us enough room to find the nearest trading
# day if the exact anniversary falls on a weekend/holiday.

start_date = today - relativedelta(
    years=5,
    days=7
)


print("\n========================================")
print("DOWNLOADING PRICE DATA")
print("========================================")

print(
    f"Stocks: {len(stocks)}"
)

print(
    f"From:   {start_date.date()}"
)

print(
    f"To:     {today.date()}"
)

print()


# ============================================================
# 14. DOWNLOAD ALL PRICE DATA
# ============================================================

data = yf.download(
    stocks,
    start=start_date.strftime(
        "%Y-%m-%d"
    ),
    end=(
        today +
        relativedelta(
            days=1
        )
    ).strftime(
        "%Y-%m-%d"
    ),
    interval="1d",
    auto_adjust=True,
    group_by="ticker",
    threads=True,
    progress=True
)


# ============================================================
# 15. PROCESS EACH STOCK
# ============================================================

results = []
failed = []


print("\n========================================")
print("PROCESSING STOCKS")
print("========================================")


for i, ticker in enumerate(
    stocks,
    1
):

    print(
        f"[{i}/{len(stocks)}] {ticker}"
    )


    try:

        # ----------------------------------------------------
        # Get this stock's data
        # ----------------------------------------------------

        stock_data = data[
            ticker
        ].copy()


        # Remove rows without closing prices

        stock_data = stock_data.dropna(
            subset=["Close"]
        )


        if stock_data.empty:

            print(
                "    No price data found"
            )

            failed.append(
                ticker
            )

            continue


        # ----------------------------------------------------
        # Remove timezone
        # ----------------------------------------------------

        stock_data.index = pd.to_datetime(
            stock_data.index
        ).tz_localize(
            None
        )


        # ----------------------------------------------------
        # CURRENT PRICE
        # ----------------------------------------------------

        latest_date = (
            stock_data.index[-1]
        )


        current_price = float(
            stock_data.loc[
                latest_date,
                "Close"
            ]
        )


        row = {

            "Ticker": ticker,

            "CurrentDate":
                latest_date.date(),

            "CurrentPrice":
                current_price
        }


        # ----------------------------------------------------
        # HISTORICAL PRICES
        # ----------------------------------------------------

        periods = [
            ("1M", relativedelta(months=1)),
            ("3M", relativedelta(months=3)),
            ("6M", relativedelta(months=6)),
            ("1Y", relativedelta(years=1)),
            ("2Y", relativedelta(years=2)),
            ("5Y", relativedelta(years=5))
        ]


        for period, offset in periods:

            # Exact calendar date
            target_date = latest_date - offset

            # Find latest trading day on or before
            # the target date
            available = stock_data.loc[
                stock_data.index <= target_date
            ]

            if available.empty:

                row[f"Date_{period}"] = None
                row[f"Price_{period}"] = None
                row[f"Return_{period}"] = None

                continue

            historical_date = available.index[-1]

            historical_price = float(
                available.iloc[-1]["Close"]
            )

            return_pct = (
                (
                    current_price /
                    historical_price
                ) - 1
            ) * 100

            row[f"Date_{period}"] = (
                historical_date.date()
            )

            row[f"Price_{period}"] = (
                historical_price
            )

            row[f"Return_{period}"] = (
                return_pct
            )


        # ----------------------------------------------------
        # COMPANY INFORMATION
        # ----------------------------------------------------

        company_info = companies[
            companies["Ticker"] ==
            ticker
        ]


        if not company_info.empty:

            company_info = (
                company_info.iloc[0]
            )


            row["Company"] = (
                company_info.get(
                    "Company"
                )
            )


            row["Exchange"] = (
                company_info.get(
                    "Exchange"
                )
            )


            row["Country"] = (
                company_info.get(
                    "Country"
                )
            )

            row["MarketCap"] = (
                company_info.get(
                    "MarketCap"
                )
            )


        results.append(
            row
        )


    except Exception as e:

        print(
            f"    ERROR: {e}"
        )

        failed.append(
            ticker
        )


# ============================================================
# 16. CREATE FINAL DATAFRAME
# ============================================================

df = pd.DataFrame(
    results
)


# ============================================================
# 17. COLUMN ORDER
# ============================================================

column_order = [

    "Ticker",
    "Company",
    "Country",
    "Exchange",
    "MarketCap",

    "CurrentDate",
    "CurrentPrice",

    "Date_1M",
    "Price_1M",
    "Return_1M",

    "Date_3M",
    "Price_3M",
    "Return_3M",

    "Date_6M",
    "Price_6M",
    "Return_6M",

    "Date_1Y",
    "Price_1Y",
    "Return_1Y",

    "Date_2Y",
    "Price_2Y",
    "Return_2Y",

    "Date_5Y",
    "Price_5Y",
    "Return_5Y"
]


# Only keep columns that exist

column_order = [
    column
    for column in column_order
    if column in df.columns
]


df = df[
    column_order
]


# ============================================================
# 18. RESULTS
# ============================================================

print("\n========================================")
print("DONE")
print("========================================")


print(
    f"Successful: {len(df)}"
)


print(
    f"Failed:     {len(failed)}"
)


if failed:

    print(
        "\nFailed tickers:"
    )

    for ticker in failed:

        print(
            f"  {ticker}"
        )


# ============================================================
# 19. DISPLAY SAMPLE
# ============================================================

print(
    "\nFinal dataset:"
)

print(
    df.head(20).to_string(
        index=False
    )
)


# ============================================================
# 20. SAVE FINAL DATASET
# ============================================================

df.to_csv(
    "stock_snapshot.csv",
    index=False
)


print(
    "\nSaved to "
    "stock_snapshot.csv"
)