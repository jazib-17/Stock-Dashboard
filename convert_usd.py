import yfinance as yf
import pandas as pd


# ============================================================
# SETTINGS
# ============================================================

INPUT_FILE = "stock_snapshot.csv"
OUTPUT_FILE = "stock_snapshot_usd.csv"


# ============================================================
# CURRENCY -> YAHOO FX TICKER
# ============================================================

FX_TICKERS = {
    "CAD": "CADUSD=X",
    "GBP": "GBPUSD=X",
    "EUR": "EURUSD=X",
    "CHF": "CHFUSD=X",
    "DKK": "DKKUSD=X",
    "SEK": "SEKUSD=X",
    "NOK": "NOKUSD=X",
    "JPY": "JPYUSD=X",
    "HKD": "HKDUSD=X",
    "AUD": "AUDUSD=X",
    "KRW": "KRWUSD=X",
    "TWD": "TWDUSD=X",
    "INR": "INRUSD=X",
    "SGD": "SGDUSD=X",
    "BRL": "BRLUSD=X",
    "MXN": "MXNUSD=X",
    "ZAR": "ZARUSD=X",
    "NZD": "NZDUSD=X"
}


# ============================================================
# CURRENCY BY EXCHANGE
# ============================================================

EXCHANGE_CURRENCY = {

    # United States
    "NMS": "USD",
    "NYQ": "USD",
    "ASE": "USD",

    # Canada
    "TOR": "CAD",
    "VAN": "CAD",
    "CNQ": "CAD",
    "NEO": "CAD",

    # United Kingdom
    "LSE": "GBP",

    # Germany
    "GER": "EUR",
    "FRA": "EUR",

    # France
    "PAR": "EUR",

    # Netherlands
    "AMS": "EUR",

    # Switzerland
    "EBS": "CHF",

    # Spain
    "MAD": "EUR",

    # Italy
    "MIL": "EUR",

    # Denmark
    "CPH": "DKK",

    # Sweden
    "STO": "SEK",

    # Norway
    "OSL": "NOK",

    # Finland
    "HEL": "EUR",

    # Belgium
    "BRU": "EUR",

    # Japan
    "JPX": "JPY",

    # Hong Kong
    "HKG": "HKD",

    # Australia
    "ASX": "AUD",

    # South Korea
    "KOE": "KRW",
    "KSC": "KRW",

    # Taiwan
    "TAI": "TWD",
    "TWO": "TWD",

    # India
    "NSI": "INR",
    "BSE": "INR",

    # Singapore
    "SES": "SGD",

    # Brazil
    "SAO": "BRL",

    # Mexico
    "MEX": "MXN",

    # South Africa
    "JNB": "ZAR",

    # New Zealand
    "NZE": "NZD"
}


# ============================================================
# 1. READ EXISTING STOCK DATA
# ============================================================

print("\n========================================")
print("READING STOCK DATA")
print("========================================")

df = pd.read_csv(INPUT_FILE)

print(
    f"Loaded {len(df)} stocks"
)


# ============================================================
# 2. DETERMINE CURRENCY FROM EXCHANGE
# ============================================================

print("\n========================================")
print("DETERMINING CURRENCIES")
print("========================================")


if "Exchange" not in df.columns:

    raise ValueError(
        "stock_snapshot.csv does not contain "
        "an 'Exchange' column."
    )


df["Currency"] = (
    df["Exchange"]
    .astype(str)
    .str.upper()
    .map(EXCHANGE_CURRENCY)
)


# Check for unknown exchanges
unknown_exchanges = (
    df.loc[
        df["Currency"].isna(),
        "Exchange"
    ]
    .dropna()
    .unique()
)


if len(unknown_exchanges) > 0:

    print("\nWARNING: Unknown exchanges:")

    print(
        ", ".join(
            map(str, unknown_exchanges)
        )
    )


# ============================================================
# 3. FIND CURRENCIES
# ============================================================

currencies = (
    df["Currency"]
    .dropna()
    .astype(str)
    .unique()
    .tolist()
)


non_usd_currencies = [
    currency
    for currency in currencies
    if currency != "USD"
    and currency in FX_TICKERS
]


print("\nCurrencies found:")

print(
    ", ".join(currencies)
)


print("\nCurrencies requiring conversion:")

print(
    ", ".join(non_usd_currencies)
)


# ============================================================
# 4. FIND FX DATE RANGE
# ============================================================

date_columns = [
    column
    for column in df.columns
    if column.startswith("Date_")
]


# Include current price date
if "CurrentDate" in df.columns:

    date_columns.append("CurrentDate")


all_dates = []


for column in date_columns:

    dates = pd.to_datetime(
        df[column],
        errors="coerce"
    )

    all_dates.extend(
        dates.dropna().tolist()
    )


if all_dates:

    fx_start = min(all_dates)
    fx_end = max(all_dates)

else:

    fx_start = (
        pd.Timestamp.today()
        - pd.Timedelta(days=7)
    )

    fx_end = pd.Timestamp.today()


# Give ourselves some buffer for weekends/holidays
fx_start -= pd.Timedelta(days=7)
fx_end += pd.Timedelta(days=2)


# ============================================================
# 5. DOWNLOAD FX DATA
# ============================================================

print("\n========================================")
print("DOWNLOADING FX DATA")
print("========================================")


fx_tickers = [
    FX_TICKERS[currency]
    for currency in non_usd_currencies
]


if fx_tickers:

    fx_data = yf.download(
        fx_tickers,
        start=fx_start.strftime("%Y-%m-%d"),
        end=fx_end.strftime("%Y-%m-%d"),
        interval="1d",
        auto_adjust=True,
        group_by="ticker",
        threads=True,
        progress=True
    )

else:

    fx_data = None


# ============================================================
# 6. GET FX RATE FOR A DATE
# ============================================================

def get_fx_rate(currency, date):

    # USD -> USD
    if currency == "USD":

        return 1.0


    if currency not in FX_TICKERS:

        return None


    if pd.isna(date):

        return None


    try:

        fx_ticker = FX_TICKERS[currency]

        rates = fx_data[
            fx_ticker
        ]["Close"].dropna()


        rates.index = pd.to_datetime(
            rates.index
        ).tz_localize(None)


        date = pd.Timestamp(date)


        # Find the most recent FX trading day
        # on or before the stock price date
        available = rates.loc[
            rates.index <= date
        ]


        if available.empty:

            return None


        return float(
            available.iloc[-1]
        )


    except Exception as e:

        print(
            f"FX error: {currency}, {date}: {e}"
        )

        return None


# ============================================================
# 7. CONVERT PRICES TO USD
# ============================================================

print("\n========================================")
print("CONVERTING PRICES TO USD")
print("========================================")


periods = [
    "Current",
    "1M",
    "3M",
    "6M",
    "1Y",
    "2Y",
    "5Y"
]


for i, row in df.iterrows():

    currency = row["Currency"]


    if pd.isna(currency):

        continue


    currency = str(currency)


    for period in periods:

        # ----------------------------------------------------
        # COLUMN NAMES
        # ----------------------------------------------------

        if period == "Current":

            price_column = "CurrentPrice"
            date_column = "CurrentDate"
            usd_column = "CurrentPriceUSD"

        else:

            price_column = f"Price_{period}"
            date_column = f"Date_{period}"
            usd_column = f"Price_{period}_USD"


        # Skip if the original column doesn't exist
        if price_column not in df.columns:

            continue


        if date_column not in df.columns:

            continue


        price = row[price_column]
        date = row[date_column]


        if pd.isna(price) or pd.isna(date):

            df.at[i, usd_column] = None

            continue


        # ----------------------------------------------------
        # GET HISTORICAL FX RATE
        # ----------------------------------------------------

        fx_rate = get_fx_rate(
            currency,
            date
        )


        # ----------------------------------------------------
        # CONVERT
        # ----------------------------------------------------

        if fx_rate is None:

            df.at[i, usd_column] = None

        else:

            df.at[i, usd_column] = (
                float(price) *
                fx_rate
            )


# ============================================================
# 8. CALCULATE USD RETURNS
# ============================================================

print("\n========================================")
print("CALCULATING USD RETURNS")
print("========================================")


for period in [
    "1M",
    "3M",
    "6M",
    "1Y",
    "2Y",
    "5Y"
]:

    historical_column = (
        f"Price_{period}_USD"
    )


    return_column = (
        f"Return_{period}_USD"
    )


    df[return_column] = (
        (
            df["CurrentPriceUSD"] /
            df[historical_column]
        ) - 1
    ) * 100


# ============================================================
# 9. SAVE
# ============================================================

df.to_csv(
    OUTPUT_FILE,
    index=False
)


print("\n========================================")
print("DONE")
print("========================================")


print(
    f"Saved {len(df)} stocks to "
    f"{OUTPUT_FILE}"
)