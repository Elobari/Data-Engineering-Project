"""
Crypto Trading Strategy Simulation

Metrics used:
- Volume Imbalance Metric (VIMB)
- Trade Dominance Ratio (TDR)
- True Range Volatility Score (TRVS)
"""

import requests
import pandas as pd

# Starting capital for absolute profit calculation
initial_capital = 10000

# --------------------------------
# 1. Ask user for asset
# --------------------------------

asset = input("Choose asset (BTC or ETH): ").upper()
pair = f"{asset}-USD"


# --------------------------------
# 2. Download data from Coinbase
# --------------------------------

def get_data(pair, granularity):

    url = f"https://api.exchange.coinbase.com/products/{pair}/candles?granularity={granularity}"

    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame(data, columns=[
        "time", "low", "high", "open", "close", "volume"
    ])

    df = df.sort_values("time").reset_index(drop=True)

    df["time"] = pd.to_datetime(df["time"], unit="s")

    return df.tail(2000)


# --------------------------------
# 3. Calculate metrics
# --------------------------------

def calculate_metrics(df):

    df["buy_volume"] = df.apply(
        lambda x: x["volume"] if x["close"] > x["open"] else 0,
        axis=1
    )

    df["sell_volume"] = df.apply(
        lambda x: x["volume"] if x["close"] < x["open"] else 0,
        axis=1
    )

    # VIMB
    df["VIMB"] = (
        (df["buy_volume"] - df["sell_volume"]) /
        (df["buy_volume"] + df["sell_volume"] + 1)
    )

    # Estimate trade counts
    df["buy_trades"] = df["buy_volume"] / 0.1
    df["sell_trades"] = df["sell_volume"] / 0.1

    # TDR
    df["TDR"] = df["buy_trades"] / (df["sell_trades"] + 1)

    # TRVS
    df["TRVS"] = (df["high"] - df["low"]) / df["open"]

    return df


# --------------------------------
# 4. Backtest trading strategy
# --------------------------------

def backtest(df):

    position = False
    entry_price = 0
    entry_index = 0

    profits = []
    consecutive_red = 0
    abs_profits = []

    for i in range(len(df)):

        row = df.loc[i]

        # --- BUY CONDITION ---
        cond1 = row["VIMB"] > 0
        cond2 = row["TDR"] >= 0.7
        cond3 = row["TRVS"] > 0.001

        metric_count = sum([cond1, cond2, cond3])

        buy_signal = metric_count >= 2 and row["close"] > row["open"]

        # BUY
        if not position and buy_signal:

            position = True
            entry_price = row["close"]
            entry_index = i
            consecutive_red = 0

        # EXIT
        elif position:

            if row["close"] < row["open"]:
                consecutive_red += 1
            else:
                consecutive_red = 0

            exit_signal = (
                row["VIMB"] < 0 or
                row["TRVS"] < 0.0005 or
                (i - entry_index) >= 5 or
                consecutive_red >= 2
            )

            if exit_signal:

                exit_price = row["close"]

                # percentage return
                profit = (exit_price - entry_price) / entry_price
                #profit = exit_price - entry_price
                abs = exit_price - entry_price

                profits.append(profit)
                abs_profits.append(abs)
                position = False

    total_return = sum(profits)
    trades = len(profits)
    abs = sum(abs_profits)

    start = df["time"].iloc[0]
    end = df["time"].iloc[-1]

    


    return total_return, trades, start, end, abs


# --------------------------------
# 5. Run hourly strategy
# --------------------------------

df_hour = get_data(pair, 3600)
df_hour = calculate_metrics(df_hour)

profit_hour, trades_hour, start_h, end_h, abs_hour = backtest(df_hour)

# --------------------------------
# 6. Run daily strategy
# --------------------------------

df_day = get_data(pair, 86400)
df_day = calculate_metrics(df_day)

profit_day, trades_day, start_d, end_d, abs_day = backtest(df_day)

# --------------------------------
# 7. Print results
# --------------------------------

print("\n----------------------------------")
print(f"You have chosen the {pair} pair.\n")

print(f"Hourly data from {start_h} to {end_h}")
print(f"Trades executed: {trades_hour}")
print(f"Profit/Loss: {profit_hour*100:.2f}% ({abs_hour:.2f} USD)\n")

print(f"Daily data from {start_d} to {end_d}")
print(f"Trades executed: {trades_day}")
print(f"Profit/Loss: {profit_day*100:.2f}% ({abs_day:.2f} USD)\n")

if profit_hour > profit_day:
    print("Therefore the Hourly trading is better!")
else:
    print("Therefore the Daily trading is better!")