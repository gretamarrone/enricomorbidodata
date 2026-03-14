# 1.1
import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from gurobipy import Model, GRB, quicksum
import matplotlib.pyplot as plt

# 1.2
# creating SQLite database
conn = sqlite3.connect("stocks.db")
cursor = conn.cursor()

# creating table for stock information
cursor.execute("""
CREATE TABLE IF NOT EXISTS stocks(
    stock_id TEXT PRIMARY KEY,
    sector TEXT,
    shares INTEGER
)
""")

# creating table for daily prices
cursor.execute("""
CREATE TABLE IF NOT EXISTS prices(
    stock_id TEXT,
    date TEXT,
    price REAL,
    PRIMARY KEY (stock_id, date),
    FOREIGN KEY (stock_id) REFERENCES stocks(stock_id)
)
""")

# useful indexes for queries and joins
cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_stocks_sector
ON stocks(sector)
""")

cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_prices_stock_date
ON prices(stock_id, date)
""")

conn.commit()


# 1.3
# data path
data_folder = "Data"

for file in os.listdir(data_folder):
    if file.endswith(".txt"):
        path = os.path.join(data_folder, file)

        with open(path, "r") as f:
            lines = f.read().splitlines()

        # extracting stock information
        stock_id = lines[1]
        sector = lines[3]
        shares = int(lines[5])

        # inserting stock metadata
        cursor.execute(
            "INSERT OR REPLACE INTO stocks VALUES (?, ?, ?)",
            (stock_id, sector, shares)
        )

        # inserting daily prices
        for row in lines[7:]:
            date, price = row.split(";")

            cursor.execute(
                "INSERT OR REPLACE INTO prices VALUES (?, ?, ?)",
                (stock_id, date, float(price))
            )

conn.commit()
conn.close()


# 1.4
# connecting
conn = sqlite3.connect("stocks.db")

stocks = pd.read_sql("SELECT * FROM stocks", conn)
prices = pd.read_sql("SELECT * FROM prices", conn)

prices["date"] = pd.to_datetime(prices["date"])

# printing example
stocks.head()


# 1.5
# creating a function 
def query_stocks(conn, sectors=None, min_price=None):
    query = """
    WITH latest_prices AS (
        SELECT p1.stock_id, p1.date, p1.price
        FROM prices p1
        JOIN (
            SELECT stock_id, MAX(date) AS max_date
            FROM prices
            GROUP BY stock_id
        ) p2
        ON p1.stock_id = p2.stock_id AND p1.date = p2.max_date
    )
    SELECT s.stock_id, s.sector, s.shares, lp.price
    FROM stocks s
    JOIN latest_prices lp ON s.stock_id = lp.stock_id
    """

    conditions = []
    params = []

    if sectors is not None and len(sectors) > 0:
        placeholders = ",".join(["?"] * len(sectors))
        conditions.append(f"s.sector IN ({placeholders})")
        params.extend(sectors)

    if min_price is not None:
        conditions.append("lp.price >= ?")
        params.append(min_price)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    return pd.read_sql(query, conn, params=params)

# usage
conn = sqlite3.connect("stocks.db")
filtered = query_stocks(conn, sectors=["AI"], min_price=100)
filtered.head()

stocks["sector"].unique()


# 2.1
# sorting by stock and date
prices = prices.sort_values(["stock_id", "date"]).copy()

# first available price for each stock
# in this dataset it corresponds to the last closing price of 2024
start_prices = prices.groupby("stock_id").first().reset_index()

# last available price for each stock
# in this dataset it corresponds to the last available closing price in 2025
end_prices = prices.groupby("stock_id").last().reset_index()

# merging in a new table
returns = start_prices[["stock_id", "price"]].merge(
    end_prices[["stock_id", "price"]],
    on="stock_id",
    suffixes=("_start", "_end")
)

# computing return in 2025
returns["return"] = (returns["price_end"] - returns["price_start"]) / returns["price_start"]

returns.head()


#2.2
data = returns["return"]

plt.figure(figsize=(8,6))

plt.hist(data,bins=35,color="steelblue",edgecolor="white")

# mean line
mean_return = data.mean()
plt.axvline(mean_return, color="red", linestyle="--", label=f"Mean: {mean_return:.1%}")

plt.xlabel("Stock Return (2025)")
plt.ylabel("Number of Stocks")
plt.title("Distribution of Stock Returns in 2025")

plt.grid(axis="y", alpha=0.3)

plt.legend()

plt.tight_layout()
plt.show()


#2.3
sector_counts = stocks["sector"].value_counts().sort_values(ascending=True)

plt.figure(figsize=(10, 7))
bars = plt.barh(sector_counts.index, sector_counts.values)

plt.xlabel("Number of Stocks")
plt.ylabel("Sector")
plt.title("Number of Stocks by Sector")

for bar, value in zip(bars, sector_counts.values):
    plt.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2, str(value), va="center")

plt.tight_layout()
plt.show()


#2.4
# merging stock info and returns
df = returns.merge(stocks, on="stock_id")

# computing market capitalization
df["marketcap"] = df["shares"] * df["price_start"]

# weighted sector return
# group by sector, 
# then sum (return*marketcap) / sum(marketcap) 
sector_returns = (
    df.assign(weighted_return=df["return"] * df["marketcap"])
      .groupby("sector")
      .agg(
          total_marketcap=("marketcap", "sum"),
          avg_return=("weighted_return", lambda x: x.sum())
      )
      .reset_index()
)

sector_returns["avg_return"] = sector_returns["avg_return"] / sector_returns["total_marketcap"]
sector_returns


#2.5
# sorting 
sector_returns = sector_returns.sort_values("avg_return")

# plotting
plt.figure(figsize=(10,6))

colors = []
for val in sector_returns["avg_return"]:
    if val >= sector_returns["avg_return"].quantile(0.75):
        colors.append("green")
    elif val <= sector_returns["avg_return"].quantile(0.25):
        colors.append("red")
    else:
        colors.append("gray")

plt.barh(sector_returns["sector"], sector_returns["avg_return"], color=colors)

plt.axvline(sector_returns["avg_return"].mean(),
            linestyle="--",
            label="Average return")

plt.xlabel("Average sector return (2025)")
plt.ylabel("Sector")
plt.title("Sector Performance in 2025")
plt.legend()

plt.show()
# sorting 
sector_returns = sector_returns.sort_values("avg_return")

# plotting
plt.figure(figsize=(10,6))

colors = []
for val in sector_returns["avg_return"]:
    if val >= sector_returns["avg_return"].quantile(0.75):
        colors.append("green")
    elif val <= sector_returns["avg_return"].quantile(0.25):
        colors.append("red")
    else:
        colors.append("gray")

plt.barh(sector_returns["sector"], sector_returns["avg_return"], color=colors)

plt.axvline(sector_returns["avg_return"].mean(),
            linestyle="--",
            label="Average return")

plt.xlabel("Average sector return (2025)")
plt.ylabel("Sector")
plt.title("Sector Performance in 2025")
plt.legend()

plt.show()


#2.6
# sorting desc
sector_returns = sector_returns.sort_values("avg_return", ascending=False)

# number of sector
n = len(sector_returns)

# computing 
top = sector_returns.iloc[:int(0.25*n)]["sector"].tolist()
bottom = sector_returns.iloc[-int(0.25*n):]["sector"].tolist()
middle = sector_returns.iloc[int(0.25*n):-int(0.25*n)]["sector"].tolist()

top, middle, bottom


#3.1
# stock as list
stocks_list = df["stock_id"].tolist()

# dictionary, in order to get the corrisponding value easier later
returns_dict = dict(zip(df["stock_id"], df["return"]))
sector_dict = dict(zip(df["stock_id"], df["sector"]))

# matematical model
model = Model()

# xi = fraction of investment in i  
# decision variables, lower and upper bound
x = model.addVars(stocks_list, lb=0, ub=0.05)

# objective function: maximazing return * investment 
model.setObjective(
    quicksum(returns_dict[i] * x[i] for i in stocks_list),
    GRB.MAXIMIZE
)

# full investment: total sum xi = 1
model.addConstr(quicksum(x[i] for i in stocks_list) == 1)

# sector constraints
model.addConstr(quicksum(x[i] for i in stocks_list if sector_dict[i] in top) == 0.5)
model.addConstr(quicksum(x[i] for i in stocks_list if sector_dict[i] in middle) == 0.3)
model.addConstr(quicksum(x[i] for i in stocks_list if sector_dict[i] in bottom) == 0.2)

# solce model
model.optimize()


#3.2
# x[i].X = optimal value (from the model) of x[i]
# take only > 0 in a dictionary
portfolio = {i: x[i].X for i in stocks_list if x[i].X > 0}

# converting in dataframe
portfolio_df = pd.DataFrame(
    portfolio.items(),
    columns=["stock", "weight"]
)
# print 
portfolio_df



#3.3
sector_portfolio = portfolio_df.merge(stocks, left_on="stock", right_on="stock_id")
sector_alloc = sector_portfolio.groupby("sector", as_index=False)["weight"].sum()
sector_alloc = sector_alloc.sort_values("weight", ascending=True)

plt.figure(figsize=(10, 7))
bars = plt.barh(sector_alloc["sector"], sector_alloc["weight"])

plt.xlabel("Total Allocation")
plt.ylabel("Sector")
plt.title("Portfolio Allocation by Sector")

for bar, value in zip(bars, sector_alloc["weight"]):
    plt.text(bar.get_width() + 0.002, bar.get_y() + bar.get_height()/2, f"{value:.1%}", va="center")

plt.tight_layout()
plt.show()



portfolio_analysis = portfolio_df.merge(
    df[["stock_id", "sector", "return"]],
    left_on="stock",
    right_on="stock_id",
    how="left"
)

portfolio_analysis["contribution"] = portfolio_analysis["weight"] * portfolio_analysis["return"]
portfolio_analysis = portfolio_analysis.sort_values("contribution", ascending=True)

plt.figure(figsize=(10, 7))
bars = plt.barh(portfolio_analysis["stock"], portfolio_analysis["contribution"])

plt.xlabel("Contribution to Portfolio Return")
plt.ylabel("Stock")
plt.title("Stock Contribution to Expected Portfolio Return")

for bar, value in zip(bars, portfolio_analysis["contribution"]):
    plt.text(bar.get_width() + 0.001, bar.get_y() + bar.get_height()/2, f"{value:.2%}", va="center")

plt.tight_layout()
plt.show()

selected_count = (
    sector_portfolio.groupby("sector")["stock"]
    .count()
    .sort_values(ascending=True)
)

plt.figure(figsize=(10, 7))
bars = plt.barh(selected_count.index, selected_count.values)

plt.xlabel("Number of Selected Stocks")
plt.ylabel("Sector")
plt.title("Number of Selected Stocks by Sector")

for bar, value in zip(bars, selected_count.values):
    plt.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2, str(value), va="center")

plt.tight_layout()
plt.show()



#4.1
# three scenarios
max_limits = [0.05, 0.10, 0.20]

results = []

# iterate over the three scenarios, maximize and find the best
for limit in max_limits:

    model = Model()
    x = model.addVars(stocks_list, lb=0, ub=limit)

    model.setObjective(
        quicksum(returns_dict[i] * x[i] for i in stocks_list),
        GRB.MAXIMIZE
    )
    model.addConstr(quicksum(x[i] for i in stocks_list) == 1)

    model.addConstr(quicksum(x[i] for i in stocks_list if sector_dict[i] in top) == 0.5)
    model.addConstr(quicksum(x[i] for i in stocks_list if sector_dict[i] in middle) == 0.3)
    model.addConstr(quicksum(x[i] for i in stocks_list if sector_dict[i] in bottom) == 0.2)

    model.optimize()

    # save current results 
    results.append(model.objVal)

# print results
results


# sensitivity
sensitivity_df = pd.DataFrame({
    "max_weight": max_limits,
    "expected_return": results
})

sensitivity_df

# plot
plt.figure(figsize=(9, 6))
bars = plt.bar(
    sensitivity_df["max_weight"].astype(str),
    sensitivity_df["expected_return"]
)

plt.xlabel("Maximum Investment per Stock")
plt.ylabel("Expected Portfolio Return")
plt.title("Sensitivity of Portfolio Return to the Concentration Limit")

for bar, value in zip(bars, sensitivity_df["expected_return"]):
    plt.text(
        bar.get_x() + bar.get_width()/2,
        bar.get_height() + 0.002,
        f"{value:.2%}",
        ha="center"
    )

plt.tight_layout()
plt.show()