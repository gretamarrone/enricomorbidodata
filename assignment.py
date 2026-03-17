# --- SECTION 1: Database creation and data loading ---
# 1.1
import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from gurobipy import Model, GRB, quicksum

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
files = os.listdir(data_folder)

for file in files:
    if file.endswith(".txt"):
        path = data_folder + "/" + file

        f = open(path, "r")
        text = f.read()
        f.close()
        lines = text.split("\n")

        # extracting stock information
        stock_id = lines[1]
        sector = lines[3]
        shares = int(lines[5])

        # inserting stock metadata
        query = "INSERT OR REPLACE INTO stocks VALUES (?, ?, ?)"
        cursor.execute(query, (stock_id, sector, shares))

        # inserting daily prices
        for i in range(7, len(lines)):
            row = lines[i]
            parts = row.split(";")
            date = parts[0]
            price = float(parts[1])

            query = "INSERT OR REPLACE INTO prices VALUES (?, ?, ?)"
            cursor.execute(query, (stock_id, date, price))

conn.commit()
conn.close()

# 1.4
# connecting
conn = sqlite3.connect("stocks.db")

# query to get tables
query_stocks = "SELECT * FROM stocks"
query_prices = "SELECT * FROM prices"

# reading tables
stocks = pd.read_sql(query_stocks, conn)
prices = pd.read_sql(query_prices, conn)

# conversion column into datetime format
date_column = prices["date"]
converted_dates = pd.to_datetime(date_column)
prices["date"] = converted_dates

# printing example
example = stocks.head()
print(example)


# 1.5
# creating a function to query the db
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

    # list for saving where condition
    conditions = []

    # list query params
    params = []

    # check sectors
    if sectors is not None and len(sectors) > 0:
            placeholders_list = []
            for i in range(len(sectors)):
                placeholders_list.append("?")
            placeholders = ",".join(placeholders_list)
            condition = "s.sector IN (" + placeholders + ")"
            conditions.append(condition)
            for sector in sectors:
                params.append(sector)

    # check min price
    if min_price is not None:
        condition = "lp.price >= ?"
        conditions.append(condition)
        params.append(min_price)

    # if both conditions
    if len(conditions) > 0:
        where_part = " AND ".join(conditions)
        query = query + " WHERE " + where_part

    result = pd.read_sql(query, conn, params=params)
    return result

# usage
conn = sqlite3.connect("stocks.db")
filtered = query_stocks(conn, sectors=["AI"], min_price=100)
filtered.head()

stocks["sector"].unique()



# --- SECTION 2: Sector performance in 2025 ---
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


# 2.2
# data to plot: returns
data = returns["return"]

# creating the plot
plt.figure(figsize=(8,6))

# make intohistogram
plt.hist(data,bins=35,color="steelblue",edgecolor="white")

# mean line
mean_return = data.mean()
plt.axvline(mean_return, color="red", linestyle="--", label=f"Mean: {mean_return:.1%}")

# x and y labels
plt.xlabel("Stock Return (2025)")
plt.ylabel("Number of Stocks")

#title
plt.title("Distribution of Stock Returns in 2025")

# adding y lines with opacity
plt.grid(axis="y", alpha=0.3)

plt.legend()

plt.show()


# 2.3
# data to show: number of stocks by sector 
sector_counts = stocks["sector"].value_counts().sort_values(ascending=True)

plt.figure(figsize=(10, 7))
bars = plt.barh(sector_counts.index, sector_counts.values)
plt.xlabel("Number of Stocks")
plt.ylabel("Sector")
plt.title("Number of Stocks by Sector")

for bar, value in zip(bars, sector_counts.values):
    plt.text(bar.get_width() + 0.5, 
             bar.get_y() + bar.get_height()/2, 
             str(value), 
             va="center")

plt.tight_layout()
plt.show()


# 2.4
# merging stock info and returns
df = returns.merge(stocks, on="stock_id")

# computing market capitalization
df["marketcap"] = df["shares"] * df["price_start"]

# weighted sector return
df["weighted_return"] = df["return"] * df["marketcap"]

# group by sector
grouped = df.groupby("sector")

# sum marketcap per sector
total_marketcap = grouped["marketcap"].sum()

# sum weighted returns
total_weighted_return = grouped["weighted_return"].sum()

# new dataframe with results
sector_returns = pd.DataFrame()
sector_returns["total_marketcap"] = total_marketcap
sector_returns["avg_return"] = total_weighted_return
sector_returns = sector_returns.reset_index()

# then sum / sum(marketcap) 
sector_returns["avg_return"] = (
    sector_returns["avg_return"] / sector_returns["total_marketcap"]
)

sector_returns


# 2.5
# sorting 
sector_returns = sector_returns.sort_values("avg_return")

# plotting
plt.figure(figsize=(10,6))

# quantile for colors
q75 = sector_returns["avg_return"].quantile(0.75)
q25 = sector_returns["avg_return"].quantile(0.25)

# list of color
colors = []
for value in sector_returns["avg_return"]:
    if value >= q75:
        colors.append("green")
    elif value <= q25:
        colors.append("red")
    else:
        colors.append("gray")

plt.barh(sector_returns["sector"], 
         sector_returns["avg_return"], 
         color=colors)

# mean for line
mean_return = sector_returns["avg_return"].mean()

plt.axvline(mean_return,
            linestyle="--",
            label="Average return")

plt.xlabel("Average sector return (2025)")
plt.ylabel("Sector")
plt.title("Sector Performance in 2025")
plt.legend()

plt.show()


# 2.5
# sorting desc
sector_returns = sector_returns.sort_values("avg_return", ascending=False)

# number of sector
n = len(sector_returns)

# compute 25% of total 
quarter = int(0.25 * n)

# computing 
# top
top_rows = sector_returns.iloc[0:quarter]
top = top_rows["sector"].tolist()

# bottom 
bottom_rows = sector_returns.iloc[n - quarter:n]
bottom = bottom_rows["sector"].tolist()

# middle
middle_rows = sector_returns.iloc[quarter:n - quarter]
middle = middle_rows["sector"].tolist()

top, middle, bottom

# stock as list
stocks_list = df["stock_id"].tolist()

# dictionary, in order to get the corrisponding value easier later
# returns 
returns_dict = {}
for i in range(len(df)):
    stock_id = df.iloc[i]["stock_id"]
    stock_return = df.iloc[i]["return"]
    returns_dict[stock_id] = stock_return

# sectors
sector_dict = {}
for i in range(len(df)):
    stock_id = df.iloc[i]["stock_id"]
    sector = df.iloc[i]["sector"]
    sector_dict[stock_id] = sector


# --- SECTION 3: Portfolio Optimization Model  ---
# 3.1
# matematical model
model = Model()

# xi = fraction of investment in i  
# decision variables, lower and upper bound
x = model.addVars(stocks_list, lb=0, ub=0.05)

# objective function: maximazing return * investment 
objective = 0
for stock_id in stocks_list:
    objective = objective + returns_dict[stock_id] * x[stock_id]
model.setObjective(objective, GRB.MAXIMIZE)

# constraint: total sum xi = 1
total_investment = 0
for stock_id in stocks_list:
    total_investment = total_investment + x[stock_id]
model.addConstr(total_investment == 1)

# constraint: top sectors
top_investment = 0
for stock_id in stocks_list:
    if sector_dict[stock_id] in top:
        top_investment = top_investment + x[stock_id]
model.addConstr(top_investment == 0.5)

# constraint: middle sectors
middle_investment = 0
for stock_id in stocks_list:
    if sector_dict[stock_id] in middle:
        middle_investment = middle_investment + x[stock_id]
model.addConstr(middle_investment == 0.3)

# constraint: bottom sectors
bottom_investment = 0
for stock_id in stocks_list:
    if sector_dict[stock_id] in bottom:
        bottom_investment = bottom_investment + x[stock_id]
model.addConstr(bottom_investment == 0.2)

# solve model
model.optimize()


# 3.2
# x[i].X = optimal value (from the model) of x[i]
# take only > 0 in a dictionary
portfolio = {}

# all stocks
for stock_id in stocks_list:
    # optimal x[i] value
    value = x[stock_id].X
    # keep only > 0
    if value > 0:
        portfolio[stock_id] = value


# converting in dataframe
items = list(portfolio.items())

# create dataframe
portfolio_df = pd.DataFrame(items)

# rename columns
portfolio_df.columns = ["stock", "weight"]

# print 
portfolio_df


# 3.3
# merge portfolio info with stock
sector_portfolio = portfolio_df.merge(stocks, left_on="stock", right_on="stock_id")
#group by sector
sector_alloc = sector_portfolio.groupby("sector")
# sum weight for each sector
sector_alloc = sector_alloc["weight"].sum()
# index column
sector_alloc = sector_alloc.reset_index()
# sort weight
sector_alloc = sector_alloc.sort_values("weight", ascending=True)



# create plot
plt.figure(figsize=(10, 7))
bars = plt.barh(sector_alloc["sector"], sector_alloc["weight"])

plt.xlabel("Total Allocation")
plt.ylabel("Sector")
plt.title("Portfolio Allocation by Sector")

# horizontal bar
bars = plt.barh(sector_alloc["sector"], sector_alloc["weight"])

plt.xlabel("Total Allocation")
plt.ylabel("Sector")
plt.title("Portfolio Allocation by Sector")

# value near bar
for i in range(len(bars)):
    bar = bars[i]
    value = sector_alloc["weight"].iloc[i]
    x_position = bar.get_width() + 0.002
    y_position = bar.get_y() + bar.get_height() / 2
    text_value = str(round(value * 100, 1)) + "%"
    
plt.show()

# 3.3 - 2
# merge portfolio with stock information (sector and return)
portfolio_analysis = portfolio_df.merge(
    df[["stock_id", "sector", "return"]],
    left_on="stock",
    right_on="stock_id"
)

# compute contribution of each stock to portfolio return
portfolio_analysis["contribution"] = (
    portfolio_analysis["weight"] * portfolio_analysis["return"]
)

# sort stocks by contribution
portfolio_analysis = portfolio_analysis.sort_values("contribution", ascending=True)

# create figure
plt.figure(figsize=(10, 7))

# create horizontal bar chart
bars = plt.barh(
    portfolio_analysis["stock"],
    portfolio_analysis["contribution"]
)

# labels and title
plt.xlabel("Contribution to Portfolio Return")
plt.ylabel("Stock")
plt.title("Stock Contribution to Expected Portfolio Return")

# add percentage value near each bar
for i in range(len(bars)):

    bar = bars[i]
    value = portfolio_analysis["contribution"].iloc[i]

    # compute text position
    x_position = bar.get_width() + 0.001
    y_position = bar.get_y() + bar.get_height() / 2

    # convert value to percentage string
    text_value = str(round(value * 100, 2)) + "%"

    plt.text(x_position, y_position, text_value, va="center")

# adjust layout to avoid overlapping
plt.tight_layout()

# show plot
plt.show()

# 3.3 - 3
# count how many selected stocks belong to each sector
grouped = sector_portfolio.groupby("sector")

# count stocks in each sector
selected_count = grouped["stock"].count()

# sort sectors by number of stocks
selected_count = selected_count.sort_values(ascending=True)

# create figure
plt.figure(figsize=(10, 7))

# create horizontal bar chart
bars = plt.barh(selected_count.index, selected_count.values)

# labels and title
plt.xlabel("Number of Selected Stocks")
plt.ylabel("Sector")
plt.title("Number of Selected Stocks by Sector")

# add value near each bar
for i in range(len(bars)):

    bar = bars[i]
    value = selected_count.values[i]

    # compute position of the text
    x_position = bar.get_width() + 0.1
    y_position = bar.get_y() + bar.get_height() / 2

    # convert number to string
    text_value = str(value)

    plt.text(x_position, y_position, text_value, va="center")

# adjust spacing in the figure
plt.tight_layout()

# show plot
plt.show()


# --- SECTION 4: Strategy Sensitivity Analysis ---
# 4.1
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


# PLOT
# create figure
plt.figure(figsize=(9, 6))

# convert max_weight to string for x-axis labels
x_values = sensitivity_df["max_weight"].astype(str)

# take expected portfolio return values
y_values = sensitivity_df["expected_return"]

# create bar chart
bars = plt.bar(x_values, y_values)

# labels and title
plt.xlabel("Maximum Investment per Stock")
plt.ylabel("Expected Portfolio Return")
plt.title("Sensitivity of Portfolio Return to the Concentration Limit")

# add value above each bar
for i in range(len(bars)):

    bar = bars[i]
    value = y_values.iloc[i]

    # compute text position
    x_position = bar.get_x() + bar.get_width() / 2
    y_position = bar.get_height() + 0.01

    # convert value to percentage
    text_value = str(round(value * 100, 2)) + "%"

    plt.text(
        x_position,
        y_position,
        text_value,
        ha="center"
    )

# adjust layout to avoid overlapping
plt.tight_layout()

# show plot
plt.show()