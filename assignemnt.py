import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from gurobipy import Model, GRB, quicksum


# ============================================================
# section 1 - data loading and database creation
# in this first part, the script reads all txt files from the
# Data folder, extracts stock metadata and daily prices, and
# stores everything inside a sqlite database.
# this creates the base dataset used in the next sections.
# ============================================================

# connect to database
conn = sqlite3.connect("stocks.db")
cursor = conn.cursor()

# create tables if they do not exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS stocks (
    stock_id TEXT PRIMARY KEY,
    sector TEXT,
    shares INTEGER
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS prices (
    stock_id TEXT,
    date TEXT,
    price REAL,
    PRIMARY KEY (stock_id, date)
)
""")

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

        # stock information
        stock_id = lines[1]
        sector = lines[3]
        shares = int(lines[5])

        # insert stock metadata
        query = "INSERT OR REPLACE INTO stocks VALUES (?, ?, ?)"
        cursor.execute(query, (stock_id, sector, shares))

        # insert daily prices
        for i in range(7, len(lines)):

            row = lines[i]

            if row.strip() != "":
                parts = row.split(";")
                date = parts[0]
                price = float(parts[1])

                query = "INSERT OR REPLACE INTO prices VALUES (?, ?, ?)"
                cursor.execute(query, (stock_id, date, price))

conn.commit()

# read tables from database
query_stocks = "SELECT * FROM stocks"
query_prices = "SELECT * FROM prices"

stocks = pd.read_sql(query_stocks, conn)
prices = pd.read_sql(query_prices, conn)

# convert date column
date_column = prices["date"]
converted_dates = pd.to_datetime(date_column)
prices["date"] = converted_dates

# print example
print("stocks table:")
print(stocks.head())
print()


# ============================================================
# section 2 - return computation and sector analysis
# this section computes stock returns, merges them with stock
# information, and evaluates sector-level performance using
# market capitalization weighted returns.
# this is useful to identify top, middle, and bottom sectors
# before building the portfolio optimization model.
# ============================================================

# get first available price for each stock
price_start = prices.groupby("stock_id")["price"].first().reset_index()
price_start.columns = ["stock_id", "price_start"]

# get last available price for each stock
price_end = prices.groupby("stock_id")["price"].last().reset_index()
price_end.columns = ["stock_id", "price_end"]

# merge start and end prices
returns = price_start.merge(price_end, on="stock_id")

# compute return
returns["return"] = (returns["price_end"] - returns["price_start"]) / returns["price_start"]

# merge stock info and returns
df = returns.merge(stocks, on="stock_id")

# compute market capitalization
df["marketcap"] = df["shares"] * df["price_start"]

# compute weighted return
df["weighted_return"] = df["return"] * df["marketcap"]

# group by sector
grouped = df.groupby("sector")

# sum marketcap and weighted return
total_marketcap = grouped["marketcap"].sum()
total_weighted_return = grouped["weighted_return"].sum()

# build sector dataframe
sector_returns = pd.DataFrame()
sector_returns["total_marketcap"] = total_marketcap
sector_returns["avg_return"] = total_weighted_return
sector_returns = sector_returns.reset_index()

# compute average weighted return
sector_returns["avg_return"] = (
    sector_returns["avg_return"] / sector_returns["total_marketcap"]
)

print("sector returns:")
print(sector_returns)
print()

# sort sector returns for plotting
sector_returns = sector_returns.sort_values("avg_return")

# create figure
plt.figure(figsize=(10, 6))

# define colors based on quartiles
colors = []

q75 = sector_returns["avg_return"].quantile(0.75)
q25 = sector_returns["avg_return"].quantile(0.25)

for value in sector_returns["avg_return"]:

    if value >= q75:
        colors.append("green")

    elif value <= q25:
        colors.append("red")

    else:
        colors.append("gray")

# create horizontal bar chart
plt.barh(
    sector_returns["sector"],
    sector_returns["avg_return"],
    color=colors
)

# compute mean return
mean_return = sector_returns["avg_return"].mean()

# draw average line
plt.axvline(
    mean_return,
    linestyle="--",
    label="Average return"
)

# labels and title
plt.xlabel("Average sector return (2025)")
plt.ylabel("Sector")
plt.title("Sector Performance in 2025")
plt.legend()

plt.tight_layout()
plt.show()

# sort sectors descending
sector_returns = sector_returns.sort_values("avg_return", ascending=False)

# compute number of sectors in each group
n = len(sector_returns)
quarter = int(0.25 * n)

# define top, middle, bottom sectors
top_rows = sector_returns.iloc[0:quarter]
top = top_rows["sector"].tolist()

bottom_rows = sector_returns.iloc[n - quarter:n]
bottom = bottom_rows["sector"].tolist()

middle_rows = sector_returns.iloc[quarter:n - quarter]
middle = middle_rows["sector"].tolist()

print("top sectors:", top)
print("middle sectors:", middle)
print("bottom sectors:", bottom)
print()


# ============================================================
# section 3 - portfolio optimization and portfolio analysis
# this section creates and solves the optimization model.
# the objective is to maximize expected portfolio return under
# investment constraints, including stock concentration and
# sector allocation rules.
# after solving the model, the selected portfolio is analyzed
# both by sector allocation and by stock contribution.
# ============================================================

# list of all stocks
stocks_list = df["stock_id"].tolist()

# dictionary of returns
returns_dict = {}
for i in range(len(df)):
    row = df.iloc[i]
    stock_id = row["stock_id"]
    stock_return = row["return"]
    returns_dict[stock_id] = stock_return

# dictionary of sectors
sector_dict = {}
for i in range(len(df)):
    row = df.iloc[i]
    stock_id = row["stock_id"]
    sector = row["sector"]
    sector_dict[stock_id] = sector

# create optimization model
model = Model()

# decision variables
# x[i] = fraction invested in stock i
x = model.addVars(stocks_list, lb=0, ub=0.05)

# objective function
objective = 0
for stock_id in stocks_list:
    objective = objective + returns_dict[stock_id] * x[stock_id]

model.setObjective(objective, GRB.MAXIMIZE)

# full investment constraint
total_investment = 0
for stock_id in stocks_list:
    total_investment = total_investment + x[stock_id]

model.addConstr(total_investment == 1)

# top sector allocation
top_investment = 0
for stock_id in stocks_list:
    sector = sector_dict[stock_id]
    if sector in top:
        top_investment = top_investment + x[stock_id]

model.addConstr(top_investment == 0.5)

# middle sector allocation
middle_investment = 0
for stock_id in stocks_list:
    sector = sector_dict[stock_id]
    if sector in middle:
        middle_investment = middle_investment + x[stock_id]

model.addConstr(middle_investment == 0.3)

# bottom sector allocation
bottom_investment = 0
for stock_id in stocks_list:
    sector = sector_dict[stock_id]
    if sector in bottom:
        bottom_investment = bottom_investment + x[stock_id]

model.addConstr(bottom_investment == 0.2)

# solve model
model.optimize()

# build portfolio dictionary with positive values only
portfolio = {}

for stock_id in stocks_list:
    value = x[stock_id].X
    if value > 0:
        portfolio[stock_id] = value

# convert to dataframe
items = list(portfolio.items())
portfolio_df = pd.DataFrame(items)
portfolio_df.columns = ["stock", "weight"]

print("optimized portfolio:")
print(portfolio_df)
print()

# merge portfolio with stock information
sector_portfolio = portfolio_df.merge(stocks, left_on="stock", right_on="stock_id")

# group by sector
grouped = sector_portfolio.groupby("sector")

# sum weight for each sector
sector_alloc = grouped["weight"].sum()

# convert index to column
sector_alloc = sector_alloc.reset_index()

# sort by weight
sector_alloc = sector_alloc.sort_values("weight", ascending=True)

# create plot
plt.figure(figsize=(10, 7))

# create horizontal bars
bars = plt.barh(sector_alloc["sector"], sector_alloc["weight"])

plt.xlabel("Total Allocation")
plt.ylabel("Sector")
plt.title("Portfolio Allocation by Sector")

# add values near bars
for i in range(len(bars)):
    bar = bars[i]
    value = sector_alloc["weight"].iloc[i]
    x_position = bar.get_width() + 0.002
    y_position = bar.get_y() + bar.get_height() / 2
    text_value = str(round(value * 100, 1)) + "%"

    plt.text(x_position, y_position, text_value, va="center")

plt.tight_layout()
plt.show()

# merge portfolio with return information
portfolio_analysis = portfolio_df.merge(
    df[["stock_id", "sector", "return"]],
    left_on="stock",
    right_on="stock_id"
)

# compute stock contribution
portfolio_analysis["contribution"] = (
    portfolio_analysis["weight"] * portfolio_analysis["return"]
)

# sort by contribution
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

# add values near bars
for i in range(len(bars)):

    bar = bars[i]
    value = portfolio_analysis["contribution"].iloc[i]

    x_position = bar.get_width() + 0.001
    y_position = bar.get_y() + bar.get_height() / 2
    text_value = str(round(value * 100, 2)) + "%"

    plt.text(x_position, y_position, text_value, va="center")

plt.tight_layout()
plt.show()

# count selected stocks by sector
grouped = sector_portfolio.groupby("sector")
selected_count = grouped["stock"].count()
selected_count = selected_count.sort_values(ascending=True)

# create figure
plt.figure(figsize=(10, 7))

# create horizontal bar chart
bars = plt.barh(selected_count.index, selected_count.values)

# labels and title
plt.xlabel("Number of Selected Stocks")
plt.ylabel("Sector")
plt.title("Number of Selected Stocks by Sector")

# add values near bars
for i in range(len(bars)):

    bar = bars[i]
    value = selected_count.values[i]

    x_position = bar.get_width() + 0.1
    y_position = bar.get_y() + bar.get_height() / 2
    text_value = str(value)

    plt.text(x_position, y_position, text_value, va="center")

plt.tight_layout()
plt.show()


# ============================================================
# section 4 - strategy sensitivity analysis
# in this final section, the optimization model is solved
# again under different concentration limits.
# this helps evaluate whether the strategy is sensitive to
# the maximum investment allowed per stock.
# the three tested scenarios are 5%, 10%, and 20%.
# ============================================================

# define scenarios
max_limits = [0.05, 0.10, 0.20]

results = []

# solve the model for each scenario
for limit in max_limits:

    model = Model()
    x = model.addVars(stocks_list, lb=0, ub=limit)

    # objective function
    objective = 0
    for stock_id in stocks_list:
        objective = objective + returns_dict[stock_id] * x[stock_id]

    model.setObjective(objective, GRB.MAXIMIZE)

    # full investment constraint
    total_investment = 0
    for stock_id in stocks_list:
        total_investment = total_investment + x[stock_id]

    model.addConstr(total_investment == 1)

    # top sector allocation
    top_investment = 0
    for stock_id in stocks_list:
        if sector_dict[stock_id] in top:
            top_investment = top_investment + x[stock_id]

    model.addConstr(top_investment == 0.5)

    # middle sector allocation
    middle_investment = 0
    for stock_id in stocks_list:
        if sector_dict[stock_id] in middle:
            middle_investment = middle_investment + x[stock_id]

    model.addConstr(middle_investment == 0.3)

    # bottom sector allocation
    bottom_investment = 0
    for stock_id in stocks_list:
        if sector_dict[stock_id] in bottom:
            bottom_investment = bottom_investment + x[stock_id]

    model.addConstr(bottom_investment == 0.2)

    # solve model
    model.optimize()

    # save scenario result
    results.append([limit, model.objVal])

# convert results to dataframe
sensitivity_df = pd.DataFrame(results, columns=["max_weight", "expected_return"])

print("sensitivity analysis:")
print(sensitivity_df)
print()

# create figure
plt.figure(figsize=(9, 6))

# prepare x and y values
x_values = sensitivity_df["max_weight"].astype(str)
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

    x_position = bar.get_x() + bar.get_width() / 2
    y_position = bar.get_height() + 0.002
    text_value = str(round(value * 100, 2)) + "%"

    plt.text(
        x_position,
        y_position,
        text_value,
        ha="center"
    )

plt.tight_layout()
plt.show()


# close database connection
conn.close()