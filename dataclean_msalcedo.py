
import pandas as pd

#loading the stock market csv file
df = pd.read_csv("stock_market.csv")

#i want to know the size of the data (rows,columns)
print(df.shape)

#first and last 5 rows
print(df.head())
print(df.tail())

#knowing the data types, columns names, and how many missing values
print(df.columns)
print(df.info())
#how many values are present
print(df.isnull().sum())


#snake_case 
df.columns = (
    df.columns.str.strip() #removes whitespaces
            .str.lower() #lowercase
            .str.replace(" ", "_") #replscing spaces w/ underscores
            .str.replace("-","_") #replacing dashes w/ underscores
)
#trimming whitespace
df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

#mapping missing values with NaN
missing_values = ["", "na", "n/a", "null", "-"]
df = df.replace(missing_values, pd.NA)

#yyyy-MM-dd format
df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')

#testing to see changes
print(df.head(50))
print(df.shape)
print(df.info())
print(df.isnull().sum())



target_schema = {
    "trade_date": "datetime64[ns]",
    "ticker": "string",
    "open_price": "float64",
    "close_price": "float64",
    "volume": "int64",
    "sector": "string",
    "validated": "bool",
    "currency": "string",
    "exchange": "string",
    "notes": "string"
}

df["open_price"] = pd.to_numeric(df["open_price"], errors="coerce")
df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
df["volume"] = pd.to_numeric(df["volume"], errors="coerce", downcast="integer")
df["validated"] = df["validated"].str.lower().map({"yes": True, "no": False})

for col in ["ticker", "sector", "currency","exchange", "notes"]:
    df[col] = df[col].astype("string")


for col, dtype in target_schema.items():
    if col in df.columns:
        df[col] = df[col].astype(dtype, errors="ignore")

print(df.dtypes)
    

df = df.drop_duplicates()
df.to_parquet("cleaned.parquet", index=False)


import pandas as pd

df = pd.read_parquet("cleaned.parquet")

agg1 = (
    df.groupby(["trade_date", "ticker"])["close_price"]
    .mean()
    .reset_index(name="avg_close")
)

#saving
agg1.to_parquet("agg1.parquet", index=False)

agg2 = (
    df.groupby("sector")["volume"]
    .mean()
    .reset_index(name="avg_volume")
)

#saving
agg2.to_parquet("agg2.parquet", index=False)


df["daily_return"] = (df["close_price"] - df["open_price"]) / df["open_price"]

agg3 = (
    df.groupby(["trade_date", "ticker"])["daily_return"]
    .mean()
    .reset_index(name="avg_daily_return")
)

#saving
agg3.to_parquet("agg3.parquet", index=False)

