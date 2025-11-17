
import streamlit as st
import pandas as pd

#loading the parquet files
df_agg1 = pd.read_parquet("agg1.parquet")
df_agg2 = pd.read_parquet("agg2.parquet")
df_agg3 = pd.read_parquet("agg3.parquet")

st.title("Stock Data")
st.write("Filters below")

#taking the min & max from agg1 to use as the default range
min = df_agg1['trade_date'].min()
max = df_agg1["trade_date"].max()
date_range = st.sidebar.date_input("Date range", [min, max])

#ticker filter
ticker = st.sidebar.selectbox("Ticker", sorted(df_agg1['ticker'].unique()))

#keeping only the rows that match the date range and ticker
filtered_agg1 = df_agg1[
    (df_agg1['trade_date'] >= pd.to_datetime(date_range[0])) &
    (df_agg1['trade_date'] <= pd.to_datetime(date_range[1])) &
    (df_agg1['ticker'] == ticker)
]

#sort by date
filtered_agg1 = filtered_agg1.sort_values("trade_date")


st.header("Daily Average Close")
#filtered table
st.dataframe(filtered_agg1)
st.bar_chart(filtered_agg1.set_index("trade_date")["avg_close"])

#filter by date range and ticker for parquet agg3
filtered_agg3 = df_agg3[
    (df_agg3['trade_date'] >= pd.to_datetime(date_range[0])) &
    (df_agg3['trade_date'] <= pd.to_datetime(date_range[1])) &
    (df_agg3['ticker'] == ticker)
]

st.header("Daily Return")
#Filtered table
st.dataframe(filtered_agg3)
st.line_chart(filtered_agg3.set_index("trade_date")["avg_daily_return"])
    
