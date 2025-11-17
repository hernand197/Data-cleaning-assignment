## Data-cleaning-assignment

Using the stockmarket.csv file I was able to explore the raw data inside the file as well as create a file that cleans the data and saved it as cleaned.parquet. In this process I also created 3 parquet files:
* Daily avg close by ticker
  * sotres the average closing price for each stock ticker on the trade date
* Avg volume by sector
  * stores the average trading volume grouped by sector
* Simple daily return by ticker
  * stores the percentage change in price (from one day to the next) for each ticker

We then were instructed to used Streamlit to load the parquet files, add filters and visualize charts.
