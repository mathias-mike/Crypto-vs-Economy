# Effects of Economic indicies on Cryptocurrency
Download crypto, stock and a few other economic indicators, process and upload to a data lake on AWS S3.

## Overview
Cryptocurrency is a trend that has slowly gained grounds from it's birth with Bitcoin in 2008 to over 4000 cryptocurrencies currently in existance and traded on different exchanges. Today, virtual financial assets and tokens are a significant part of the global financial markets with the crypto market capitalization exceeding one trillion dollars. However, there is a lot of discussion about cryptocurrency functions and their correlation the basic economic indices. In this project, we seek to build a pipeline to periodically pulls cryptocurrency data as well as stock data and other economic indicators and store them in an S3 bucket for future analysis.

The data pipeline is built on Apache Airflow, with Apache Spark to pull the data and store them in a data lake on an S3 bucket in Parquet format.

As of the time of this writing (2022-01-30), there are over 10,000 crypto currencies currently in existence and new once springing up each day. There are over 2,000 of them that have been in existance since 2018 and with an hourly pull of over 4 years of the data with no weekend breaks (a complete 365 days data availability) gives us 70 million data points to process at max (24 * 365 * 4 * 2000).

To demonstrate that the pipeline works, we only use a small subset of the data consisting of 4 cryptocurrencies;
1. BTC,
2. ETH,
3. BNB,
4. LTC,

4 stocks;

1. AAPL: Apple Inc.,
2. TSLA: Tesla, Inc.,
3. GOOGL: Alphabet Inc.,
4. AMZN: Amazon.com, Inc.,

and 5 other economic indicators;

1. Unemployment, total (% of total labor force) (national estimate)
2. GDP (current US$)
3. Official exchange rate (LCU per US$, period average)
4. Real interest rate (%)
5. Population, total

The cryptocurrencies have data points for each day, the stock data have data points for each working day while the other economic indices are updated anually. For this reason the pipeline is built to fetch each of this data according to their respective update frequency.


## Analysis Example

## Steps 
The pipeline consists of the following tasks;
1. `cluster_dag` this configure and launches as AWS EMR cluster then waits for the task to complete.
2. The `spark_dag` is responsible for uploading spark jobs to the already configure EMR cluster, this jobs includes;
    * Installing python dependencies to be used in the spark scripts
    * Running the `pull_assets_data.py`, and
    * Running the `pull_econs_data.py`
3. The `pull_assets_data.py` pulls both cryptocurrency data from different exchanges using [coinapi.io free api](https://www.coinapi.io/), also pulls stock data from [twelvedata free api](https://twelvedata.com/) both at an hourly interval.\
It performs ETL on the data and saves it in S3 in a parquet format.
4. The `pull_econs_data,py` pulls data on economic indicators from the [world bank](https://data.worldbank.org/indicator/) and performs cleaning and etl on the data and stores it in S3.
