# Effects of Economic indicies on Cryptocurrency
Download crypto, stock and a few other economic indicators, process and upload to a data lake on AWS S3.

## Overview
Cryptocurrency is a trend that has slowly gained grounds from it's birth with Bitcoin in 2008 to over 10,000 cryptocurrencies currently in existance and traded on different exchanges. Today, virtual financial assets and tokens are a significant part of the global financial markets with the crypto market capitalization exceeding one trillion dollars. However, there is a lot of discussion about cryptocurrency functions and their correlation with the basic economic indices. In this project, we seek to build a pipeline to periodically pulls cryptocurrency data as well as stock data and other economic indicators and store them in an S3 bucket for future analysis.

The data pipeline is built on Apache Airflow, with Apache Spark to pull the data and store them in a data lake on an S3 bucket in Parquet format.

As of the time of this writing (2022-01-30), there are over 10,000 crypto currencies currently in existence and new once springing up each day. There are over 2,000 of them that have been in existance since 2018 and with an hourly pull of over 4 years of the data with no weekend breaks (a complete 365 days data availability) gives us over 70 million data points to process at max (24 * 365 * 4 * 2000).

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
    * Installing python dependencies to be used by the spark scripts
    * Running the `pull_assets_data.py`, and
    * Running the `pull_econs_data.py`
3. The `pull_assets_data.py` pulls both cryptocurrency data from different exchanges using [coinapi.io api](https://www.coinapi.io/), also pulls stock data from [twelvedata api](https://twelvedata.com/) both on an hourly interval.\
It performs ETL on the data and saves it in S3 in a parquet format.
4. The `pull_econs_data,py` pulls data on economic indicators from the [world bank](https://data.worldbank.org/indicator/) and performs cleaning and ETL on the data and stores it in S3.

## How to run
To run the pipeline, you need to have the following setup and ready;

1. You need airflow setup to run the pipeline, [view here](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html) to get quick setup of airflow on your machine.
2. Create a `pipeline.cfg` file and place it in your `$AIRFLOW_HOME` directory.
3. You need to have an AWS account from where you proceed to create a programatic user with admin privilages. Get the user `ACCESS_KEY_ID` and `SECRET_ACCESS_KEY` and enter them in your `pipeline.cfg` file.
4. Create a bucket on S3 for your data lake, enter the bucket name in `pipeline.cfg`.
5. Next you need to get a [CoinApi.io api key](https://www.coinapi.io/). This is a great tool that grants you access to a good number of crypto assets from different exchanges with api calls. Getting an api key is easy here, just enter your email address and an api key is sent to you with some restriction on the free option.
4. Lastly, we get [Twelvedata api key](https://twelvedata.com/). This tool is great as it allows us fetch stock information at an hourly interval from various exchanges including **NASDAQ** and **NYSE**. Just create an account and you have access to this data with their free api key.

Once you have all of this, your `pipeline.cfg` file should look like this;
![pipeline config file](https://raw.githubusercontent.com/mathias-mike/Crypto-vs-Economy/master/media/pipeline_cfg.png)

With this, you can just run your `cluster_dag` and `spark_dag` from the airflow UI.






