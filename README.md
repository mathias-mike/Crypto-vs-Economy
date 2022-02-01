# Effects of Economic indicies on Cryptocurrency
Download crypto, stock and a few other economic indicators, process and upload to a data lake on AWS S3.

## Overview
Cryptocurrency is a trend that has slowly gained grounds from it's birth with Bitcoin in 2008 to over 10,000 cryptocurrencies currently in existance and traded on different exchanges. Today, virtual financial assets and tokens are a significant part of the global financial markets with the crypto market capitalization exceeding one trillion dollars. However, there is a lot of discussion about cryptocurrency functions and their correlation with the basic economic indices. In this project, we seek to build a pipeline to periodically pulls cryptocurrency data as well as stock data and other economic indicators and store them in an S3 bucket for future analysis.

The data pipeline is built on Apache Airflow, with Apache Spark to pull the data and store them in a data lake on an S3 bucket in Parquet format.

As of the time of this writing (2022-01-30), there are over 10,000 crypto currencies currently in existence and new once springing up each day. There are over 2,000 of them that have been in existance since 2018 and with an hourly pull of over 4 years of the data with no weekend breaks (a complete 365 days data availability) gives us over 70 million data points to process at max (24 * 365 * 4 * 2000).

To demonstrate that the pipeline works, we only use a small subset of the data consisting of;
3 cryptocurrencies; 
1. BTC, 
2. ETH, and 
3. LTC.
3 stocks;
1. TSLA: Tesla, Inc.,
2. GOOGL: Alphabet Inc.,
3. AMZN: Amazon.com, Inc.,
and 4 other economic indicators;
1. Unemployment, total (% of total labor force) (national estimate)
2. GDP (current US$)
3. Official exchange rate (LCU per US$, period average)
4. Real interest rate (%)

The cryptocurrencies have data points for each day, the stock data have data points for each working day while the other economic indices are updated anually. For this reason the pipeline is built to fetch each of this data according to their respective update frequency.


## Data model
The model choosen for the project is a simple snowflake schema with just one level hierarchy.
![Data model](https://raw.githubusercontent.com/mathias-mike/Crypto-vs-Economy/master/media/data_model.png)
### Fact Table
#### crypto_data
Crypto time series data
* `crypto_id` (Integer): Value used to identify crypto assets 
* `start_time` (Timestamp): Period starting time
* `open` (Double): First trade price inside period range
* `high` (Double): Highest traded price inside period range
* `low` (Double): Lowest traded price inside period range
* `close` (Double): Last trade price inside period range
* `volume` (Double): Cumulative base amount traded inside period range
* `trades_count` (Integer): Amount of trades executed inside period range
* `year` (Integer): Year of data point. Can be aggregated and mapped to `economics_data['year']`
* `month` (Integer): Month of data point
* `dayofweek` (Integer): Day of week of data point
* `dayofmonth` (Integer): Day of month of data point
* `dayofyear` (Integer): Day of year of data point

### Dimension Table
#### crypto_meta
Detail of each crypto currency
* `id` (Integer): Unique value used to identify crypto asset mapped to `crypto_data['crypto_id']`
* `symbol` (String): Crypto symbol from CoinApi.io
* `currency_base` (String): Base currency
* `currency_quote` (String): Quote currency

#### stock_data
Stock time series data
* `stock_id` (Integer): Value used to identify stock assets 
* `start_time` (Timestamp): Period starting time
* `open` (Double): First trade price inside period range
* `high` (Double): Highest traded price inside period range
* `low` (Double): Lowest traded price inside period range
* `close` (Double): Last trade price inside period range
* `volume` (Double): Cumulative base amount traded inside period range
* `year` (Integer): Year of data point. Can be aggregated and mapped to `economics_data['year']`
* `month` (Integer): Month of data point
* `dayofweek` (Integer): Day of week of data point
* `dayofmonth` (Integer): Day of month of data point
* `dayofyear` (Integer): Day of year of data point

#### stock_meta
Detail of each stock
* `id` (Integer): Unique value used to identify stock asset mapped to `stock_data['stock_id']`
* `symbol` (String): Ticker symbol from Twelvedata
* `company` (String): Company
* `currency` (String): Currency

#### econs_indicator
Detail of each indicator
* `id` (Integer): Unique value used to identify economic indicator mapped to `econs_data['indicator_id']`
* `symbol` (String): Indicator symbol from [world bank](https://data.worldbank.org/indicator/) 
* `indicator` (String): Economic indicator name

#### country
Countries with data in our economics table
* `id` (Integer): Unique value used to identify country mapped to `econs_data['country_id']`
* `country` (String): Countries we've collected their economic data

#### economics_data
Actual economic data
* `indicator_id` (Integer): Value used to identify economic indicator
* `country_id` (Integer): Value used to identify country
* `year` (Integer): Year of data point
* `value` (Double): Value of data point


## Steps 
The pipeline consists of the following tasks;
1. `cluster_dag` this configure and launches as AWS EMR cluster then waits for the task to complete.
2. The `spark_dag` is responsible for uploading spark jobs to the already configure EMR cluster, then running those jobs aswell.
3. The `pull_crypto_data.py` pulls cryptocurrency data from different exchanges using [coinapi.io api](https://www.coinapi.io/) on an hourly interval, performs etl on the data and saves the data in parquet format following the predefined model above.
4. The `pull_stock_data.py` pulls stock data from [twelvedata api](https://twelvedata.com/) on an hourly interval. Etl is performed on downloaded data and resulting dataframe is saved in parquet format in s3.
5. The `pull_econs_data,py` pulls data on economic indicators from the [world bank](https://data.worldbank.org/indicator/) and performs cleaning and ETL on the data and stores it in S3.
6. Quality check is performed on the data lake to ensure that;
    * No duplicate cryptocurrency exist in crypto_meta,
    * No duplicate stock exist in stock_meta,
    * Countries and indticators are unique,
    * No null values exist in the econs data
7. Once all of this checks are passed, the dag run completes succesfully


## Analysis Example
As an example to demonstrate how the data can be used we analyze the relationship between btc price and tesla stock price over a period of 1 year from 	2021-02-01 (Y-m-d) to 2022-01-31 (Y-m-d).

The image below show a scatter plot of the price of btc against the price of tesla stock of the given time period;

![btc vs. tesla scatter plot](https://raw.githubusercontent.com/mathias-mike/Crypto-vs-Economy/master/media/btc_vs_tsla.png)

We can notice a slight correlation between the two prices.\
With `statsmodels.api`, we can view the `R-Squared` value and the `p-value` of the relationship.

![btc vs. tesla regression](https://raw.githubusercontent.com/mathias-mike/Crypto-vs-Economy/master/media/btc_tesla_statsmodel.png)

The `p-value` show that the price of tesla stock is statistically significant in predicting the price of btc with an alpha level of 0.05, however, the `R-squared` value shows only a slight correlation between the two.


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


## Possible cases
### 1. Data was increased by 100x
In this case, we could increase emr cluster to allow for more parrallel pull of the data.

### 2. The pipelines needs to run on a daily basis by 7 am every day
For this case we simply need to set our `schedule_interval` to `@daily`. Now if the data needs to be run at a specific time period, then we can set a Service Level Agreement (SLA) to allow us monitor our DAGs abit more thoroughly.

### 3. The database needed to be accessed by 100+ people.
We could move our data into a warehouse (Redshift with it's auto-scaling capabilities and columnar storage) which can be queried with SQL.


## Choice of technology
1. **Apache Airflow**: is an easy to setup scheduler application. Since we want to build a pipeline that would run periodically to fetch data, airflow is the best tool for the Job.
2. **Apache Spark**: is a tool in the big data ecosystem used to process large amount of data. Making use of a cluster of compute power and it's lazy evaluation design makes it the best tool to use as we have millions of data points to process.
3. **AWS EMR**: make it easy to setup a spark cluster without having to do all the work required.
4. **AWS S3**: is an easy to scale cloud storage. This provides us with enough space to store our data as well as making our data available in the cloud.





