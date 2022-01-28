from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit, max as s_max
from pyspark.sql.functions import year, month, dayofyear, dayofweek, dayofmonth
from pyspark.sql import types as T

import requests
import numpy
import pandas
import json
import sys
import os

def get_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def download_data(base_url, endpoint, apikey_append):
    response = requests.get(base_url + endpoint + apikey_append)

    if response.status_code != 200:
        raise Exception(f'Request error:\n{response.text}')
    else:
        return response.json()



def parse_data(data, spark):
    stock = "Common Stock"
    crypto = "Digital Currency"

    stock_meta = None
    crypto_meta = None
    stock_asset_values = None
    crypto_asset_values = None

    for asset in data:
        asset_info = data[asset]
        if asset_info['status'] == 'ok':
            meta = spark.createDataFrame([asset_info['meta']])
            values = spark.createDataFrame(asset_info['values'])

            if asset_info['meta']['type'] == stock:
                if stock_meta == None:
                    stock_meta = meta
                else:
                    stock_meta = stock_meta.union(meta)

            elif asset_info['meta']['type'] == crypto:
                if crypto_meta == None:
                    crypto_meta = meta
                else:
                    crypto_meta = crypto_meta.union(meta)
                
                
            else: raise Exception('Unrecorgnised asset type!')

            values = values.withColumn('asset', lit(asset))
            if asset_info['meta']['type'] == stock:
                if stock_asset_values == None:
                    stock_asset_values = values
                else:
                    stock_asset_values = stock_asset_values.union(values)

            elif asset_info['meta']['type'] == crypto:
                if crypto_asset_values == None:
                    crypto_asset_values = values
                else:
                    crypto_asset_values = crypto_asset_values.union(values)
                

    return stock_meta, crypto_meta, stock_asset_values, crypto_asset_values



def etl_stock(spark, stock_meta, stock_asset_values, companies, output_bucket, stock_path):
    meta_path = 'meta'
    data_path = 'data'

    if stock_meta != None and stock_asset_values != None:
        get_company = udf(lambda symbol: companies[symbol])
        stock_meta = stock_meta.withColumn('company', get_company('symbol'))

        meta_schema = T.StructType([
            T.StructField("id", T.IntegerType(), False),
            T.StructField("symbol", T.StringType(), False),
            T.StructField("company", T.StringType(), False),
            T.StructField("currency", T.StringType(), False)
        ])

        try:
            prev_meta = spark.read.parquet(output_bucket + stock_path + meta_path)
            if prev_meta.count() > 0:
                max_id = prev_meta.select(s_max(prev_meta.id)).collect()[0][0] + 1

                new_stock = stock_meta.join(prev_meta, on=['symbol'], how='left') \
                                    .select(stock_meta.symbol, stock_meta.company, stock_meta.currency) \
                                    .where(prev_meta.symbol.isNull())

                if new_stock.count() > 0:
                    stock_meta_df = new_stock.toPandas()
                    stock_meta_df.reset_index(inplace=True)
                    stock_meta_df['id'] = stock_meta_df['index'] + max_id
                    new_stock = spark.createDataFrame(stock_meta_df[['id', 'symbol', 'company', 'currency']], schema=meta_schema)

                    new_stock.write \
                        .format('parquet') \
                        .save(output_bucket + stock_path + meta_path, mode='append')

                    stock_meta = prev_meta.union(new_stock)
                else:
                    stock_meta = prev_meta

            else: raise Exception('Take me to: except')
        except:
            stock_meta_df = stock_meta.toPandas()
            stock_meta_df.reset_index(inplace=True)
            stock_meta_df.rename(columns={'index':'id'}, inplace=True)
            stock_meta = spark.createDataFrame(stock_meta_df[['id', 'symbol', 'company', 'currency']], schema=meta_schema)

            
            stock_meta.write \
                .format('parquet') \
                .save(output_bucket + stock_path + meta_path, mode='overwrite')
        


        # Stock data
        join_cols = [stock_asset_values.asset == stock_meta.symbol]
        stock_asset_values = stock_asset_values.join(stock_meta, on=join_cols, how='inner')
        stock_asset_values = stock_asset_values.select(
                                            stock_asset_values.datetime.cast(T.TimestampType()),
                                            stock_asset_values.open.cast(T.DoubleType()),
                                            stock_asset_values.high.cast(T.DoubleType()),
                                            stock_asset_values.low.cast(T.DoubleType()),
                                            stock_asset_values.close.cast(T.DoubleType()),
                                            stock_asset_values.volume.cast(T.IntegerType()),
                                            stock_meta.id,
                                        )

        stock_asset_values = stock_asset_values.withColumn('year', year(stock_asset_values.datetime)) \
                                            .withColumn('month', month(stock_asset_values.datetime)) \
                                            .withColumn('dayofweek', dayofweek(stock_asset_values.datetime)) \
                                            .withColumn('dayofmonth', dayofmonth(stock_asset_values.datetime)) \
                                            .withColumn('dayofyear', dayofyear(stock_asset_values.datetime))

        
        stock_asset_values.write \
                .format('parquet') \
                .save(output_bucket + stock_path + data_path, 
                        mode='append',
                        partitionBy=['year', 'month'])



def etl_crypto(spark, crypto_meta, crypto_asset_values, output_bucket, crypto_path):
    meta_path = 'meta'
    data_path = 'data'

    if crypto_meta != None and crypto_asset_values != None:
        meta_schema = T.StructType([
            T.StructField("id", T.IntegerType(), False),
            T.StructField("symbol", T.StringType(), False),
            T.StructField("currency_base", T.StringType(), False),
            T.StructField("currency_quote", T.StringType(), False)
        ])

        try:
            prev_meta = spark.read.parquet(output_bucket + crypto_path + meta_path)
            if prev_meta.count() > 0:
                max_id = prev_meta.select(s_max(prev_meta.id)).collect()[0][0] + 1

                new_crypto = crypto_meta.join(prev_meta, on=['symbol'], how='left') \
                                    .select(crypto_meta.symbol, crypto_meta.currency_base, crypto_meta.currency_quote) \
                                    .where(prev_meta.symbol.isNull())

                if new_crypto.count() > 0:
                    crypto_meta_df = new_crypto.toPandas()
                    crypto_meta_df.reset_index(inplace=True)
                    crypto_meta_df['id'] = crypto_meta_df['index'] + max_id
                    new_crypto = spark.createDataFrame(crypto_meta_df[['id', 'symbol', 'currency_base', 'currency_quote']], schema=meta_schema)

                    new_crypto.write \
                        .format('parquet') \
                        .save(output_bucket + crypto_path + meta_path, mode='append')

                    crypto_meta = prev_meta.union(new_crypto)
                else:
                    crypto_meta = prev_meta

            else: raise Exception('Take me to: except')
        except:
            crypto_meta_df = crypto_meta.toPandas()
            crypto_meta_df.reset_index(inplace=True)
            crypto_meta_df.rename(columns={'index':'id'}, inplace=True)
            crypto_meta = spark.createDataFrame(crypto_meta_df[['id', 'symbol', 'currency_base', 'currency_quote']], schema=meta_schema)

            
            crypto_meta.write \
                .format('parquet') \
                .save(output_bucket + crypto_path + meta_path, mode='overwrite')
        


        # Stock data
        join_cols = [crypto_asset_values.asset == crypto_meta.symbol]
        crypto_asset_values = crypto_asset_values.join(crypto_meta, on=join_cols, how='inner')
        crypto_asset_values = crypto_asset_values.select(
                                            crypto_asset_values.datetime.cast(T.TimestampType()),
                                            crypto_asset_values.open.cast(T.DoubleType()),
                                            crypto_asset_values.high.cast(T.DoubleType()),
                                            crypto_asset_values.low.cast(T.DoubleType()),
                                            crypto_asset_values.close.cast(T.DoubleType()),
                                            crypto_meta.id,
                                        )

        crypto_asset_values = crypto_asset_values.withColumn('year', year(crypto_asset_values.datetime)) \
                                            .withColumn('month', month(crypto_asset_values.datetime)) \
                                            .withColumn('dayofweek', dayofweek(crypto_asset_values.datetime)) \
                                            .withColumn('dayofmonth', dayofmonth(crypto_asset_values.datetime)) \
                                            .withColumn('dayofyear', dayofyear(crypto_asset_values.datetime))

        
        crypto_asset_values.write \
                .format('parquet') \
                .save(output_bucket + crypto_path + data_path, 
                        mode='append',
                        partitionBy=['year', 'month'])




def main():
    if len(sys.argv) < 2:
        raise Exception('Not enough arguement for spark job!')

    script_args = json.loads(sys.argv[1])

    aws_access_key_id = script_args['aws_access_key_id']
    aws_secret_access_key = script_args['aws_secret_access_key']

    os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key

    _12data_apikey = script_args['_12data_apikey']
    start_date = script_args['start_date']
    end_date = script_args['end_date']
    symbols = script_args['symbols']
    companies = script_args['companies']
    interval = script_args['interval']
    output_bucket = 's3://' + script_args['output_bucket'] + '/' 


    spark = get_spark_session()

    base_url = "https://api.twelvedata.com/"
    endpoint = f"time_series?start_date={start_date}&end_date={end_date}&symbol={symbols}&interval={interval}"
    apikey_append = f"&apikey={_12data_apikey}"
    
    data = download_data(base_url, endpoint, apikey_append)

    stock_meta, crypto_meta, stock_asset_values, crypto_asset_values = parse_data(data, spark)

    stock_path = 'lake/stocks/'
    crypto_path = 'lake/cryptos/'

    etl_stock(spark, stock_meta, stock_asset_values, companies, output_bucket, stock_path)

    etl_crypto(spark, crypto_meta, crypto_asset_values, output_bucket, crypto_path)

    spark.stop()



if __name__ == "__main__":
    main()






# spark-submit pull_assets_data.py aaa bbb <twelve_data_api_key> 2022-01-21 2022-01-22 AAPL,TSLA,BTC/USD,ETH/USD '{"AAPL":"Apple","TSLA":"Tesla"}' 1h /home/mike/random/



# TODO: Read parquet as cvs, got wierd data, when I tried reading it again as parquet I got error  



    