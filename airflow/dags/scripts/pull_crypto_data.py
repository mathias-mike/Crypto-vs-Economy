from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, max as s_max
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


def download_data(base_url, endpoint, headers, symbols):
    response = {}

    for symbol in symbols:
        query_url = base_url + endpoint.replace('<to_be_replaced>', symbol)
        res = requests.get(query_url, headers=headers)

        if res.status_code != 200:
            raise Exception(f'Request error:\n{response}')
        
        response[symbol] = res.json()

    return response


def parse_data(data, spark):
    meta_data = None
    main_data = None

    for symbol in data:
        values = data[symbol]

        if len(values) > 0:
            split_symbol = symbol.split('_')

            meta = {}
            meta['symbol'] = symbol
            meta['currency_base'] = split_symbol[2]
            meta['currency_quote'] = split_symbol[3]

            meta_spark = spark.createDataFrame([meta])
            if meta_data == None:
                meta_data = meta_spark
            else:
                meta_data = meta_data.union(meta_spark)


            values_spark = spark.createDataFrame(values)
            values_spark = values_spark.withColumn('symbol', lit(symbol))
            if main_data == None:
                main_data = values_spark
            else:
                main_data = main_data.union(values_spark)
            

    return meta_data, main_data


def etl(spark, meta_data, main_data, output_bucket):
    base_path = 'lake/crypto/'
    meta_path = 'meta'
    data_path = 'data'

    if meta_data != None and main_data != None:
        meta_schema = T.StructType([
            T.StructField("id", T.IntegerType(), False),
            T.StructField("symbol", T.StringType(), False),
            T.StructField("currency_base", T.StringType(), False),
            T.StructField("currency_quote", T.StringType(), False)
        ])

        try:
            prev_meta = spark.read.parquet(output_bucket + base_path + meta_path)
            if prev_meta.count() > 0:
                max_id = prev_meta.select(s_max(prev_meta.id)).collect()[0][0]
                new_meta = meta_data.join(prev_meta, on=['symbol'], how='left') \
                                    .select(meta_data.symbol, meta_data.currency_base, meta_data.currency_quote) \
                                    .where(prev_meta.symbol.isNull())

                if new_meta.count() > 0:
                    meta_df = new_meta.toPandas()
                    meta_df.reset_index(inplace=True)
                    meta_df['id'] = meta_df['index'] + max_id + 1

                    new_meta = spark.createDataFrame(meta_df[['id', 'symbol', 'currency_base', 'currency_quote']], schema=meta_schema)
                    new_meta.write \
                        .format('parquet') \
                        .save(output_bucket + base_path + meta_path, mode='append')

                    meta_data = prev_meta.union(new_meta)
                else:
                    meta_data = prev_meta

            else: raise Exception('Take me to: except')
        except:
            meta_df = meta_data.toPandas()
            meta_df.reset_index(inplace=True)
            meta_df.rename(columns={'index':'id'}, inplace=True)

            meta_data = spark.createDataFrame(meta_df[['id', 'symbol', 'currency_base', 'currency_quote']], schema=meta_schema)
            meta_data.write \
                .format('parquet') \
                .save(output_bucket + base_path + meta_path, mode='overwrite')
        


        # Main data
        join_cols = [meta_data.symbol == main_data.symbol]
        main_data = main_data.join(meta_data, on=join_cols, how='inner')
        main_data = main_data.select(
                        main_data.time_period_start.cast(T.TimestampType()).alias('start_time'),
                        main_data.price_open.cast(T.DoubleType()).alias('open'),
                        main_data.price_high.cast(T.DoubleType()).alias('high'),
                        main_data.price_low.cast(T.DoubleType()).alias('low'),
                        main_data.price_close.cast(T.DoubleType()).alias('close'),
                        main_data.volume_traded.cast(T.DoubleType()).alias('volume'),
                        main_data.trades_count.cast(T.IntegerType()),
                        meta_data.id.alias('crypto_id'),
                    )

        main_data = main_data.withColumn('year', year(main_data.start_time)) \
                                .withColumn('month', month(main_data.start_time)) \
                                .withColumn('dayofweek', dayofweek(main_data.start_time)) \
                                .withColumn('dayofmonth', dayofmonth(main_data.start_time)) \
                                .withColumn('dayofyear', dayofyear(main_data.start_time))

        
        main_data.write \
                .format('parquet') \
                .save(output_bucket + base_path + data_path, 
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

    coinapi_api_key = script_args['coinapi_api_key']
    start_date = script_args['start_date']
    end_date = script_args['end_date']
    symbols = script_args['symbols']
    period = script_args['period']
    output_bucket = script_args['output_bucket']


    spark = get_spark_session()

    base_url = "https://rest.coinapi.io/v1/"
    endpoint = f"ohlcv/<to_be_replaced>/history?period_id={period}&time_start={start_date}&time_end={end_date}"
    headers = {'X-CoinAPI-Key': coinapi_api_key}
    
    data = download_data(base_url, endpoint, headers, symbols)
    meta_data, main_data = parse_data(data, spark)

    etl(spark, meta_data, main_data, output_bucket)

    spark.stop()



if __name__ == "__main__":
    main()


 


    