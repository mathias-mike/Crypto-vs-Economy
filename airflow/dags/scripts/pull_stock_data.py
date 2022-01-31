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

    meta_data = None
    main_data = None

    for symbol in data:
        asset = data[symbol]

        if asset['status'] == 'ok':
            meta_spark = spark.createDataFrame([asset['meta']])
            values_spark = spark.createDataFrame(asset['values'])

            if asset['meta']['type'] == stock:
                if meta_data == None:
                    meta_data = meta_spark
                else:
                    meta_data = meta_data.union(meta_spark)
 
            else: raise Exception('Unrecorgnised asset type!')


            values_spark = values_spark.withColumn('symbol', lit(symbol))
            if main_data == None:
                main_data = values_spark
            else:
                main_data = main_data.union(values_spark)

                
    return meta_data, main_data



def etl(spark, meta_data, main_data, companies, output_bucket):
    base_path = 'lake/stock/'
    meta_path = 'meta'
    data_path = 'data'

    if meta_data != None and main_data != None:
        get_company = udf(lambda symbol: companies[symbol])
        meta_data = meta_data.withColumn('company', get_company('symbol'))

        meta_schema = T.StructType([
            T.StructField("id", T.IntegerType(), False),
            T.StructField("symbol", T.StringType(), False),
            T.StructField("company", T.StringType(), False),
            T.StructField("currency", T.StringType(), False)
        ])

        try:
            prev_meta = spark.read.parquet(output_bucket + base_path + meta_path)
            if prev_meta.count() > 0:
                max_id = prev_meta.select(s_max(prev_meta.id)).collect()[0][0]
                new_meta = meta_data.join(prev_meta, on=['symbol'], how='left') \
                                    .select(meta_data.symbol, meta_data.company, meta_data.currency) \
                                    .where(prev_meta.symbol.isNull())

                if new_meta.count() > 0:
                    meta_df = new_meta.toPandas()
                    meta_df.reset_index(inplace=True)
                    meta_df['id'] = meta_df['index'] + max_id + 1

                    new_meta = spark.createDataFrame(meta_df[['id', 'symbol', 'company', 'currency']], schema=meta_schema)
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

            meta_data = spark.createDataFrame(meta_df[['id', 'symbol', 'company', 'currency']], schema=meta_schema)
            meta_data.write \
                .format('parquet') \
                .save(output_bucket + base_path + meta_path, mode='overwrite')
        

        # Main dara
        join_cols = [meta_data.symbol == main_data.symbol]
        main_data = main_data.join(meta_data, on=join_cols, how='inner')
        main_data = main_data.select(
                        main_data.datetime.cast(T.TimestampType()).alias('start_time'),
                        main_data.open.cast(T.DoubleType()),
                        main_data.high.cast(T.DoubleType()),
                        main_data.low.cast(T.DoubleType()),
                        main_data.close.cast(T.DoubleType()),
                        main_data.volume.cast(T.IntegerType()),
                        meta_data.id.alias('stock_id'),
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

    _12data_apikey = script_args['_12data_apikey']
    start_date = script_args['start_date']
    end_date = script_args['end_date']
    symbols = script_args['symbols']
    companies = script_args['companies']
    interval = script_args['interval']
    output_bucket = script_args['output_bucket']


    spark = get_spark_session()

    base_url = "https://api.twelvedata.com/"
    endpoint = f"time_series?start_date={start_date}&end_date={end_date}&symbol={symbols}&interval={interval}"
    apikey_append = f"&apikey={_12data_apikey}"
    
    data = download_data(base_url, endpoint, apikey_append)
    meta_data, main_data = parse_data(data, spark)

    etl(spark, meta_data, main_data, companies, output_bucket)

    spark.stop()



if __name__ == "__main__":
    main()




    