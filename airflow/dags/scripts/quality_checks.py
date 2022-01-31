from pyspark.sql import SparkSession

import json
import sys
import os

def get_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def check_unique_crypto(spark, data_lake_location):
    base_path = 'lake/crypto/'
    meta_path = 'meta'

    dup_data = None
    try:
        crypto_meta = spark.read.parquet(data_lake_location + base_path + meta_path)
        if crypto_meta.count() > 0:
            dup_data = crypto_meta.groupBy(['symbol', 'currency_base', 'currency_quote']).count().filter('count > 1')
    except:
        raise Exception('No crypto data available')


    if dup_data == None:
        raise Exception('No crypto data available')
    else:
        if dup_data.count() > 0:
            raise Exception(f'Check failed, duplicate data exist!\n{dup_data.show()}')
        else: 
            print('Check passed!!!')
        

def check_unique_stock(spark, data_lake_location):
    base_path = 'lake/stock/'
    meta_path = 'meta'

    dup_data = None
    try:
        stock_meta = spark.read.parquet(data_lake_location + base_path + meta_path)
        if stock_meta.count() > 0:
            dup_data = stock_meta.groupBy('symbol').count().filter('count > 1')
    except:
        print('No stock data available')
        

    if dup_data == None:
        print('No stock data available')
    else:
        if dup_data.count() > 0:
            raise Exception(f'Check failed, duplicate data exist!\n{dup_data.show()}')
        else: 
            print('Check passed!!!')

    
def check_unique_country_and_indicator(spark, data_lake_location):
    base_path = 'lake/economics/'
    indicator_path = 'indicator'
    country_path = 'country'

    dup_data = None
    try:
        indicators = spark.read.parquet(data_lake_location + base_path + indicator_path)
        if indicators.count() > 0:
            dup_data = indicators.groupBy('symbol').count().filter('count > 1')
    except:
        print('No indicator data available')
        

    if dup_data == None:
        print('No indicator data available')
    else:
        if dup_data.count() > 0:
            raise Exception(f'Check failed, duplicate data exist!\n{dup_data.show()}')
        else: 
            print('Check passed!!!')


    dup_data = None
    try:
        countries = spark.read.parquet(data_lake_location + base_path + country_path)
        if countries.count() > 0:
            dup_data = countries.groupBy('country').count().filter('count > 1')
    except:
        print('No country data available')
        

    if dup_data == None:
        print('No country data available')
    else:
        if dup_data.count() > 0:
            raise Exception(f'Check failed, duplicate data exist!\n{dup_data.show()}')
        else: 
            print('Check passed!!!')

    
def check_for_null_in_econs_values(spark, data_lake_location):
    base_path = 'lake/economics/'
    data_path = 'data'

    try:
        data = spark.read.parquet(data_lake_location + base_path + data_path)

        init_count = data.count()
        data = data.dropna(how='any')
        final_count = data.count()

    except:
        init_count = 0
        final_count = 0
        print('No country data available')
    
    if init_count == final_count:
        print('Check passed!!!')
    else:
        raise Exception('Check failed, data contains null values')


def main():
    if len(sys.argv) < 2:
        raise Exception('Not enough arguement for spark job!')

    script_args = json.loads(sys.argv[1])

    aws_access_key_id = script_args['aws_access_key_id']
    aws_secret_access_key = script_args['aws_secret_access_key']
    data_lake_location = script_args['data_lake_location']

    os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key

    spark = get_spark_session()

    check_unique_crypto(spark, data_lake_location)
    check_unique_stock(spark, data_lake_location)
    check_unique_country_and_indicator(spark, data_lake_location)
    check_for_null_in_econs_values(spark, data_lake_location)

    spark.stop()



if __name__ == "__main__":
    main()


 


    