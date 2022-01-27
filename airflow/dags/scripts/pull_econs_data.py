from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql.functions import max as s_max
from pandas_datareader import wb
import pandas as pd
import json
import os
import sys

def get_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark

def download_data(spark, indicators, countries, start_year, end_year):
    indicator_df = pd.DataFrame()
    for indicator in indicators:
        data = wb.download(
            indicator=indicator['symbol'], 
            country=countries, 
            start=start_year, 
            end=end_year
        )

        if data.shape[0] > 0 and data.shape[1] > 0:
            data.rename(columns={indicator['symbol']: 'value'}, inplace=True)
            data.reset_index(inplace=True)
            data['indicator'] = indicator['indicator']
            data['symbol'] = indicator['symbol']

        if indicator_df.shape[0] == 0:
            indicator_df = data
        else:
            indicator_df = indicator_df.append(data, ignore_index=True)

    
    spark_indicator_df = None
    if indicator_df.shape[0] != 0:
        spark_indicator_df = spark.createDataFrame(indicator_df)
        spark_indicator_df = spark_indicator_df.filter(spark_indicator_df.value.isNotNull()) \
                                                .where(spark_indicator_df.value != 'NaN')
        
    return spark_indicator_df


def etl_indicator(spark, econs_indicator, output_bucket, base_path):
    indicator_path = 'indicator'

    if econs_indicator != None:
        indicator_table = econs_indicator.select(econs_indicator.symbol, econs_indicator.indicator).distinct()
        indicator_schema = T.StructType([
            T.StructField("id", T.IntegerType(), False),
            T.StructField("symbol", T.StringType(), False),
            T.StructField("indicator", T.StringType(), False)
        ])

        try:
            prev_indicator = spark.read.parquet(output_bucket + base_path + indicator_path)
            if prev_indicator.count() > 0:
                max_id = prev_indicator.select(s_max(prev_indicator.id)).collect()[0][0] + 1

                new_indicator = indicator_table.join(prev_indicator, on=['symbol'], how='left') \
                                    .select(indicator_table.symbol, indicator_table.indicator) \
                                    .where(prev_indicator.symbol.isNull())

                if new_indicator.count() > 0:
                    indicator_df = new_indicator.toPandas()
                    indicator_df.reset_index(inplace=True)
                    indicator_df['id'] = indicator_df['index'] + max_id
                    new_indicator = spark.createDataFrame(indicator_df[['id', 'symbol', 'indicator']], schema=indicator_schema)

                    new_indicator.write \
                        .format('parquet') \
                        .save(output_bucket + base_path + indicator_path, mode='append')

                    indicator_table = prev_indicator.union(new_indicator)
                else:
                    indicator_table = prev_indicator

                return indicator_table

            else: raise Exception('Take me to: except')
        except:
            indicator_df = indicator_table.toPandas()
            indicator_df.reset_index(inplace=True)
            indicator_df.rename(columns={'index':'id'}, inplace=True)
            indicator_table = spark.createDataFrame(indicator_df[['id', 'symbol', 'indicator']], schema=indicator_schema)

            indicator_table.write \
                .format('parquet') \
                .save(output_bucket + base_path + indicator_path, mode='overwrite')

            return indicator_table


def etl_country(spark, econs_indicator, output_bucket, base_path):
    country_path = 'country'

    if econs_indicator != None:
        country_table = econs_indicator.select(econs_indicator.country).distinct()
        country_schema = T.StructType([
            T.StructField("id", T.IntegerType(), False),
            T.StructField("country", T.StringType(), False)
        ])

        try:
            prev_country = spark.read.parquet(output_bucket + base_path + country_path)
            if prev_country.count() > 0:
                max_id = prev_country.select(s_max(prev_country.id)).collect()[0][0] + 1

                new_country = country_table.join(prev_country, on=['country'], how='left') \
                                    .select(country_table.country) \
                                    .where(prev_country.country.isNull())

                if new_country.count() > 0:
                    country_df = new_country.toPandas()
                    country_df.reset_index(inplace=True)
                    country_df['id'] = country_df['index'] + max_id
                    new_country = spark.createDataFrame(country_df[['id', 'country']], schema=country_schema)

                    new_country.write \
                        .format('parquet') \
                        .save(output_bucket + base_path + country_path, mode='append')

                    country_table = prev_country.union(new_country)
                else:
                    country_table = prev_country

                return country_table

            else: raise Exception('Take me to: except')
        except:
            country_df = country_table.toPandas()
            country_df.reset_index(inplace=True)
            country_df.rename(columns={'index':'id'}, inplace=True)
            country_table = spark.createDataFrame(country_df[['id', 'country']], schema=country_schema)

            country_table.write \
                .format('parquet') \
                .save(output_bucket + base_path + country_path, mode='overwrite')

            return country_table



def etl_econs_data(econs_indicator, output_bucket, base_path, indicator_table, country_table):
    econs_data_path = 'econs_data'

    if econs_indicator != None:
        econs_data_table = econs_indicator.select(econs_indicator.symbol, econs_indicator.country, econs_indicator.year, econs_indicator.value)

        econs_data_table = econs_data_table.join(indicator_table, on=[indicator_table.symbol == econs_data_table.symbol], how='inner')
        econs_data_table = econs_data_table.join(country_table, on=[country_table.country == econs_data_table.country], how='inner')
        econs_data_table = econs_data_table.select(
                                            indicator_table.id.alias('indicator_id').cast(T.IntegerType()),
                                            country_table.id.alias('country_id').cast(T.IntegerType()),
                                            econs_data_table.year.cast(T.IntegerType()),
                                            econs_data_table.value.cast(T.DoubleType())
                                        )

        econs_data_table.write \
                .format('parquet') \
                .save(output_bucket + base_path + econs_data_path, mode='append')

        
def main():
    if len(sys.argv) < 8:
        raise Exception('Not enough arguement for spark job!')

    aws_access_key_id = sys.argv[1]
    aws_secret_access_key = sys.argv[2]

    os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key
    
    indicators = json.loads(sys.argv[3])
    countries = json.loads(sys.argv[4])
    start_year = int(sys.argv[5])
    end_year = int(sys.argv[6])
    output_bucket = sys.argv[7] # Just the bucket s3 url

    spark = get_spark_session()
    
    econs_indicator = download_data(spark, indicators, countries, start_year, end_year)

    base_path = 'crypto_vs_econs/economics/'

    indicator_table = etl_indicator(spark, econs_indicator, output_bucket, base_path)

    country_table = etl_country(spark, econs_indicator, output_bucket, base_path)

    etl_econs_data(econs_indicator, output_bucket, base_path, indicator_table, country_table)

    spark.stop()


if __name__ == "__main__":
    main()









# spark-submit pull_econs_data.py '[{"symbol":"SL.UEM.TOTL.NE.ZS","indicator":"Unemployment"},{"symbol":"NY.GDP.MKTP.CD","indicator":"GDP"}]' '["US","NG"]' 2005 2021

    