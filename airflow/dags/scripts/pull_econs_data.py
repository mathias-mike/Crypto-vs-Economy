from pyspark.sql import SparkSession
from pandas_datareader import wb
import pandas as pd
import json
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
            indicator=indicator['indicator_id'], 
            country=countries, 
            start=start_year, 
            end=end_year
        )

        if data.shape[0] > 0 and data.shape[1] > 0:
            data.rename(columns={indicator['indicator_id']: 'value'}, inplace=True)
            data.reset_index(inplace=True)
            data['indicator'] = indicator['indicator_name']
            data['indicator_id'] = indicator['indicator_id']

        if indicator_df.shape[0] == 0:
            indicator_df = data
        else:
            indicator_df = indicator_df.append(data, ignore_index=True)

    
    spark_indicator_df = None
    if indicator_df.shape[0] != 0:
        spark_indicator_df = spark.createDataFrame(indicator_df)
        
    return spark_indicator_df


def etl(spark_indicator_df):
    if spark_indicator_df != None:
        spark_indicator_df.write.csv('/home/mike/random/spark_indicator_df', header=True)


def main():
    if len(sys.argv) < 5:
        raise Exception('Not enough arguement for spark job!')
    
    indicators = json.loads(sys.argv[1])
    countries = json.loads(sys.argv[2])
    start_year = int(sys.argv[3])
    end_year = int(sys.argv[4])

    spark = get_spark_session()
    
    spark_indicator_df = download_data(spark, indicators, countries, start_year, end_year)

    etl(spark_indicator_df)

    spark.stop()



if __name__ == "__main__":
    main()









# spark-submit pull_econs_data.py '[{"indicator_id":"SL.UEM.TOTL.NE.ZS","indicator_name":"Unemployment"},{"indicator_id":"NY.GDP.MKTP.CD","indicator_name":"GDP"}]' '["US","NG"]' 2005 2021

    