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



def fetch_data(spark, indicators, countries, start_year, end_year):
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

    
    if indicator_df.shape[0] != 0:
        spark_indicator_df = spark.createDataFrame(indicator_df)
        print(f'\n\n{spark_indicator_df.show(2)}\n\n')
        spark_indicator_df.write.csv('/home/mike/random/spark_indicator_df')




def main():
    if len(sys.argv) < 5:
        raise Exception('Not enough arguement for spark job!')
    
    # '[{"indicator_id":"SL.UEM.TOTL.NE.ZS","indicator_name":"Unemployment"},{"indicator_id":"NY.GDP.MKTP.CD","indicator_name":"GDP"}]'
    print(f'\n\n{sys.argv[1]}\n\n')
    
    indicators = json.loads(sys.argv[1])
    countries = json.loads(sys.argv[2])
    start_year = int(sys.argv[3])
    end_year = int(sys.argv[4])

    spark = get_spark_session()
    
    fetch_data(spark, indicators, countries, start_year, end_year)



    spark.stop()




if __name__ == "__main__":
    main()











    