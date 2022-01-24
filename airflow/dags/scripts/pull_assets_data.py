from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import requests
import sys

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
                    stock_meta.union(meta)

            elif asset_info['meta']['type'] == crypto:
                if crypto_meta == None:
                    crypto_meta = meta
                else:
                    crypto_meta.union(meta)
                
            else: raise Exception('Unrecorgnised asset type!')


            asset_name = udf(lambda _: asset)
            values = values.withColumn('asset', asset_name('datetime'))
            if asset_info['meta']['type'] == stock:
                if stock_asset_values == None:
                    stock_asset_values = values
                else:
                    stock_asset_values.union(values)

            elif asset_info['meta']['type'] == crypto:
                if crypto_asset_values == None:
                    crypto_asset_values = values
                else:
                    crypto_asset_values.union(values)


    if stock_meta != None and crypto_meta != None and crypto_asset_values != None:
        stock_meta.write.csv('/home/mike/random/stock_meta')
    elif crypto_meta != None:
        crypto_meta.write.csv('/home/mike/random/crypto_meta')
    elif stock_asset_values != None:
        stock_asset_values.write.csv('/home/mike/random/stock_asset_values')
    elif crypto_asset_values != None:
        crypto_asset_values.write.csv('/home/mike/random/crypto_asset_values')




def main():
    if len(sys.argv) < 6:
        raise Exception('Not enough arguement for spark job!')
    
    _12data_apikey = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    symbols = sys.argv[4]
    interval = sys.argv[5]

    spark = get_spark_session()

    base_url = "https://api.twelvedata.com/"
    endpoint = f"time_series?start_date={start_date}&end_date={end_date}&symbol={symbols}&interval={interval}"
    apikey_append = f"&apikey={_12data_apikey}"
    
    data = download_data(base_url, endpoint, apikey_append)

    parse_data(data, spark)





    # save_file(spark)


    spark.stop()




if __name__ == "__main__":
    main()











    