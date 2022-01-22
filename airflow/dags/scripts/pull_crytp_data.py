from pyspark.sql import SparkSession
import requests
import json
import configparser
import os


config = configparser.ConfigParser()
config.read_file(open('pipeline.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'SECRET_ACCESS_KEY')


def get_spark_session():
    spark = SparkSession.getActiveSession()
    return spark


def download_data(base_url, endpoint, headers):
    response = requests.get(base_url + endpoint, headers=headers)

    return json.loads(response.text) # TODO: Add response checks 


def parse_data(date):
    pass

def main():
    spark = get_spark_session()

    base_url = "https://rest.coinapi.io/v1/"
    coinapi_key = config.get('COINAPI', 'COINAPI_KEY')
    headers = {'X-CoinAPI-Key', coinapi_key}
    endpoint = 'OHLCV'

    data = download_data(base_url, endpoint, headers)




if __name__ == "__main__":
    main()