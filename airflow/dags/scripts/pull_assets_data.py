from pyspark.sql import SparkSession
import requests
import configparser


config = configparser.ConfigParser()
config.read_file(open('pipeline.cfg'))


def get_spark_session():
    spark = SparkSession.getActiveSession()
    return spark


def download_data(base_url, endpoint, apikey_append):
    response = requests.get(base_url + endpoint + apikey_append)

    if response.status_code != 200:
        raise Exception(f'Request error:\n{response.text}')
    else:
        return response.json()



def parse_date(data):
    # Asset types 
    stock = "Common Stock"
    crypto = "Digital Currency"


    df = spark.createDataFrame(data)

    df.createTempView('crypto_data')




def main():
    spark = get_spark_session()

    _12data_apikey = config.get('TWELVE_DATA', 'API_KEY')

    base_url = "https://api.twelvedata.com/"
    endpoint = f"time_series?start_date={start_date}&end_date={end_date}&symbol={symbols}&interval={interval}"
    apikey_append = f"&apikey={_12data_apikey}"
    
    data = download_data(base_url, endpoint, apikey_append)
    parse_data(data)



if __name__ == "__main__":
    main()











    