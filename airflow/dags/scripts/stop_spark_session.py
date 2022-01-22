from pyspark.sql import SparkSession

def stop_spark_session():
    spark = SparkSession.getActiveSession()

    spark.stop()


def main():
    stop_spark_session()


if __name__ == "__main__":
    main()