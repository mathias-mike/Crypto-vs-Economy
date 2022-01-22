from pyspark.sql import SparkSession

def create_spark_session():
    SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()


def main():
    create_spark_session()


if __name__ == "__main__":
    main()