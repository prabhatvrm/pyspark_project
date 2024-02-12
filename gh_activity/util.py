import findspark

findspark.init()

from pyspark.sql import SparkSession


def get_spark_session(env):
    global spark
    if env == 'DEV':
        spark = SparkSession. \
            builder. \
            master("local"). \
            appName("GitHub Activity-DEV"). \
            getOrCreate()
    elif env == 'PROD':
        spark = SparkSession. \
            builder. \
            master("yarn"). \
            appName("GitHub Activity-PROD"). \
            getOrCreate()

    return spark


# spark session test
#env='DEV'
#spark = get_spark_session(env)
#print(spark)

