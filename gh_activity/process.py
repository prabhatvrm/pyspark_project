from pyspark.sql.functions import *
def gh_process(df):
    df1 = df.withColumn('year', year('created_at')). \
        withColumn('month', month('created_at')). \
        withColumn('day', dayofmonth('created_at'))
    return df1