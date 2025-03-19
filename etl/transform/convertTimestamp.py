from pyspark.sql.functions import to_timestamp

def convert_timestamp(df, CONFIG):
    return df.withColumn('timestamp', to_timestamp('timestamp', CONFIG['transform']['timestamp_format']))