from pyspark.sql.functions import window, desc, col, count, avg

def calculate_aggregates(df):
    peak_traffic = df.groupBy(window('timestamp', '1 hour').alias('time_window')).count().orderBy(desc('count'))
    error_analysis = df.filter(col('status_code') >= 400).groupBy("status_code","endpoint_type")\
                        .agg(count("*").alias("error_count"), avg('bytes').alias("avg_bytes"))    
    return peak_traffic, error_analysis