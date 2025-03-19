from pyspark.sql.window import Window
from pyspark.sql.functions import lag, unix_timestamp, when, col, sum
    

def sessionize_data(df, CONFIG):    
    window = Window.partitionBy(CONFIG['sessionization']['partition_col']).orderBy(CONFIG['sessionization']['order_col'])
    
    return df.withColumn("prev_time", lag("timestamp").over(window)).withColumn("time_diff", unix_timestamp('timestamp') - unix_timestamp('prev_time')).withColumn("new_session", when( col("time_diff") > CONFIG['sessionization']['timeout_seconds'], 1).otherwise(0)).withColumn("session_id", sum('new_session').over(window.rowsBetween(Window.unboundedPreceding, 0))).drop("prev_time", "new_session", "time_diff")