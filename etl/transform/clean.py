from pyspark.sql.functions import col
def clean_data(df):
    return df.na.drop(how = 'any').filter((col('status_code') >= 100) & (col('status_code') < 600) & (col('method').isin([ 'GET', 'POST', 'DELETE', 'PATCH'])))