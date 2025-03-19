from pyspark.sql.functions import col, substring_index, when
from pyspark.sql import DataFrame

def categorize_endpoint(df, CONFIG):
    df = df.withColumn('endpoint_type', substring_index(substring_index(df.endpoint, '/', 2), '/', -1))
    return df.filter((df.endpoint_type.isNotNull()) & (df.endpoint_type != "")).withColumn('endpoint_type', when((col('endpoint_type').isin(CONFIG['transform']['endpoint_categories'])), col('endpoint_type')).otherwise('other'))