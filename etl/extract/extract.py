from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit

def extract_raw_data(spark: SparkSession, CONFIG):
    schema = StructType([StructField('value', StringType()), StructField('source', StringType())])
    df = spark.createDataFrame([], schema)
    for source, data_paths in CONFIG['input']['raw_path'].items():
        for path in data_paths:
            temp_df = spark.read.text(path)
            df = df.union(temp_df.withColumn('source', lit(source)))    
    return df