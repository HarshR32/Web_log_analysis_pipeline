import sys
sys.path.insert(0,"../")

from pyspark.sql import SparkSession
from config.config import CONFIG
from etl.extract.extract import extract_raw_data
from etl.transform import parseLogs, convertTimestamp, clean, aggregate, sessionize
from etl.load.load import load_data

def start_session(appname: str) -> SparkSession:
    return SparkSession.builder.appName(appname).getOrCreate()

def stop_session(spark: SparkSession) -> None:
    spark.stop()

def run_pipeline() -> None:
    spark = start_session(CONFIG['app_name'])
    raw_df = extract_raw_data(spark, CONFIG)
    parsed_df = parseLogs.parse_logs(raw_df, CONFIG)
    converted_df = convertTimestamp.convert_timestamp(parsed_df, CONFIG)
    cleaned_df = clean.clean_data(converted_df)

    sessions_df = sessionize.sessionize_data(cleaned_df, CONFIG)

    traffic_df, error_df = aggregate.calculate_aggregates(cleaned_df)

    load_data(cleaned_df, CONFIG['output']['cleaned_path'], CONFIG)
    load_data(sessions_df, CONFIG['output']['sessionized_path'], CONFIG)
    load_data(error_df, CONFIG['output']['error_analysis_path'], CONFIG)
    load_data(traffic_df, CONFIG['output']['peak_traffic_path'], CONFIG)

    stop_session(spark)

if __name__ == "__main__":
    run_pipeline()