CONFIG = {
    "app_name": "Web_log_analysis_pipeline",
    "input": {
        "raw_path": {
            "NASAhttp": ["../data/raw/NASA_access_log_Jul95", "../data/raw/NASA_access_log_Aug95"]
        },
        "log_regex": r'^(\S+) - - \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)$'
    },
    "output": {
        "processed_path": "../data/processed/",
        "cleaned_path": "cleaned_logs.csv",
        "sessionized_path": "sessionized_logs.csv",
        "error_analysis_path": "error_analysis.csv",
        "peak_traffic_path": "peak_traffic.csv"
    },
    "sessionization": {
        "timeout_seconds": 1800,  # 30 min
        "partition_col": "ip",
        "order_col": "timestamp"
    },
    "aggregations": {
        "peak_traffic_path": "peak_traffic.parquet",
        "error_analysis_path": "error_analysis.parquet"
    },
    "transform": {
        "timestamp_format": 'dd/MMM/yyyy:HH:mm:ss Z',
        "endpoint_categories": ['images', 'shuttle', 'software', 'history', 'finance', 'icons', 'news', 'statistics', 'facts', 'news']
    }
}
