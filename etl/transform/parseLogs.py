from pyspark.sql.functions import regexp_extract

def parse_logs(df, CONFIG):
    regex = CONFIG['input']['log_regex']
    return df.select( 
        regexp_extract( 'value', regex, 1).alias( "ip"), 
        regexp_extract( 'value', regex, 2).alias( 'timestamp'),
        regexp_extract( 'value', regex, 3).alias( 'method'),
        regexp_extract( 'value', regex, 4).alias( 'endpoint'),
        regexp_extract( 'value', regex, 5).alias( 'protocol'),
        regexp_extract( 'value', regex, 6).alias( 'status_code').cast('integer'),
        regexp_extract( 'value', regex, 7).alias( 'bytes').cast('integer'),
        'source'
    )