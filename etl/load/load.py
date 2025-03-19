import pandas as pd
import os

def load_data(df, path: str, CONFIG) -> None:
    pd_df = df.toPandas()
    pd_df.to_csv(os.path.join(CONFIG['output']['processed_path'], path), index = False)