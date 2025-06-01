import pandas as pd
from datetime import timedelta as td
import tqdm
def evaluate_log_for_parameters(df: pd.DataFrame, z_values: list[int], dt_values: list[td], filter):

    for z in tqdm(z_values, desc="Z- Thresholds"):
        for t in tqdm(dt_values, desc="Delta t - thresholds"):


            # 1. filter event log with function pointer filter
            filtered_df = filter(df.copy(), t, z)