import sys
from datetime import timedelta as td
from concurrent.futures import ThreadPoolExecutor

from zfilters.prepare_data import read_data_and_check_for_invalid_sources
from evaluation.evaluate_model_quality import evaluate_log_for_model_quality as eval_quality, evaluate_log_for_risk as eval_risk
from zfilters.filtering_balanced_z import apply_filter_wrapper as balanced_filter
from constants import res_path, file_name, abstract_timestamps

# set recursion limit for pm4py algorithms
sys.setrecursionlimit(5000)


def run_eval_quality(original_df, z_values, delta_t_values):
    return eval_quality(original_df.copy(), z_values.copy(), dt_values=delta_t_values.copy(), filter=balanced_filter)


def run_eval_risk(original_df, z_values, delta_t_values, rel_risk, abs_risk, repetitions: int):
    return eval_risk(original_df, z_values, delta_t_values, filter=balanced_filter,
                     relative_points=rel_risk, abs_points=abs_risk, repetitions=repetitions, projection=['A', 'E'])


def main():
    original_df = read_data_and_check_for_invalid_sources()

    for z in range(1, 100):
        z_values = [z]
        relative_risk_ass = []  # e.g. [0.3, 0.6, 0.9]
        abs_risk_ass = [1, 3, 5, 7, 10]      # e.g. [1, 5, 8]
        delta_t_values = [td(days=3)]
        risk_assessment_repitition = 10

        with ThreadPoolExecutor() as executor:
            future_risk = executor.submit(run_eval_risk, original_df, z_values, delta_t_values,
                                          relative_risk_ass, abs_risk_ass, risk_assessment_repitition)
            future_quality = executor.submit(
                run_eval_quality, original_df, z_values, delta_t_values)

            df_quality = future_quality.result()
            df_risk = future_risk.result()

        merged = df_quality.merge(df_risk, on=['z', 'delta_t'])
        merged.to_csv(
            f"{res_path}/{file_name.removesuffix('.xes')}_balanced_{'generalized' if abstract_timestamps else ''}_z{z_values}.csv")


if __name__ == "__main__":
    main()


# 1 fram quality - 1 fram risk mit i als 3. dim
