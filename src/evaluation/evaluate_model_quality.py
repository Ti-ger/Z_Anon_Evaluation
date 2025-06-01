import sys

import pandas as pd
from datetime import timedelta as td
from tqdm import tqdm
from pm4py import discover_petri_net_inductive, fitness_alignments, precision_alignments, generalization_tbr, \
    simplicity_petri_net, convert_to_event_log
from constants import multiprocessing, case_id, file_name, res_path, write_middle_results as m_res, \
    abstract_timestamps
from re_identification_risk.utils import quantifyer
from re_identification_risk.abstraction_timestamp import run_abstraction


def evaluate_log_for_risk(o_log: pd.DataFrame, z_values: list[int], dt_values: list[td], filter,
                          relative_points: list[float], abs_points, projection: list = []):
    result = []
    # 1. filter all logs up front in order to make things faster afterwards
    for z in tqdm(z_values, desc="RISK: z-values"):
        for t in tqdm(dt_values, desc="RISK: Delta t -thresholds"):
            # 1. filter event log with function pointer filter
            filtered_df = filter(o_log.copy(), t, z)

            if filtered_df.empty:
                continue

            if abstract_timestamps:
                filtered_df = run_abstraction(eventLog=filtered_df)
            # 2. convert to event log (just to make sure)
            # event_log = convert_to_event_log(filtered_df)

            result.append(
                quantifyer(filtered_df, z, t, p_relative=relative_points, p_absolute=abs_points, projection=projection))

            if m_res:
                name = file_name.removesuffix('.xes')
                pd.DataFrame(result).to_csv(f"{res_path}/Risk{name}.csv")

    return pd.DataFrame(result)


def evaluate_log_for_model_quality(o_log: pd.DataFrame, z_values: list[int], dt_values: list[td], filter):
    # results dataframe
    results = []
    for z in tqdm(z_values, desc="Model Quality: Z- Thresholds"):
        for t in tqdm(dt_values, desc="Model Quality: Delta t - thresholds"):

            # 1. filter event log with function pointer filter
            filtered_df = filter(o_log.copy(), t, z)

            if filtered_df.empty:
                continue

            if abstract_timestamps:
                filtered_df = run_abstraction(eventLog=filtered_df)
            # 2. convert to event log (just to make sure)
            event_log = convert_to_event_log(filtered_df)

            # 3. mine modell

            try:
                net, im, fm = discover_petri_net_inductive(log=event_log, multi_processing=multiprocessing)
            except:
                print("Somethig went wrong while minng")
                continue;

            # 4. evaluate

            fitness_ref = fitness_alignments(o_log, net, im, fm, multi_processing=multiprocessing)
            precision_ref = precision_alignments(o_log, net, im, fm, multi_processing=multiprocessing)
            generality_ref = generalization_tbr(o_log, net, im, fm)
            simplicity = simplicity_petri_net(net, im, fm)

            results.append({
                'z': z,
                'delta_t': td.total_seconds(t),
                'fitness': fitness_ref,
                'precision': precision_ref,
                'generality': generality_ref,
                'simplicity': simplicity,
                'num_traces': len(filtered_df[case_id].unique()),
                'num events': len(filtered_df)
            })

            # write results if wanted
            if m_res:
                name = file_name.removesuffix('.xes')
                pd.DataFrame(results).to_csv(f"{res_path}/Quality{name}.csv")

    return pd.DataFrame(results)


if __name__ == "__main__":
    sys.setrecursionlimit(5000)
    from zfilters.prepare_data import read_data_and_check_for_invalid_sources
    from zfilters.filtering_balanced_z import apply_filter_wrapper

    event_log = read_data_and_check_for_invalid_sources()
    df = evaluate_log_for_risk(event_log, [10], [td(hours=72)], apply_filter_wrapper, [0.3], [], ['A', 'E'])

    df.to_csv("test.csv")

    # df = evaluate_log_for_parameters(event_log, [15, 30, 25 ], [td(hours=72), td(hours=1), td(days=7)], apply_filter_wrapper)
    # df.to_csv("test.csv")
