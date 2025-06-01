import pandas as pd
import sys
from datetime import timedelta as td

import pm4py
from pm4py import read_xes

from constants import source_path, activity, timestamp, source, case_id, req_cols
from filtering_basic_z import process_sublog as basic_filtering
from filtering_balanced_z import process_sublog as balanced_filtering
from concurrent.futures import ThreadPoolExecutor


# define columns required for analysis


def read_data_and_check_for_invalid_sources():
    # import XES event log
    orig_log = read_xes(source_path)
    # take columns required
    orig_log = orig_log[req_cols]

    # make sure to interpret timestamp columns as timestamps
    orig_log[timestamp] = pd.to_datetime(orig_log[timestamp])

    # detect whether a source entry is invalid
    invalid_mask = orig_log[source].isna() | (orig_log[source].astype(str).str.strip() == "")
    orig_log.loc[invalid_mask, source] = orig_log.loc[invalid_mask, activity]

    return orig_log


# group according to source and
# grouped_sorted = orig_log.sort_values(by=[source, timestamp])


# group by source attributes
# grouped_sorted = grouped_sorted.groupby(source)


def process_all_sublogs(event_log_df, time_delta: td = td(hours=72), z=3, balanced: bool = True):
    grouped = event_log_df.groupby(source)
    results = []

    with ThreadPoolExecutor() as executor:

        # add one thread for every group to thread pool
        if balanced:
            futures = {
                executor.submit(balanced_filtering, name, group.copy(), time_delta, z): name
                for name, group in grouped
            }
        else:
            futures = {
                executor.submit(basic_filtering, name, group.copy(), time_delta, z): name
                for name, group in grouped
            }

        for future in futures:
            result_df = future.result()
            if not result_df.empty:
                results.append(result_df)

    if results:
        return pd.concat(results, ignore_index=True)
    else:
        return pd.DataFrame(columns=req_cols)


# orig_log = read_data_and_check_for_invalid_sources()
# filtered_log = pd.DataFrame(process_all_sublogs(orig_log)).sort_values(by=[case_id, timestamp]).reset_index(drop=True)
#
# filtered_log.to_csv("test.csv")
#
# sys.setrecursionlimit(5000)
# net, im, fm = pm4py.discover_petri_net_inductive(filtered_log, multi_processing=True)
# print(pm4py.fitness_alignments(filtered_log, net, im, fm, multi_processing=True))
# print(pm4py.precision_alignments(filtered_log, net, im, fm, multi_processing=True))
# print(pm4py.generalization_tbr(filtered_log, net, im, fm))
# print(pm4py.simplicity_petri_net(net, im, fm))
