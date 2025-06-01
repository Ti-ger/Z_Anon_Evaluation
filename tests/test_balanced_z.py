import unittest
from prepare_data import read_data_and_check_for_invalid_sources as reader
from datetime import timedelta as td
import pandas as pd
from filtering_balanced_z import process_log_sequentially as process
from datetime import timedelta as t

# global variables for tests to not interfer with user changes
case_id = "case:concept:name"
activity = "concept:name"
source = "org:group"
timestamp = "time:timestamp"

def evaluate(data, expected_data, z, delta_t):
    df = pd.DataFrame(data)
    expected = pd.DataFrame(expected_data).sort_values(by=[case_id, timestamp])
    expected[timestamp] = pd.to_datetime(expected[timestamp])
    df[timestamp] = pd.to_datetime(df[timestamp])

    df = pd.DataFrame(process(df, z=z, time_delta=delta_t)).sort_values(
        by=[case_id, timestamp]).reset_index(drop=True)

    print(df)
    if expected.empty:
        assert (df.empty)
    else:
        assert (df.equals(expected))


class TestZBalanced(unittest.TestCase):

    def test_filtered_log_stays_equal(self):
        log = reader()
        log = log.sort_values(by=[case_id, timestamp], kind="mergesort").reset_index(drop=True)
        df = pd.DataFrame(process(df=log.copy(), time_delta=td(hours=72), z=1))
        df_sorted = df.sort_values(
            by=[case_id, timestamp], kind="mergesort"
        ).reset_index(drop=True)

        assert len(log) == len(df_sorted)

        assert (log.equals(df_sorted))

    def test_does_z_threshold_apply1(self):
        sample_data = {
            case_id: [1, 2, 3, 4, 5],
            activity: ["A", "A", "A", "A", "A"],
            timestamp: ["2024-11-1", "2024-11-1", "2024-11-1", "2024-11-1", "2024-11-1"],
            source: ["A", "A", "A", "A", "A"]
        }

        expected = {
            case_id: [1, 2, 3, 4, 5],
            activity: ["A", "A", "A", "A", "A"],
            timestamp: ["2024-11-1", "2024-11-1", "2024-11-1", "2024-11-1", "2024-11-1"],
            source: ["A", "A", "A", "A", "A"]
        }

        evaluate(sample_data, expected, z=3, delta_t=td(hours=72))

    def test_does_z_threshold_apply2(self):
        sample_data = {
            case_id: [1, 2, 3, 4, 5],
            activity: ["A", "A", "A", "A", "A"],
            timestamp: ["2024-11-1", "2024-11-4", "2024-11-4", "2024-11-4", "2024-11-7"],
            source: ["A", "A", "A", "A", "A"]
        }

        expected = {
            case_id: [1, 2, 3, 4, 5],
            activity: ["A", "A", "A", "A", "A"],
            timestamp: ["2024-11-1", "2024-11-4", "2024-11-4", "2024-11-4", "2024-11-7"],
            source: ["A", "A", "A", "A", "A"]
        }

        evaluate(sample_data, expected, z=3, delta_t=td(hours=72))

    def test_does_z_threshold_apply3(self):
        sample_data = {
            case_id: [1, 2, 3, 4, 5],
            activity: ["A", "A", "A", "A", "A"],
            timestamp: ["2024-11-1", "2024-11-6", "2024-11-10", "2024-11-10", "2024-11-15"],
            source: ["A", "A", "A", "A", "A"]
        }

        expected = {
            case_id: [2, 3, 4],
            activity: ["A", "A", "A"],
            timestamp: [ "2024-11-6", "2024-11-10", "2024-11-10"],
            source: ["A", "A", "A"]
        }

        evaluate(sample_data, expected, z=3, delta_t=td(days=4))
    def test_does_delta_t_threshold_apply(self):
        sample_data = {
            case_id: [1, 2, 3, 4, 5],
            activity: ["A", "A", "A", "A", "A"],
            timestamp: ["2024-11-1", "2024-11-3", "2024-11-5", "2024-11-5", "2024-11-6"],
            source: ["A", "A", "A", "A", "A"]
        }

        expected = {
            case_id: [2, 3, 4, 5],
            activity: ["A", "A", "A", "A"],
            timestamp: ["2024-11-3", "2024-11-5","2024-11-5", "2024-11-6"],
            source: ["A", "A", "A", "A"]
        }

        evaluate(sample_data, expected, z=3, delta_t=td(hours=72))

    def test_different_sources_no_output(self):
        sample_data = {
            case_id: [1, 2, 3, 4, 5],
            activity: ["A", "A", "A", "A", "A"],
            timestamp: ["2024-11-1", "2024-11-3", "2024-11-5", "2024-11-5", "2024-11-6"],
            source: ["A", "B", "C", "D", "E"]
        }

        expected = {
            case_id: [],
            activity: [],
            timestamp: [],
            source: []
        }

        evaluate(sample_data, expected, z=2, delta_t=td(days=28))

    def test_different_activities_no_output(self):
        sample_data = {
            case_id: [1, 2, 3, 4, 5],
            activity: ["A", "B", "C", "D", "E"],
            timestamp: ["2024-11-1", "2024-11-3", "2024-11-5", "2024-11-5", "2024-11-6"],
            source: ["A", "A", "A", "A", "A"]
        }

        expected = {
            case_id: [],
            activity: [],
            timestamp: [],
            source: []
        }

        evaluate(sample_data, expected, z=2, delta_t=td(days=28))



    def test_z_and_t_combined(self):
        sample_data = {
            case_id: [1, 2, 3, 4, 5],
            activity: ["A", "A", "A", "A", "A"],
            timestamp: ["2024-10-1", "2024-11-2", "2024-11-6", "2024-11-7", "2024-12-1"],
            source: ["A", "A", "A", "A","A"]
        }

        expected = {
            case_id: [2, 3, 4],
            activity: ["A", "A", "A"],
            timestamp: ["2024-11-2", "2024-11-6", "2024-11-7"],
            source: ["A", "A", "A"]
        }

        evaluate(sample_data, expected, z=2, delta_t=td(days=4))


    def test_mixed_up_timestamps(self):
        sample_data = {
            case_id: [1, 1, 2, 1, 2],
            activity: ["A", "A", "A", "A", "A"],
            timestamp: ["2024-11-1", "2024-10-30", "2024-12-3", "2024-11-7", "2024-12-1"],
            source: ["A", "A", "A", "A","A"]
        }

        expected = {
            case_id: [1, 1, 1, 2, 2],
            activity: ["A", "A", "A", "A", "A"],
            timestamp: ["2024-10-30", "2024-11-1", "2024-11-7", "2024-12-1", "2024-12-3"],
            source: ["A", "A", "A", "A", "A"]
        }

        evaluate(sample_data, expected, z=2, delta_t=td(days=100))

    def test_empty_input_empty_output(self):
        sample_data = {
                case_id: [],
                activity: [],
                timestamp: [],
                source: []
            }

        expected = {
                case_id: [],
                activity: [],
                timestamp: [],
                source: []
            }

        evaluate(sample_data, expected, z=5, delta_t=td(days=100))