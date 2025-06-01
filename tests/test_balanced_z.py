import unittest
from prepare_data import read_data_and_check_for_invalid_sources as reader

from filtering_basic_z import BasicZFilter, process_sublog as basic_filter, process_sequential as process
from datetime import timedelta as t

# global variables for tests to not interfer with user changes
case_id = "case:concept:name"
activity = "concept:name"
source = "org:group"
timestamp = "time:timestamp"

class TestZBalanced(unittest.TestCase):

    def test_filtered_log_stays_equal(self):
        log = reader()
        log = log.sort_values(by=[case_id, timestamp], kind="mergesort").reset_index(drop=True)
        df = pd.DataFrame(process(df=log.copy(), timedelta=td(hours=72), z=1))
        df_sorted = df.sort_values(
            by=[case_id, timestamp], kind="mergesort"
        ).reset_index(drop=True)

        assert len(log) == len(df_sorted)

        assert (log.equals(df_sorted))
