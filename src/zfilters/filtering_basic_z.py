from datetime import timedelta as td
import datetime
import pandas as pd
from collections import defaultdict, deque
from constants import case_id, timestamp, activity, source, req_cols


class BasicZFilter:

    def __init__(self, z_threshold: int, timeframe: td):
        self.z = z_threshold
        self.delta_t = timeframe
        self.H = set()  # known attributes
        self.V = defaultdict(set)  # V[a]: set of users for known attribute a
        self.LRU = defaultdict(deque)  # LRU[a]: LRU-Queue for (t, u) pairs
        self.c = defaultdict(int)  # c[a]: current amount of users for attribute a

    def process_event(self, t: datetime, user: str, activity: str):
        output = None

        if activity not in self.H:
            self.H.add(activity)
            self.V[activity].add(user)
            self.LRU[activity].append((t, user))
            self.c[activity] = 1
        else:
            if user not in self.V[activity]:
                self.V[activity].add(user)
                self.c[activity] += 1
                self.LRU[activity].append((t, user))
            else:
                # Update timestamp of existing user
                self.LRU[activity] = deque((t1, u1) for (t1, u1) in self.LRU[activity] if u1 != user)
                self.LRU[activity].append((t, user))

            # Evict old entries
        while self.LRU[activity] and (t - self.LRU[activity][0][0]) > self.delta_t:
            old_t, old_u = self.LRU[activity].popleft()
            self.V[activity].remove(old_u)
            self.c[activity] -= 1

            # Check z-anonymity
        if self.c[activity] >= self.z:
            output = (t, user, activity)

        return output


def process_sublog(group_name, sublog_df, timedelta, z):
    zanon = BasicZFilter(z_threshold=z, timeframe=timedelta)
    sublog_df = sublog_df.sort_values(by=timestamp)
    result_rows = []

    for _, row in sublog_df.iterrows():
        t = row[timestamp]
        u = row[case_id]
        a = row[activity]
        out = zanon.process_event(t, u, a)
        if out:
            t_out, u_out, a_out = out
            result_rows.append({
                case_id: u_out,
                activity: a_out,
                timestamp: t_out,
                source: group_name
            })
    return pd.DataFrame(result_rows)



def apply_filter_wrapper(df, time_delta, z):
    # use mergesort as it is one of the only stable alogrithms available
    df = df.sort_values(by=timestamp, kind="mergesort")
    df = process_sequentially(df, time_delta, z)
    return df.sort_values(by=[case_id, timestamp])


def process_sequentially(df, timedelta, z):
    source_map = {}
    output = {
        case_id: [],
        activity: [],
        timestamp: [],
        source: []
    }

    for _, row in df.iterrows():
        t = pd.to_datetime(row[timestamp])  # Sicherstellen, dass timestamp datetime ist
        u = row[case_id]
        a = row[activity]
        s = row[source]

        if s not in source_map:
            source_map[s] = BasicZFilter(timeframe=timedelta, z_threshold=z)

        # can yield none values
        result = source_map[s].process_event(t=t, user=u, activity=a)

        if result is not None:
            (out_t, out_u, out_a) = result
            output[case_id].append(out_u)
            output[activity].append(out_a)
            output[timestamp].append(pd.to_datetime(out_t))
            output[source].append(s)

    output_df = pd.DataFrame(output)

    return output_df
    # return output_df.sort_values(by=[case_id, timestamp]).reset_index(drop=True)
