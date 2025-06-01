from datetime import timedelta
from collections import deque, defaultdict
from constants import case_id, timestamp, activity, source
import pandas as pd


class BalancedZFilter:
    def __init__(self, delta_t: int, z: int):
        # time window
        self.delta_t = delta_t

        if z < 1:
            raise Exception("z threshold should not be below 1.")
        # user threshold
        self.z = z

        # same as in basic filtering
        self.H = set()
        self.V = defaultdict(set)
        self.LRU = defaultdict(deque)
        self.c = defaultdict(int)
        self.buffer = defaultdict(deque)  # Buffer for every attribute

    def process_event(self, t, u, a):
        outputs = []

        # Attribut noch nie gesehen
        if a not in self.H:
            self.H.add(a)
            self.V[a].add(u)
            self.LRU[a].append((t, u))
            self.buffer[a].append((t, u, a))
            self.c[a] = 1

        # Attribut existiert schon
        else:
            if u not in self.V[a]:
                self.V[a].add(u)
                self.c[a] += 1
                self.LRU[a].append((t, u))
                self.buffer[a].append((t, u, a))
            else:
                # Update timestamp des Nutzers in LRU (kein c-Inkrement!)
                self.LRU[a] = deque((t1, u1) for (t1, u1) in self.LRU[a] if u1 != u)
                self.LRU[a].append((t, u))
                # keine neue Anonymität → kein Zuwachs
                self.buffer[a].append((t, u, a))

        # evict old events
        while self.LRU[a] and (t - self.LRU[a][0][0]) > self.delta_t:
            old_t, old_u = self.LRU[a].popleft()
            if old_u in self.V[a]:
                self.V[a].remove(old_u)
                self.c[a] -= 1

        # clear buffer from old entries
        self.buffer[a] = deque([(bt, bu, ba) for (bt, bu, ba) in self.buffer[a] if (t - bt) <= self.delta_t])

        # Wenn jetzt c[a] ≥ z → Buffer ausgeben
        if self.c[a] >= self.z:
            outputs.extend(self.buffer[a])
            self.buffer[a].clear()

        return outputs


def process_sublog(group_name, sublog_df, delta_t, z):
    zanon = BalancedZFilter(delta_t=delta_t, z=z)
    sublog_df = sublog_df.sort_values(by=timestamp)
    result_rows = []

    for _, row in sublog_df.iterrows():
        t = row[timestamp]
        u = row[case_id]
        a = row[activity]
        outputs = zanon.process_event(t, u, a)
        for (t_out, u_out, a_out) in outputs:
            result_rows.append({
                case_id: u_out,
                activity: a_out,
                timestamp: t_out,
                source: group_name
            })

    return pd.DataFrame(result_rows)
