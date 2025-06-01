import os

def find_source(path, name):
    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)

source_path = find_source("/home/fabian/Github/", "Sepsis Cases - Event Log.xes")
activity = "concept:name"
timestamp = "time:timestamp"
source = "org:group"
case_id = "case:concept:name"
req_cols = [case_id, activity, timestamp, source]
