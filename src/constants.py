import os


def find_source(path, name):
    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)


file_name = "Sepsis Cases - Event Log.xes"
project_path = os.getcwd()

source_path = find_source(project_path, file_name)
activity = "concept:name"
timestamp = "time:timestamp"
source = "org:group"
case_id = "case:concept:name"
req_cols = [case_id, activity, timestamp, source]

# results path
res_path = f"{project_path}/res/"
if not os.path.exists(res_path):
    os.makedirs(res_path)

# Evaluation

multiprocessing = True
multiprocessing_quantifying_risk = True

# write things to disk
write_simplified_log = False
write_middle_results = True

# abstract timestamps
abstract_timestamps = True
# abtraction_level = "d"
# abstractionLevel =

# default repetitions = 1
RISK_ASSESSMENT_REPETITIONS = 1

def set_risk_assesment_repitionions(i: int):
    if i < 1:
        raise Exception("Repetitions should not be below '1'.")
    # refer to global variable
    global RISK_ASSESSMENT_REPETITIONS

    # set global variable to i
    RISK_ASSESSMENT_REPETITIONS = i



