from constants import timestamp
import pandas as pd
from datetime import datetime

def run_abstraction(eventLog):
    """
    Applies timestamp abstraction to the given event log.

    Parameters:
    eventLog (DataFrame): The event log containing timestamps.
    abstractionLevel (str): The frequency level for rounding timestamps (e.g., 'H' for hour, 'T' for minute).

    Returns:
    DataFrame: The event log with abstracted timestamps.
    """
    eventLog[timestamp] = pd.to_datetime(eventLog[timestamp])
    #run abstraction to date
    eventLog[timestamp] = eventLog[timestamp].dt.date

    eventLog[timestamp] = pd.to_datetime(eventLog[timestamp])

    # print(datetime.fromtimestamp(eventLog[timestamp]))
    # # Convert timestamp column to datetime format, removing ' UTC' if present
    # eventLog[timestamp] = pd.to_datetime(
    #     eventLog[timestamp].str.replace(" UTC", "", regex=True),
    #     format='ISO8601',
    #     errors='raise'
    # )

    # Round timestamps to month end
    # eventLog[timestamp] = eventLog[timestamp].apply(lambda x: x.ceil(freq=abstractionLevel))
    eventLog.to_csv("test.csv")
    return eventLog

    # Round timestamps to the specified abstraction level
    # eventLog[constants.COL_NAME_TIMESTAMP] = eventLog[constants.COL_NAME_TIMESTAMP].apply(
    #     lambda x:
    #         MonthEnd().rollforward(x)
    # )
