# pandas for handling data_frames
import pandas as pd
# used for efficient column naming
import numpy as np
from constants import write_simplified_log
from numpy import char
# for folder creation

from constants import case_id, timestamp, activity
import os
# date and time
from datetime import datetime

################################################
# Variables to customise
################################################
def convert_csv2auto(df_data):
    # global df_for_export

    # path of sources
    # delimiter of csv-file
    csv_delimiter = ','
    # Customisable variables have to be in the format:
    # ['event_j', 'dynamic_feature_h_1', 'dynamic_feature_h_2']
    # unique identifier column (static feature)
    # examples: CaseID, serial number
    # only give one unique identifier
    unique_identifier = [case_id]
    # # # Either define attributes to consider or attributes to exclude # # #
    # List of attributes to consider.
    # If left blank, a list of attributes to exclude can be given
    attributes = [activity, timestamp]
    # exclude attributes. the list of 'attributes' has to be left empty for the exclusion list to be taken into account
    attributes_to_exclude = []
    ################################################
    ################################################
    # read csv. data from disk
    # df_data = pd.read_csv(filepath_or_buffer=path_data_sources + csv_source_file_name, delimiter=csv_delimiter)
    if not attributes:
        attributes = [attr for attr in list(df_data) if attr not in (attributes_to_exclude + unique_identifier)]
    # drop all unnecessary columns
    df_important_columns = df_data[unique_identifier + attributes]
    # group data by unique identifier
    df_grouped_by_identifier = df_important_columns.groupby(unique_identifier)
    # enumerate all data in their respective column
    df_enumerated_data = df_grouped_by_identifier.aggregate(lambda x: list(x))
    # create list to store data frames of each attribute
    list_of_data_frames = []
    list_column_names = []
    # insert constant values in the beginning, but respect given order
    # use this variable to determine the insertion position
    constant_value_count = 0
    # use attributes in file name
    list_file_name = []
    # loop through all variable attributes
    for attribute in attributes:
        # create data frame from list (from enumerated data)
        df_current_iteration = pd.DataFrame.from_records(list(df_enumerated_data[attribute]))

        # if attribute is constant only use it once and do not create multiple columns
        # determined by: count unique values for each row and drops 'None' values
        # if only the first column has a value or if all columns have the same value 'df.nunique' will
        # return '1'
        # if all 'df.nunique' returns for all rows '1' it will sum up to the number of rows
        # and therefore if those numbers are the same every row only contains one unique value
        if sum(df_current_iteration.nunique(dropna=True, axis=1)) == df_current_iteration.shape[0]:

            # get only first column. all other columns should either be empty or equal
            df_current_iteration = df_current_iteration.iloc[:, 0]

            # save it in a list of data frames
            list_of_data_frames.insert(constant_value_count, df_current_iteration)
            # create meaningful header, use the attribute name
            list_column_names.insert(constant_value_count, attribute.replace(" ", ""))

            # add attribute to filename
            list_file_name.insert(constant_value_count, attribute.replace(" ", ""))

            # increase insertion position by one
            constant_value_count += 1
        else:
            # save it in a list of data frames
            list_of_data_frames.append(df_current_iteration)
            # create meaningful header, use the attribute name and a number
            list_column_names.extend(np.char.add(
                [attribute.replace(" ", "")] * list_of_data_frames[-1].shape[1],
                np.array(range(0, list_of_data_frames[-1].shape[1]), dtype=str)))
            list_file_name.append(attribute.replace(" ", ""))
    # concatenate separate data frames to one data frame
    df_for_export = pd.concat(list_of_data_frames, axis=1)
    # rename columns
    df_for_export.columns = list_column_names
    # get index (unique identifier) from enumerated data
    df_for_export.index = df_enumerated_data.index

    # reset index, otherwise unicity_activities does not work
    return df_for_export.reset_index()
