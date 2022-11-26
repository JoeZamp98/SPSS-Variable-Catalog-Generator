import numpy as np
import pandas as pd
import json

#Get column labels
def get_labels(var, column_names_to_labels_cleaned):

    all_labels = column_names_to_labels_cleaned.get(var)

    unique_labels = set(all_labels)

    if len(unique_labels) == 0:

        unique_labels = ""
    
    return unique_labels

#Get variable value labels
def get_var_val_labels(var, variable_value_labels_cleaned):

    all_var_val = variable_value_labels_cleaned.get(var)

    if all_var_val is None:

        all_var_val = ""

    text_dicts = []
    final_dicts = []

    for item in all_var_val:

        text_dicts.append(json.dumps(item, sort_keys=True))

    final_dicts = set(text_dicts)

    if len(final_dicts) == 0:

        final_dicts = ""

    return final_dicts

#Get variable measures
def get_variable_measures(var, variable_measure_cleaned):

    all_var_measures = variable_measure_cleaned.get(var)

    try:

        label = all_var_measures[0]

    except:

        label = all_var_measures

    return label


#Get variable display widths
def get_variable_width(var, variable_display_width_cleaned):

    all_variable_widths = variable_display_width_cleaned.get(var)

    try:

        label = all_variable_widths[0]

    except:

        label = all_variable_widths

    return label
