from boxsdk import JWTAuth, Client
import csv
from http import client
import json
import numpy as np
import os
import pandas as pd
import pyreadstat
import shutil

#Import Lambda functions (defined in a complementary file)

from variableCatalogLambdaFunctions import get_labels
from variableCatalogLambdaFunctions import get_var_val_labels
from variableCatalogLambdaFunctions import get_variable_measures
from variableCatalogLambdaFunctions import get_variable_width

## -- DECLARE GLOBAL VARIABLES -- ##

parent_file = 'PARENT_FILE'
retain_specific_files = []

### -- ESTABLISH CONNECTION TO BOX -- ###

def establish_box_connection():

    auth = JWTAuth.from_settings_file(r'box_json.json') #file is hidden from public repository
    box_client = Client(auth)

    service_account = box_client.user().get()

    print(f"Connected to: {service_account}")

    return box_client

#Download SPSS files from Box to Local Directory

def download_spss_files(box_client):

    folder_id = 'DIRECTORY_ID'

    directory_items = box_client.folder()

    os.mkdir('temp')

    for item in directory_items:

        file_name = 'temp/' + str(file_name)

        file_id = str(item.id)

        with open(file_name, 'wb') as open_file:

            box_client.file(file_id).download_to(open_file)

            open_file.close()

#Establish & properly order list of files in 'temp' directory
            
def determine_import_list():

    directory_files = os.listdir('temp')

    all_original_spss_files = [item for item in directory_files if item.endswith('.sav')]

    all_original_spss_files.insert(0, all_original_spss_files.pop(all_original_spss_files.index(parent_file))) #Moves the parent file to the top of the sequence, so it will serve as the reference file in any overridden metadata conflicts

    return all_original_spss_files

## -- EXTRACT AND CATALOG METADATA FROM EACH SPSS FILE -- ##    

def extract_metadata(all_original_spss_files):

    #Generate empty dictionaries to house extracted metadata
    all_column_names_dict = {}
    all_column_labels_dict = {}
    all_column_names_to_labels_dict = {}
    variable_value_labels_dict = {}
    variable_measure_dict = {}
    variable_display_width_dict = {}
    value_labels_dict = {}
    missing_ranges_dict = {}
    variable_types_dict = {}

    #Generate lists of all metadata types/storage vehicles to enable efficient looping later in function
    all_metadata_types = ['column_names', 'column_labels', 'column_names_to_labels', 'variable_value_labels', 'variable_measure', 'variable_display_width', 'value_labels', 'missing_ranges', 'variable_types']

    all_metadata_dicts = [all_column_names_dict, all_column_labels_dict, all_column_names_to_labels_dict, variable_value_labels_dict, variable_measure_dict, variable_display_width_dict, value_labels_dict, missing_ranges_dict, variable_types_dict]

    #Determine which files should be merged - leaving "retain_particular_files" blank will merge all files in the directory

    if len(retain_specific_files) > 1:

        active_files = [x for x in all_original_spss_files if x in retain_specific_files]

    elif len(retain_specific_files) == 0:

        active_files = all_original_spss_files

    print("Merging " + str(len(active_files)) + " files.")

    #Extract metadata using the pyreadstat package
    for file in all_original_spss_files:

        df, meta = pyreadstat.read_sav('temp/' + str(file))

        #Extract each piece of metadata from the SAV file

        column_names = meta.column_names
        column_labels = meta.column_labels
        column_names_to_labels = meta.column_names_to_labels

        variable_value_labels = meta.variable_value_labels
        variable_measure = meta.variable_measure
        variable_display_width = meta.variable_display_width

        value_labels = meta.value_labels
        missing_ranges = meta.missing_ranges
        variable_types = meta.original_variable_types

        #Store each set of metadata as a dictionary entry, KEYED by file

        all_column_names_dict[file] = column_names
        all_column_labels_dict[file] = column_labels
        all_column_names_to_labels_dict[file] = column_names_to_labels

        variable_value_labels_dict[file] = variable_value_labels
        variable_measure_dict[file] = variable_measure
        variable_display_width_dict[file] = variable_display_width

        value_labels_dict[file] = value_labels
        missing_ranges_dict[file] = missing_ranges
        variable_types_dict[file] = variable_types

    #Create dictionary housing all metadata, KEYED by type, SUB-KEYED by file

    all_original_metadata = {all_metadata_types[i]: all_metadata_dicts[i] for i in range(len(all_metadata_dicts))}

    return all_original_metadata, active_files

## -- CREATE A DIRECTORY OF ALL UNIQUE VARIABLES AND HOW FREQUENTLY THEY APPEAR ACROSS RELEVANT FILES -- ##

def determine_variable_inclusion(extracted_metadata, explicit_overrides):

    all_unique_variables = []
    all_column_names_dict = extracted_metadata[0]

    #Create a comprehensive list of unique variables    

    for survey in extracted_metadata['column_names']:

        for colname in all_column_names_dict[survey]:

            if colname not in all_unique_variables:

                all_unique_variables.append(colname)

    #Count how frequently each variable appears

    all_variable_instances = {}

    for survey in all_column_names_dict:

        for colname in all_column_names_dict[survey]:

            if colname not in all_variable_instances:

                all_variable_instances[colname] = 1

            elif colname in all_variable_instances:

                all_variable_instances[colname] += 1

    #Create a list of which file each variable appears in
                
    list_of_variable_appearances = {}

    for survey in all_column_names_dict:

        for colname in all_column_names_dict[survey]:

                if colname not in list_of_variable_appearances:

                    list_of_variable_appearances[colname] = [survey]

                elif  colname in list_of_variable_appearances:

                    list_of_variable_appearances[colname].append(survey)

    return all_unique_variables, all_variable_instances, list_of_variable_appearances

## -- ORGANIZE METADATA OF VARIOUS TYPES INTO A DICTIONARY KEYED BY VARIABLE -- ##

def organize_metadata_by_var(extracted_metadata, variable_inclusion):

    all_unique_variables = variable_inclusion[0]
    all_original_metadata = extracted_metadata[0]

    #GROUP METADATA INTO DICTIONARIES KEYED BY COLUMN NAME

    column_names_to_labels_cleaned = {}
    variable_value_labels_cleaned = {}
    missing_ranges_cleaned = {}
    variable_display_width_cleaned = {}
    variable_measure_cleaned = {}

    inconsistent_column_names_to_labels = {}
    inconsistent_variable_value_labels = {}
    inconsistent_missing_ranges = {}
    inconsistent_variable_display_width = {}
    inconsistent_variable_measures = {}

    key_metadata_types = {'column_names_to_labels': [column_names_to_labels_cleaned, inconsistent_column_names_to_labels],
    'variable_value_labels': [variable_value_labels_cleaned, inconsistent_variable_value_labels],
    'missing_ranges': [missing_ranges_cleaned, inconsistent_missing_ranges],'variable_display_width': [variable_display_width_cleaned, inconsistent_variable_display_width], 'variable_measure': [variable_measure_cleaned, inconsistent_variable_measures]}

    for type in key_metadata_types: #Cycles through metadata type

        full_file_set = all_original_metadata[type]

        for file in full_file_set: #Cycles through files

            instance = full_file_set[file]

            for colname in instance: #Cycles through individual variable (column) names within each file

                if colname in all_unique_variables: #If the variable (column) name appears in active unique variables, the corresponding metadata is appended to a cleaned dictionary

                    cleaned_dict = key_metadata_types[type][0]

                    if colname not in cleaned_dict: 

                        cleaned_dict[colname] = [instance[colname]]
                    
                    elif colname in cleaned_dict:

                        cleaned_dict[colname].append(instance[colname])    

    return key_metadata_types


## -- CONSTRUCT CSV FOR ACTIVE, CONSISTENT VARIABLES -- ##

def construct_csv(extracted_metadata, inconsistencies, variable_inclusion):

    active_files = extracted_metadata[1]
    all_variable_instances = variable_inclusion[3]
    inconsistent_variables = inconsistencies[0]

    #Exctract CSV Data from SAV files

    all_extracted_csv_files = []

    for file in active_files:

        df, meta = pyreadstat.read_sav('temp/' + str(file))

        all_extracted_csv_files.append(df)

    #Append 'wave' column to enable filtering/display over time

    wave_counter = 0

    for file in all_extracted_csv_files:

        file['wave'] = active_files[wave_counter]

        wave_counter += 1
        
    #Concatenate dataframes into one

    full_dataframe = pd.concat(all_extracted_csv_files, keys = active_files)

    return full_dataframe

## -- PRODUCE VARIABLE CATALOG -- ##

def generate_variable_catalog(variable_inclusion):

    all_unique_variables = variable_inclusion[0]
    
    data_dictionary = pd.DataFrame() #Initializes as a blank DataFrame, which we will gradually build out below

    #Append list of all unique variables as keys

    data_dictionary['variable_name'] = all_unique_variables
