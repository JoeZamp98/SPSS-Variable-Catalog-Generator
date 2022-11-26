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

def determine_variable_inclusion(extracted_metadata):

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
    
    variable_catalog = pd.DataFrame() #Initializes as a blank DataFrame, which we will gradually build out below

    #Append list of all unique variables as keys

    variable_catalog['variable_name'] = all_unique_variables

    return variable_catalog

## -- GENERATE FLAGS FOR INCONSISTENT VARIABLES -- ##    

def generate_inconsistency_flags(key_metadata_types):

    #Column labels 
    def detect_col_label_inconsistenceis(key_metadata_types):

        inconsistent_col_labels = {}

        var_val_dict = key_metadata_types['column_names_to_labels'][0]

        for variable in var_val_dict:

            val_instances = var_val_dict[variable]

            if len(val_instances) > 0:

                comparison_result = all(ele == val_instances[0] for ele in val_instances)

                if comparison_result == False:

                    inconsistent_col_labels[variable] = "Inconsistent column labels"
                
                else:

                    inconsistent_col_labels[variable] = ""

        return inconsistent_col_labels

    inconsistent_col_labels = detect_col_label_inconsistenceis(key_metadata_types)

    #Variable value labels
    def detect_var_val_inconsistencies(key_metadata_types):

        inconsistent_var_vals = {}

        var_val_dict = key_metadata_types['variable_value_labels'][0]

        for variable in var_val_dict:

            val_instances = var_val_dict[variable]

            if len(val_instances) > 0:

                comparison_result = all(ele == val_instances[0] for ele in val_instances)

                if comparison_result == False:

                    inconsistent_var_vals[variable] = "inconsistent variable value labels"

        return inconsistent_var_vals

    inconsistent_var_vals = detect_var_val_inconsistencies(key_metadata_types)

    return inconsistent_col_labels, inconsistent_var_vals

## -- IMPORT TEAM COMMENTS FROM GOOGLE SHEET -- ##

def import_comments(box_client):
    
    file_id = 'FILE_ID' #This is the team comments Google Sheet
    file_content = box_client.file(file_id).content()

    file = box_client.file(file_id)

    with open('team_comments.xlsx', 'wb') as open_file:

        box_client.file(file_id).download_to(open_file)
        open_file.close()
    
    team_comments = pd.read_excel('team_comments.xlsx')

    os.remove('team_comments.xlsx')

    #Convert team comments to a dictionary

    team_comments_dict = {}

    team_comments.set_index("Variable", inplace=True)

    for var in team_comments.index:

        team_comments_dict[var] = team_comments.loc[var, "Team Comments"]

    return team_comments_dict

## -- POPULATE THE DESCRIPTIVE COLUMNS OF THE VARIABLE CATALOG -- ##

def populate_columns(variable_catalog, variable_inclusion, inconsistencies, team_comments_dict, key_metadata_types):

    all_unique_variables = variable_inclusion[0]
    all_variable_instances = variable_inclusion[1]
    list_of_variable_appearances = variable_inclusion[2]
    inconsistent_col_labels = inconsistencies[0]
    inconsistent_var_vals = inconsistencies[1]
    column_names_to_labels_cleaned = key_metadata_types[0][0]
    variable_value_labels_cleaned = key_metadata_types[1][0]
    variable_display_width_cleaned = key_metadata_types[2][0]
    variable_measure_cleaned = key_metadata_types[3][0]

    for var in all_unique_variables:

        #Column labels
        variable_catalog['variable_labels'] = variable_catalog['variable_name'].apply(lambda var: get_labels(var, column_names_to_labels_cleaned))
        
        #Variable value labels
        variable_catalog['variable_value_labels'] = variable_catalog['variable_name'].apply(lambda var: get_var_val_labels(var, variable_value_labels_cleaned))

        #Variable measures
        variable_catalog['variable_measures'] = variable_catalog['variable_name'].apply(lambda var: get_variable_measures(var, variable_measure_cleaned))

        #Variable display widths
        variable_catalog['variable_widths'] = variable_catalog['variable_name'].apply(lambda var: get_variable_width(var, variable_display_width_cleaned))

        #Inconsistent column label flag
        variable_catalog['inconsistent_column_labels'] = variable_catalog['variable_name'].apply(lambda var: inconsistent_col_labels.get(var))

        #Inconsistent variable value label flag
        variable_catalog['inconsistent_variable_value_labels'] = variable_catalog['variable_name'].apply(lambda var: inconsistent_var_vals.get(var))

        #URC team comments
        variable_catalog['URC_team_comments'] = variable_catalog['variable_name'].apply(lambda var: team_comments_dict.get(var))

        #Number of appearances
        variable_catalog['number_of_appearances'] = variable_catalog['variable_name'].apply(lambda var: all_variable_instances.get(var))

        #List of all appearances
        variable_catalog['list_of_appearances'] = variable_catalog['variable_name'].apply(lambda var: list_of_variable_appearances.get(var))

    if os.path.isdir('generated-csv-files') == False:

        os.mkdir('generated-csv-files')

    variable_catalog.to_csv('generated-csv-files/variable_catalog.csv')

    return variable_catalog

## -- POST FILE TO RELEVANT LOCATION IN BOX -- ##

def post_to_box(box_client):

    folder_id = 'FOLDER_ID'
    file_id = 'FILE_ID'

    existing_files = box_client.folder(folder_id = folder_id).get_items()

    file_names = []

    for file in existing_files:

        file_names.append(file.name)

    if 'data_dictionary.csv' in file_names:

        updated_file = box_client.file(file_id).update_contents('generated-csv-files/variable_catalog.csv')
        print(f'{updated_file.name} has been updated with a new version.')

    else:

        new_file = box_client.folder(folder_id).upload('generated-csv-files/variable_catalog.csv')
        print(f'An initial version of {new_file.name} has been uploaded successfully.')

    shutil.rmtree('temp')

## -- FUNCTION CALLS -- ##

box_client = establish_box_connection()
download_spss_files(box_client)
all_original_spss_files = determine_import_list()

extracted_metadata = extract_metadata(all_original_spss_files)
variable_inclusion = determine_variable_inclusion(extracted_metadata)
key_metadata_types = organize_metadata_by_var(extracted_metadata, variable_inclusion)
inconsistencies = generate_inconsistency_flags(key_metadata_types)
blank_catalog = generate_variable_catalog(variable_inclusion)
team_comments = import_comments(box_client)

variable_catalog = populate_columns(blank_catalog, variable_inclusion, inconsistencies, team_comments, key_metadata_types)
post_to_box(box_client)
