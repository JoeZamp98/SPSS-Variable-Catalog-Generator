from boxsdk import JWTAuth, Client
import csv
from http import client
import json
import numpy as np
import os
import pandas as pd
import pyreadstat
import shutil

## -- DECLARE GLOBAL VARIABLES -- ##

parent_file = 'PARENT_FILE'

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

