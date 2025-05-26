########################## Pre-Processing Script for PA #############################

# Before running the script in VS Code, please activate the docutils environment 
# Info on how to set up the docutils env can be found in the AI Channel OneNote
# (conda activate docutils), open the Command Palette in VS Code (Ctrl+Shift+P), select 
# "Python: Select Interpreter" and select the env-specific interpreter

##### Set-up

# Fix Python path so modules in sibling folders (e.g., repos/) can be imported
import sys
import os
sys.path.append('/home/azureuser/cloudfiles/code')

# General packages
import pandas as pd
import json

# Config packages
import configparser
import os

# Blob storage
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Anonymization 
from nlputils.components.ner.anonymization import entity_recognizer

# Import other scripts
import repos.azure_utils.preprocessing as preprocessing
#import repos.nlp_utils.src.nlputils.components.pa.pa_functions as pa_functions
import repos.azure_utils.blob_storage as blob_storage
# from repos.nlp_utils.src.nlputils.components.pa.pa_functions import (
#     remove_first_page,
#     clean_text,
#     is_gibberish, remove_gibberish_paragraphs,
#     filter_min_text_length,
#     remove_paragraphs_with_afghanistan,
#     entity_recognizer, anonymize_entities_in_dataframe,
#     tensor_to_float
# )



#################### ADJUST THE FOLLOWING PARAMETERS ##################


# ToDo: Swich from raw docs in active directory to raw docs in document container in padscgiz storage account!

# Make sure to create a folder "docs" in the same directory as your notebook
# In docs there should be one folder:
# (1) "00_raw_docs": containing all unprocessed documents

PATH_TO_ML_STUDIO_DIRECTORY = "./Users/wagner_ann/pa_25/docs/"
UNPROCESSED_DOCS_FOLDER = "00_raw_docs"

# Mark the project briefing batch number
BATCH = 'Batch_Test'

# Storage Account info
STORAGE_ACCOUNT = "padscgiz"
DOC_CONTAINER = "documents"

###################### PREPROCESSING (GENERAL) ########################

# Retrieve storage account key
path_to_config = "./Users/wagner_ann/pa_25/archive/config.cfg"
config = configparser.ConfigParser()
config.read_file(open(path_to_config))
ACCOUNT_KEY = config.get(STORAGE_ACCOUNT,'account_key')

### Preprocess docs

# Create list of all doc names and file types
# This is needed for the pre-processing in the next step
# Output will be stored in directory under ./docs/01_fileinfo

df_file_info = preprocessing.load_docs(directory_path=PATH_TO_ML_STUDIO_DIRECTORY,
                            docs_input_folder= UNPROCESSED_DOCS_FOLDER,
                            file_types=['pdf', 'docx'])

# Save in ML Studio
blob_storage.save_as_json_ML_Studio(dataframe=df_file_info,
                                    directory_path=PATH_TO_ML_STUDIO_DIRECTORY,
                                    file_path="01_fileinfo/df_files.json")

# Save in Storage Account
blob_storage.save_df_to_blob(storage_account=STORAGE_ACCOUNT,
                             container=DOC_CONTAINER,
                             blob_name=f"01_file_info/file_info_{BATCH}",
                             connection_string=ACCOUNT_KEY,
                             dataframe=df_file_info,
                             overwrite_blob=True)


# Preprocess all docs in raw_docs folder using docling (incl. layout parsing)
# Outputs txt, json, md and doctag file
# Output saved in ./docs/02_processed_docs 
# PLEASE CHECK OUTPUT, E.G. HOW MUCH "<!-- missing-text -->" in txt.

preprocessing.process_docs_docling(directory_path=PATH_TO_ML_STUDIO_DIRECTORY)


# Combine processed text of all docs to one file
# Please choose if you want to have only text or also layout info
# Output saved in ./docs/all_docs_processed

# OPTION 1: Text with layout info (output:combined_docs_with_layout.json )

preprocessing.combine_docs_with_layout(directory_path=PATH_TO_ML_STUDIO_DIRECTORY)

# ToDo: Check splitting option in layout parsing and define max token


################# PREPROCESSING (PA-SPECIFIC) ################

# Info on modification saved under "03_all_docs_processed/modification_info.json"
# ToDo: Adjust functions in pa_preprocessing (they are outdated from pa 24) and check for utils which one can be replaced



####### Placeholder for actual functions:############

# 1. Remove first page
# df_no_first = remove_first_page(df)

# 2. Clean text
# cleaned = clean_text(raw_text)

# 3. Gibberish detection and removal
# is_gib = is_gibberish(text)
# df_no_gibberish = remove_gibberish_paragraphs(df)

# 4. Filter by minimum text length
# df_min_len = filter_min_text_length(df)

# 5. Remove paragraphs mentioning Afghanistan
# df_no_afg = remove_paragraphs_with_afghanistan(df)

# 6a. Entity recognition/anonymization for a list of paragraphs
# anonymized = entity_recognizer(list_of_para)

# 6b. Entity recognition/anonymization for a DataFrame column
# df_anonymized = anonymize_entities_in_dataframe(df, text_column='paragraphs_content')

# 7. Convert tensor/string to float
# flt = tensor_to_float(val)


########################## ANONYMIZATION  ##########################

# ToDo: Check results, is german/english language in docs affecting NER quality?

# Note: Changed script because previous function have seem to have dropped some rows?
final_df['text'] = final_df['text'].apply(
    lambda t: entity_recognizer(t, entity_list=['name', 'e-mail', 'phone number', 'address'])
)
#final_df['text'] = entity_recognizer(final_df['text'], entity_list=['name', 'e-mail', 'phone number', 'address'])




##################### Save in Storage Account #######################

blob_storage.save_df_to_blob(storage_account= STORAGE_ACCOUNT, 
                            container= DOC_CONTAINER, 
                            blob_name= f"all_batches_processed/docs_processed_{BATCH}", 
                            connection_string= ACCOUNT_KEY,
                            dataframe=final_df,
                            overwrite_blob=True)



########################## Merge batches ##########################

# Store one json with all documents in documents container

df_all = blob_storage.merge_blobs(storage_account=STORAGE_ACCOUNT,
                         container=DOC_CONTAINER,
                         blob_name_starts_with="all_batches_processed/docs_processed",
                         connection_string=ACCOUNT_KEY)

blob_storage.save_df_to_blob(storage_account= STORAGE_ACCOUNT, 
                            container= DOC_CONTAINER, 
                            blob_name= f"processed_docs_all", 
                            connection_string= ACCOUNT_KEY,
                            dataframe=df_all,
                            overwrite_blob=True)  



