########################## Inference Script for PA #############################
# ToDo: Script not adjusted yet for binary models

# Before running the script in VS Code, please activate the developpp-inference environment 
# Info on how to set up the env can be found in the AI Channel OneNote
# pip install requirements.txt with packages
# (conda activate pa-inference), open the Command Palette in VS Code (Ctrl+Shift+P), select 
# "Python: Select Interpreter" and select the env-specific interpreter

##### Set-up

# General packages
import pandas as pd
import json
import numpy as np

# Config packages
import configparser
import os

# Blob storage
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Other scripts
import repos.azure_utils.blob_storage as blob_storage
import inference


#################### ADJUST THE FOLLOWING PARAMETERS ##################

# ToDo: Swich from raw docs in active directory to raw docs in document container in padscgiz storage account

# Make sure to create a folder "docs" in the same directory as your notebook
# In docs there should be one folder:
# (1) "00_raw_docs": containing all unprocessed documents

PATH_TO_ML_STUDIO_DIRECTORY = "./Users/wagner_ann/pa_25/docs/"

# Mark the project briefing batch number
BATCH = 'Batch_Test'

# Storage Account info
STORAGE_ACCOUNT = "padscgiz"
DOC_CONTAINER = "documents"
INFERENCE_CONTAINER = "inference"

# Define models and "other" category
TF_models = {"agriculture_develoPPP_binary_TF": "Other",
              "agriculture_develoPPP_ML_TF": "Nicht Landwirtschaft"}

SF_models = {"health_develoPPP_ML_SF": "Nicht Gesundheit"}

# Aggregate results
sector_threshold = 0.5
subsector_threshold = 0.5
two_step_topics = {'Agriculture': ['LWS Innovationen']}




######################## Load paragraphs ####################################


# Retrieve storage account key
path_to_config = "./Users/wagner_ann/pa_25/archive/config.cfg"
config = configparser.ConfigParser()
config.read_file(open(path_to_config))
ACCOUNT_KEY = config.get(STORAGE_ACCOUNT,'account_key')

# Load batch of paragraphs from storage

df_paragraphs = pd.DataFrame(blob_storage.load_blob(
                        storage_account=STORAGE_ACCOUNT, 
                        container=DOC_CONTAINER,
                        connection_string=ACCOUNT_KEY,
                        blob_path=f"all_batches_processed/docs_processed_{BATCH}", 
                        file_type="json"))


####################### Do inference ####################################

df_test = df_paragraphs[:10]

# Without SetFit

for model, other_class in TF_models.items(): 

    # Do inference
    predictions = inference.inference_TF(paragraphs_dataframe=df_test,
                                                text_column="text",
                                                model_name=model,
                                                directory_path=PATH_TO_ML_STUDIO_DIRECTORY)
    
    # Drop "other" class
    predictions.drop(columns=[other_class], inplace=True)

    # Attach to df 
    df_test = pd.concat([df_test, predictions], axis=1)

    print(df_test.head())


# Do inference - SetFit 

for model, other_class in SF_models.items(): 

    # Do inference
    predictions = inference.inference_SF(paragraphs_dataframe=df_test,
                                         text_column="text",
                                         model_name=model,
                                         directory_path=PATH_TO_ML_STUDIO_DIRECTORY)
    
    # Drop "other" class
    predictions.drop(columns=[other_class], inplace=True)

    # Attach to df 
    df_test = pd.concat([df_test, predictions], axis=1)

    print(df_test)



# Save detailde results (by paragraph) in Storage
blob_storage.save_df_to_blob(storage_account= STORAGE_ACCOUNT, 
                            container= INFERENCE_CONTAINER, 
                            blob_name= f"detailed_results/detailed_results_{BATCH}", 
                            connection_string= ACCOUNT_KEY,
                            dataframe=df_test,
                            overwrite_blob=True)  


######################## Aggregate results on document level ###########################


##### Load data from storage 

df_paragraphs = pd.DataFrame(blob_storage.load_blob(storage_account=STORAGE_ACCOUNT,
                                 container=INFERENCE_CONTAINER,
                                 connection_string=ACCOUNT_KEY,
                                 blob_path=f"detailed_results/detailed_results_{BATCH}",
                                 file_type="json"))


##### Two-step classifiers 

df_paragraphs = inference.two_step_classifier(predictions_dataframe=df_paragraphs,
                                              two_step_dictionary=two_step_topics,
                                              sector_threshold=sector_threshold,
                                              subsector_threshold=subsector_threshold)


##### Change predictions to binary 0 or 1 based on threshold

subsector_list = df_paragraphs.drop(columns=['file_name', 'text', 'batch']).columns

for subsector in subsector_list:
    df_paragraphs[subsector] = np.where(df_paragraphs[subsector] > subsector_threshold, 1, 0)


###### Aggregate results 

# Find total number of paragraphs per doc
para_count_per_doc = df_paragraphs.groupby('file_name').count()['text'].reset_index()
para_count_per_doc.rename(columns={'text' : 'paragraph count'}, inplace=True)

# Add number of paragraphs per topic 
sector_count_per_doc = df_paragraphs.groupby('file_name').sum().reset_index()

# Merge
df_paragraphs= para_count_per_doc.merge(sector_count_per_doc, left_on="file_name", right_on="file_name")

# Replace count of paragraphs by percentage share 
columns_to_operate = df_paragraphs.columns.difference(['file_name', 'paragraph count'])
df_paragraphs[columns_to_operate] = df_paragraphs[columns_to_operate].div(df_paragraphs['paragraph count'], axis=0)

# Round values to two decimals 
df_paragraphs[subsector_list] = df_paragraphs[subsector_list].round(2)


##### Save aggregated results in Storage 

blob_storage.save_df_to_blob(storage_account= STORAGE_ACCOUNT, 
                            container= INFERENCE_CONTAINER, 
                            blob_name= f"aggregated_results/aggregated_results_{BATCH}", 
                            connection_string= ACCOUNT_KEY,
                            dataframe=df_paragraphs,
                            overwrite_blob=True)  



######################## Merge all batch results ###############################

df_all = blob_storage.merge_blobs(storage_account=STORAGE_ACCOUNT,
                         container=INFERENCE_CONTAINER,
                         blob_name_starts_with="aggregated_results/aggregated_results",
                         connection_string=ACCOUNT_KEY)


blob_storage.save_df_to_blob(storage_account= STORAGE_ACCOUNT, 
                            container= INFERENCE_CONTAINER, 
                            blob_name= f"aggregated_results_all", 
                            connection_string= ACCOUNT_KEY,
                            dataframe=df_paragraphs,
                            overwrite_blob=True)  


