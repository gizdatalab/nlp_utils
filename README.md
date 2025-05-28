# NLP - utils
This is repo serves as nlp utils for document processing

The modules in package include:
1. axaparsr: Document Processing using the [axaparsr](https://github.com/axa-group/Parsr)
2. pymupdf_util: Document Processing using the [pymupdf](https://pymupdf.readthedocs.io/en/latest/)
3. ner: Utilizing the [gliner](https://github.com/urchade/GLiNER) for ner extraction or anonymization
4. doclingserver: Document Processing using the [docling](https://ds4sd.github.io/docling/)


## Prerequisites 
This package has system-level dependencies that must be installed before installing the Python package

### For Linux or Mac:
**LibreOffice** Your system needs LibreOffice installed for certain functionalities (e.g., converting documents). 
* **For Ubuntu/Debian-based systems:** 
``` 
sudo apt update 
sudo apt install libreoffice 
``` 
* **For macOS (using Homebrew):** 
```bash brew install libreoffice ``` 

### For Windows
**Option 1.LibreOffice** Download and install LibreOffice from the official website: [https://www.libreoffice.org/download/download/](https://www.libreoffice.org/download/download/)

**Option 2.Microsoft Word**

## To convert the docx to pdf:
**LibreOffice** (Linux and Mac): Get the path of libreoffice and pass it to the *utils.convert_docx_to_pdf_linux*
```
>>which soffice
/usr/bin/soffice
```
**Microsoft Word** (Windows): Use *utils.convertfile* (will leverage docx2pdf)



How to use:
Step 1: Declare Variables


```
# data asset variables
data_asset_name = 'raw_doc'
data_asset_version = '1'
get_metadata = False
# pass None if you dont want the files to be downloaded to active dir, this will save them to "/tmp/{data_asset_name}/"
local_download_folder = 'abc/

# folder location to save the batch files, logs and other important info 
processing_folder = "/mnt/batch/tasks/shared/...../....test/"
batch_size = 2

# datastore in which you want the processed files to go
datastore_name="test"
# Access key to storage account in which datastore is located
storage_account_key="xxxxx"
# folder where you want the processed files to be saved locally before they are pushed to blob-storage
# "/tmp/..." will save output in temporary folder
local_folder_path="/tmp/processed/"
# folder path in datastore (datastore points to container in blob-storage)
destination_path="processed_script/"
# variables to be used to create data asset for processed files
processed_data_asset_name= "processed_doc"
processed_data_asset_version = "5"
```

Step 2: Download raw files and Create batch files

```
# check for if the batch files for each file type exist or not
if not os.path.isdir(processing_folder + "files_info/"):
    # check if files are locally available or not : Scenario 1 when files were downloaded in tmp folder
    if local_download_folder == None:
        # check if local folder exists which contains files (only existence of folder is checked)
        if not os.path.isdir(f"/tmp/{data_asset_name}"):
            # if folder not existing then will download files to temporary folder 
            # this step is intensive and takes roughly 30 min for 3.5k files
            files = azure_utils.download_files_dataassets(data_asset_name=data_asset_name,
                                            data_asset_version=data_asset_version,
                                            get_metadata=get_metadata)
            # get files list
            files = files['files']
        else:
            # if temp folder exists then just collect all the files
            files = utils.get_files(f'/tmp/{data_asset_name}', file_extensions="*")['allfiles']
    else: # Repeat previous steps for Scenario 2: Non-Temporary folder
        if not os.path.isdir(local_download_folder):
            files = azure_utils.download_files_dataassets(data_asset_name=data_asset_name,
                                            data_asset_version=data_asset_version,
                                            get_metadata=get_metadata, 
                                            local_download_folder=local_download_folder)
        else:
            files = utils.get_files(local_download_folder, file_extensions="*")['allfiles']

# Create the batches for each file type
# if docx_to_pdf =True, will convert docx to pdf using libreoffice and 
# save them in folder'docx_to_pdf' under dir local_download_folder
# this step will internally also perform page counts and segragating imagepdf with normal pdf
# converted pdf's batching is done in processing_folder/file_info/docx2pdf_files.json
### page_count: < 5 min for 3.5k files
### image pdf check: <10 min for 3.5k files
### docx2pdf conversion: ~30 min for 3.5k files
files_df = azure_utils.create_batches(files = files, local_download_folder=local_download_folder,
                                processing_folder= processing_folder + "files_info/",
                                docx_to_pdf=True,
                                batch_size=batch_size)   
```

Step 3: Process batches

```
# most heavy lifting will happen here, the documents will be processed and uploaded to destination path in datastore given
# Two important outcomes in processing_folder
- processing_folder/
     - files_info/
        - docx_files.json
        - pdf_files.json
        -
        - batch_logger/
            - batch_logs.json

batch_logs: contains the filetype and batch number of each batch type succesfully processed and uploaded
Each filetype json (docx_files.json, pdf_files.json.....): will be getting updated with which files are processed and uploaded 
NOTE: DO NOT modify the contents and structure of these files.

azure_utils.batch_handler(processing_folder = processing_folder,
            datastore_name=datastore_name,
            storage_account_key=storage_account_key,
            local_folder_path=local_folder_path,
            destination_path=destination_path,
            data_asset_name= processed_data_asset_name,
            data_asset_version = processed_data_asset_version)
```

Step 4:Create one merged dataframe wtih docling json

```
# while combined files is returned it is also saved in the datastore,if upload_df: bool = True (default)
file_path_docling = azure_utils.df_with_docling_json(processed_data_asset_name=processed_data_asset_name,
                     processed_data_asset_version=processed_data_asset_version,
                     processing_folder=processing_folder, destination_path=destination_path, data_asset_name=data_asset_name)
```

Step 5:Create chunks
```
df= pd.read_json(file_path_docling)
df['chunks'] = df.apply(lambda x: doclingserver.hybrid_chunking_memory(x['filename'],x['json_file'],embed_model_id="BAAI/bge-base-en"), axis=1)
```