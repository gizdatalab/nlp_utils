import os
import logging
from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential
from azureml.fsspec import AzureMachineLearningFileSystem
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from typing import Optional, Dict, List, Any, Union
from azure.ai.ml.entities import Data
from azure.ai.ml.constants import AssetTypes
from nlputils import utils
from nlputils.components.docling_util import doclingserver
import pandas as pd

def read_files_dataassets(
    data_asset_name: str,
    data_asset_version: str,
    list_files: bool = True
) -> Optional[List[str]]:
    """
    Retrieves and optionally lists files from a specified version of an Azure Machine Learning data asset.

    Args:
        data_asset_name (str): The name of the data asset.
        data_asset_version (str): The version of the data asset to read.
        list_files (bool, optional): If True, lists the files within the data asset. Defaults to True.

    Returns:
        Optional[List[str]]: A list of file paths within the data asset if list_files is True and
                           the operation is successful. Returns None if the data asset is not found
                           or an error occurs.
    """
    # 1. Input Validation
    if not data_asset_name:
        logging.error("Data asset name must be provided.")
        return None
    if not data_asset_version:
        logging.error("Data asset version must be provided.")
        return None

    logging.info(f"Reading files from data asset: {data_asset_name}, version: {data_asset_version}")

    # 2. Connect to Azure ML Workspace
    try:
        ml_client = MLClient.from_config(credential=DefaultAzureCredential())
    except Exception as e:
        logging.error(f"Error connecting to Azure ML workspace: {e}")
        print(f"Error connecting to Azure ML workspace: {e}")
        return None

    # 3. Get Data Asset
    try:
        data_asset = ml_client.data.get(data_asset_name, version=data_asset_version)
    except Exception as e:
        logging.error(f"Error retrieving data asset '{data_asset_name}', version '{data_asset_version}': {e}")
        print(f"Error retrieving data asset: {data_asset_name}, version: {data_asset_version}, error: {e}")
        return None

    # 4. Create AzureMachineLearningFileSystem
    try:
        fs = AzureMachineLearningFileSystem(data_asset.path)
    except Exception as e:
        logging.error(f"Error creating AzureMachineLearningFileSystem: {e}")
        print(f"Error creating AzureMachineLearningFileSystem for data asset: {data_asset_name}, error: {e}")
        return None

    # 5. List and Optionally return Files list
    try:
        all_paths = fs.ls()
        logging.info(f"Found {len(all_paths)} files in data asset '{data_asset_name}':{data_asset_version}")
        print(f"All paths in the data asset '{data_asset_name}':")
        for pt in all_paths:
            print(pt)
        if list_files:
            return all_paths  # Return the list of paths
        else: 
            return None #If list_files is false, return None
    except Exception as e:
        logging.error(f"Error listing files in data asset: {e}")
        print(f"Error listing files in data asset: {data_asset_name}, error: {e}")
        return None 
    

def download_files_dataassets(
    data_asset_name: str,
    data_asset_version: str,
    folder_to_download: Optional[str] = None,
    local_download_folder: Optional[str] = None,
    get_metadata: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Downloads files from a specified data asset version in Azure Machine Learning and optionally its metadata.
    Files will be downloaded to temporary folder omly.

    Args:
        data_asset_name (str): The name of the data asset.
        data_asset_version (str): The version of the data asset.
        folder_to_download (Optional[str], optional): The folder path within the data asset to download.
            If None, downloads all files in the data asset. Defaults to None.
        local_download_folder: (Optional[str], optional): The folder path to where files will be downloaded.
            If None, downloads files to tmp folder (which will be removed when compute is gone). Defaults to None.
        get_metadata (bool, optional): Whether to retrieve the data asset's description and tags.
            Defaults to True.

    Returns:
        Optional[Dict[str, Any]]: A dictionary containing the list of downloaded file paths ('files'),
            and optionally the data asset description and tags.
            Returns None if the data asset is not found or an error occurs.
    """
    # 1. Input Validation and Logging
    if not data_asset_name:
        logging.error("Data asset name must be provided.")
        return None
    if not data_asset_version:
        logging.error("Data asset version must be provided.")
        return None
    logging.info(f"Retrieving files from data asset: {data_asset_name}, version: {data_asset_version}, folder: {folder_to_download}")

    # 2. Connect to Azure ML Workspace
    try:
        ml_client = MLClient.from_config(credential=DefaultAzureCredential())
    except Exception as e:
        logging.error(f"Error connecting to Azure ML workspace: {e}")
        print(f"Error connecting to Azure ML workspace: {e}")
        return None

    # 3. Get Data Asset
    try:
        data_asset = ml_client.data.get(data_asset_name, version=data_asset_version)
    except Exception as e:
        logging.error(f"Error retrieving data asset: {e}")
        print(f"Error retrieving data asset: {e}")
        return None

    # 4. Prepare Download Directory
    if not local_download_folder:
        local_download_folder = f"/tmp/{data_asset_name}"
        os.makedirs(local_download_folder, exist_ok=True)
    else:
        try:
            os.makedirs(local_download_folder, exist_ok=True)
        except Exception as e:
            logging.error(e)
            return None
        

    # 5. Initialize the AzureMachineLearningFileSystem
    try:
        fs = AzureMachineLearningFileSystem(data_asset.path)
    except Exception as e:
        logging.error(f"Error initializing AzureMachineLearningFileSystem: {e}")
        print(f"Error initializing AzureMachineLearningFileSystem: {e}")
        return None

    # 6. Download Files
    lst: List[str] = []
    try:
        files_to_download = fs.ls(folder_to_download) if folder_to_download else fs.ls()  # Corrected logic
        for file_path in files_to_download:
            if fs.isfile(file_path):
                local_file_path = os.path.join(local_download_folder, os.path.basename(file_path))
                with fs.open(file_path, "rb") as remote_file, open(local_file_path, "wb") as local_file:
                    local_file.write(remote_file.read())
                logging.info(f"Downloaded '{file_path}' to '{local_file_path}'")
                print(f"Downloaded '{file_path}' to '{local_file_path}'")
                lst.append(local_file_path)
    except Exception as e:
        logging.error(f"Error during file download: {e}")
        print(f"Error during file download: {e}")
        return None  # Important: Return None if download fails

    # 7. Construct and Return Result
    result: Dict[str, Any] = {"files": lst}  # Start with the base data

    if get_metadata:
        result["description"] = data_asset.description
        result["tags"] = data_asset.tags
        result["folder_location"] = local_download_folder

    return result



def upload_folder_to_datastore(
    datastore_name: str,
    storage_account_key: str,
    local_folder_path: str,
    destination_path: str,  # Path within the datastore
    create_data_asset: bool = False, #flag to create data asset
    data_asset_name: Optional[str] = None,
    data_asset_version: Optional[str] = None
) -> None:
    """
    Uploads a local folder from the Azure ML workspace's file system to a specified datastore
    and optionally creates a data asset.

    Args:
        subscription_id (str): Your Azure subscription ID.
        resource_group (str): The resource group name.
        workspace_name (str): Your Azure ML workspace name.
        datastore_name (str): The name of the datastore to upload to.
        local_folder_path (str): The path to the local folder in the workspace's file system.
        destination_path (str): The path within the datastore where the folder will be uploaded.
        create_data_asset (bool, optional): Whether to create a data asset after uploading. Defaults to False.
        data_asset_name (Optional[str], optional): The name of the data asset to create. Required if create_data_asset is True. Defaults to None.
        data_asset_version (Optional[str], optional): The version of the data asset.  Defaults to "1".
    """
    logging.info(
        f"Uploading folder: {local_folder_path} to datastore: {datastore_name}, "
        f"destination: {destination_path}, create_data_asset: {create_data_asset}"
    )

    # 1. Input Validation
    if not local_folder_path:
        raise ValueError("local_folder_path must be provided.")
    if not destination_path:
        raise ValueError("destination_path must be provided.")
    if create_data_asset and not data_asset_name:
        raise ValueError("data_asset_name must be provided when create_data_asset is True.")
    if not storage_account_key:
        raise ValueError("account access key must be provided.")


    # 2. Connect to Azure ML Workspace
    try:
        ml_client = MLClient.from_config(credential=DefaultAzureCredential())
    except Exception as e:
        logging.error(f"Error connecting to Azure ML workspace: {e}")
        print(f"Error connecting to Azure ML workspace: {e}")
        return None
    print("getting client")

    # 3. Get the Datastore
    try:
        datastore = ml_client.datastores.get(name=datastore_name)
        storage_account = datastore.account_name
        container_name = datastore.container_name
    except Exception as e:
        logging.error(f"Error retrieving datastore '{datastore_name}': {e}")
        print(f"Error retrieving datastore '{datastore_name}': {e}")
        return None
    print("getting datastore")

    # 4. Connect to datastore using blob-client
    try:
        blob_service_client = BlobServiceClient(account_url=f"https://{storage_account}.blob.core.windows.net", credential=storage_account_key) 
        container_client = blob_service_client.get_container_client(container_name)
        folder_path = destination_path
        blob_list = container_client.list_blobs(name_starts_with=folder_path)
        if any(blob.name.startswith(folder_path) for blob in blob_list):
            logging.info(f"Folder (blob prefix) '{folder_path}' already exists.")
            print(f"Folder (blob prefix) '{folder_path}' already exists.")
        else:
            logging.info(f"Folder (blob prefix) '{folder_path}' does not exist. Creating it.")
            print(f"Folder (blob prefix) '{folder_path}' does not exist. Creating it.")
            # 4. Create the folder (by creating an empty blob with the folder name)
            blob_client = blob_service_client.get_blob_client(container = container_name, blob=folder_path)
            blob_client.upload_blob(data=b"", overwrite=True)  # Create an empty blob
            logging.info(f"Folder (blob prefix) '{folder_path}' created.")
            print(f"Folder (blob prefix) '{folder_path}' created.")   
    except Exception as e:
        logging.error(f"Error accessing the  blob container doesnt exist, Check storage_account key or if container in Storage account {storage_account} exists or not")   
        print(f"Error accessing the  blob container doesnt exist, Check storage_account key or if container in Storage account {storage_account} exists or not")
        return None
    print("connect with blob client")
    

    # 5. Upload the Folder
    try:
        # blob_storage_upload_folder = f"{datastore.name}/{destination_path}/"  # Combine datastore name and destination
        # bcontainer_client = blob_service_client.get_container_client(container_name)
        # print(blob_storage_upload_folder)

        # Walk through the local folder
        for root, _, files in os.walk(local_folder_path):
            for file_name in files:
                local_file_path = os.path.join(root, file_name)
                # Calculate the relative path, important for preserving folder structure in Blob Storage
                relative_path = os.path.relpath(local_file_path, local_folder_path)
                # Construct the full blob name, combining the destination folder and relative path
                blob_name = destination_path + relative_path.replace("\\", "/")  # Ensure forward slashes in blob names
                print(blob_name)
                # 5.1 Upload each file
                with open(local_file_path, "rb") as data:
                    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                    blob_client.upload_blob(data, overwrite=True)
                logging.info(f"Uploaded '{local_file_path}' to '{blob_name}'")
                print(f"Uploaded '{local_file_path}' to '{blob_name}'")


    except Exception as e:
        logging.error(f"Error uploading folder:{local_folder_path} to datastore:{datastore_name}")
        print(f"Error uploading folder:{local_folder_path} to datastore:{datastore_name}")
        return None
    print("upload complete")
    

    # 5. Create Data Asset (Optional)
    if create_data_asset:
        # Input Validation
        if not data_asset_name:
            raise ValueError("data-asset-name should be provided when create-asset = True")
        if not data_asset_version:
            raise ValueError("data-asset-version should be provided when create-asset = True")
        
        # 5.1 Check if data-asset referencing same folder already exists
        # Construct the datastore-relative path
        datastore_path = f"azureml://datastores/{datastore_name}/paths/{destination_path}"
        
        # List all data assets in the workspace
        all_data_assets = ml_client.data.list()  # type: List[Data]

        for data_asset in all_data_assets:
            # Check if the data asset's path matches the datastore path
            if data_asset.path == datastore_path:
                logging.warning(f"Data Asset {data_asset.name} referencing same folder already exists")
        
        
        # 5.2 Check if data-asset already exists
        logging.info(f"Checking for data asset: Name={data_asset_name}, Version={data_asset_version}")
        try:
            # Attempt to get the data asset by name and version.
            # If it doesn't exist, this will raise an error.
            ml_client.data.get(name=data_asset_name, version=data_asset_version)
            logging.warning(f"Data asset '{data_asset_name}', version '{data_asset_version}' exists.")
            print(f"Data asset '{data_asset_name}', version '{data_asset_version}' already exists.")
            return True
        except Exception:
            my_data = Data(
                path=f"azureml://datastores/{datastore.name}/paths/{destination_path}",  # Use datastore-relative path
                type=AssetTypes.URI_FOLDER,  # Or AssetType.URI_FILE, depending on what you uploaded
                name=data_asset_name,
                description=f"Data asset created from folder {local_folder_path} in datastore {datastore_name}",
                version=data_asset_version,
            )
            data_asset = ml_client.data.create_or_update(my_data)
            logging.info(f"Created data asset: {data_asset.name}, version: {data_asset.version}")
            print(f"Created data asset: {data_asset.name}, version: {data_asset.version}")
            return True
    else:
        return True


def create_batches(files: List[str],
                   file_extensions: Union[List[str], str] = ['pdf', 'docx'],
                   batch_size: int = 20 , processing_folder:str = None) -> Dict[str, pd.DataFrame]:
    """
    Organizes files into a dictionary of pandas DataFrames, categorized by file type,
    and assigns batch numbers to each file within each DataFrame.

    Args:
        files (List[str]): A list of file paths.
        file_extensions (Union[List[str], str], optional): A list of file extensions to process
            (e.g., ['pdf', 'docx']) or "*" to process all files.
            Defaults to ['pdf', 'docx'].
        batch_size (int, optional): The number of files per batch. Defaults to 20.
        processing_folder (str): Save the batch files in a folder, Defualt to None (will use current working dir)

    Returns:
        Dict[str, pd.DataFrame]: A dictionary where keys are file types (e.g., 'pdf', 'docx', 'imagepdf')
            and values are pandas DataFrames.  Each DataFrame contains file information
            ('filepath', 'filename', 'page_count', 'ImagePDF', 'batch') for files of that type.
            Returns an empty dictionary if no files match the criteria.

    Raises:
        TypeError: If 'files' is not a list.
        ValueError: If 'file_extensions' is not a list, string, or empty list.
    """
    # Input validation
    if not isinstance(files, list):
        raise TypeError(f"Expected 'files' to be a list, got {type(files)}")
    if not isinstance(file_extensions, (list, str)):
        raise TypeError(f"Expected 'file_extensions' to be a list or str, got {type(file_extensions)}")
    if isinstance(file_extensions, list) and not file_extensions:
        raise ValueError("'file_extensions' cannot be an empty list")

    file_list: Dict[str, List[str]] = {}  # Use type hinting for clarity

    if file_extensions == "*":
        logging.info(f"Processing all file types. Total file count: {len(files)}")
        file_list = {'allfiles': files}  # Store all files under the key 'allfiles'
    else:
        if isinstance(file_extensions, str):
            file_extensions = [file_extensions] # Convert to list for consistent handling

        for extension in file_extensions:
            file_list[extension] = []  # Initialize an empty list for each extension
            for file in files:
                _, f_extension = os.path.splitext(file)
                if f_extension[1:].lower() == extension.lower():  # Case-insensitive comparison
                    file_list[extension].append(file)

    # Prepare data for DataFrame creation
    keys = [k for k, v in file_list.items() for _ in v]  # Efficient flattening
    values = [v for sublist in file_list.values() for v in sublist]  # More readable flattening

    df_files = pd.DataFrame({'filetype': keys, 'filepath': values})
    if df_files.empty:
        logging.warning("No files matched the specified file extensions.")
        return {} # Return empty dict

    logging.info(f"Created DataFrame with {len(df_files)} files.")

    # Get page count and ImagePDF status
    df_files['page_count'] = df_files.filepath.apply(lambda x: utils.get_page_count(x))
    df_files['ImagePDF'] = df_files.apply(
        lambda x: utils.check_if_imagepdf(x['filepath']) if x['filetype'] == 'pdf' else False,
        axis=1
    )
    df_files['filename'] = df_files.filepath.apply(os.path.basename)

    file_list_dfs: Dict[str, pd.DataFrame] = {} # Use a different name to avoid shadowing
    for filetype in file_extensions:
        if filetype == 'pdf':
            # Separate image and normal pdf, sort, and reset index
            condition_pdf_image = df_files[(df_files['filetype'] == 'pdf') & (df_files['ImagePDF'] == True)]\
                .sort_values(by='filename', ignore_index=True)  # Use ignore_index for efficiency
            file_list_dfs['imagepdf'] = condition_pdf_image

            condition_pdf_text = df_files[(df_files['filetype'] == 'pdf') & (df_files['ImagePDF'] == False)]\
                .sort_values(by='filename', ignore_index=True)
            file_list_dfs['pdf'] = condition_pdf_text
        else:
            file_list_dfs[filetype] = df_files[(df_files['filetype'] == filetype)]\
                .sort_values(by='filename', ignore_index=True)

    # Assign batches
    for key, df in file_list_dfs.items():
        batches = [(index // batch_size) + 1 for index in df.index] # use list comprehension
        df['batch'] = batches
        logging.info(f"Assigned batches to {key} files.  Total {key} file count: {len(df)}.")

        # Save to JSON if save_path is provided
        if processing_folder:
            # Ensure the directory exists
            try:
                os.makedirs(processing_folder, exist_ok=True)  # Create directory if it doesn't exist
            except OSError as e:
                raise OSError(f"Error creating directory {processing_folder}: {e}") from e

            json_filename = os.path.join(processing_folder, f"{key}_files.json")
            df.to_json(json_filename, orient="records", indent=4)  # Save as JSON
            logging.info(f"Saved DataFrame for {key} to {json_filename}")
        elif processing_folder is None: # added condition
            json_filename = f"{key}_files.json"
            df.to_json(json_filename, orient="records", indent=4)  # Save as JSON
            logging.info(f"Saved DataFrame for {key} to {json_filename}")
        
    return file_list_dfs



def read_json_files_to_dfs(folder_path: str,
                           encoding: str = 'utf-8',
                           errors: str = 'strict') -> List[pd.DataFrame]:
    """
    Reads all JSON files in the specified folder and returns a list of pandas DataFrames.

    Args:
        folder_path (str): The path to the folder containing the JSON files.
        encoding (str, optional): The encoding to use when reading the JSON files.
            Defaults to 'utf-8'.
        errors (str, optional): How to handle encoding errors.  Options are:
            'strict' - raise an exception,
            'ignore' - ignore errors,
            'replace' - replace with a replacement character.
            Defaults to 'strict'.

    Returns:
        List[pd.DataFrame]: A list of pandas DataFrames, one for each JSON file
        successfully read. Returns an empty list if no JSON files are found
        or if an error occurs during folder processing.

    Raises:
        TypeError: If `folder_path` is not a string.
        NotADirectoryError: If `folder_path` is not a directory.
        OSError: If there is an issue reading a JSON file.
        UnicodeDecodeError: If there is an issue decoding a file with the specified encoding.
    """
    if not isinstance(folder_path, str):
        raise TypeError(f"folder_path must be a string, not {type(folder_path)}")
    if not os.path.isdir(folder_path):
        raise NotADirectoryError(f"folder_path '{folder_path}' is not a valid directory")
    if not isinstance(encoding, str):
        raise TypeError(f"encoding must be a string, not {type(encoding)}")
    if not isinstance(errors, str):
        raise TypeError(f"errors must be a string, not {type(errors)}")
    if errors not in ['strict', 'ignore', 'replace']:
        raise ValueError(f"errors must be one of 'strict', 'ignore', or 'replace', not '{errors}'")

    dfs = {} # Use type hinting
    for filename in os.listdir(folder_path):
        if filename.lower().endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            try:
                # Read JSON file into a DataFrame
                df = pd.read_json(file_path, encoding=encoding, encoding_errors=errors)
                dfs[filename.split("_")[0]] = df
            except OSError as e:
                # Catch general OS errors during file reading (e.g., file not found, permission error)
                print(f"Error reading JSON file '{file_path}': {e}")
                # Consider logging the error with the logging module, instead of printing.
                #  logging.error(f"Error reading JSON file '{file_path}': {e}")
                continue  # Skip to the next file
            except UnicodeDecodeError as e:
                # Handle encoding errors specifically
                print(f"UnicodeDecodeError reading '{file_path}': {e}")
                continue
            except Exception as e:
                # Catch any other exceptions during JSON parsing
                print(f"Error parsing JSON file '{file_path}': {e}")
                continue
         

    return dfs


def azure_process_batch(
    df: pd.DataFrame,
    batch_val: int,
    filetype: str,
    storage_account_key: str,
    datastore_name: str,
    local_folder_path: str,
    destination_path: str,
    data_asset_name: str,
    data_asset_version: str,
    processing_folder:str,
    create_data_asset: bool = True,
    
) -> bool:
    """
    Processes a batch of files, uploads them to Azure Blob Storage, and updates
    the DataFrame with processing and upload status.

    Args:
        df (pd.DataFrame): DataFrame containing file information, including 'filepath',
            'filename', and 'batch' columns.  The DataFrame is modified in place.
        batch_val (int): The batch number to process.
        filetype (str): The type of files being processed (e.g., 'pdf', 'docx', 'imagepdf').
        storage_account_key (str): The key for the Azure Storage account.
        datastore_name (str): The name of the Azure datastore.
        local_folder_path (str): The local path where files are stored.
        destination_path (str): The destination path in Azure Blob Storage.
        data_asset_name (str): The name of the data asset.
        data_asset_version (str): The version of the data asset.
        create_data_asset (bool, optional): Whether to create a data asset. Defaults to True.
        processing_folder (str): Path to folder to save jsonfile with meta-info

    Returns:
        bool: True if the batch was processed and uploaded successfully, False otherwise.
              Returns False if doclingserver processing fails.

    Raises:
        ValueError: If required arguments are missing or invalid.
        Exception:  For errors during doclingserver processing or Azure upload.  The
                    function attempts to catch and log specific exceptions, but may
                    raise others.
    """
    # Input Validation (more comprehensive)
    if not isinstance(df, pd.DataFrame):
        raise ValueError("df must be a pandas DataFrame")
    if not all(col in df for col in ['filepath', 'filename', 'batch']):
        raise ValueError("df must contain 'filepath', 'filename', and 'batch' columns")
    # if not isinstance(batch_val, str):
    #     raise ValueError("batch_val must be an str")
    if not isinstance(filetype, str):
        raise ValueError("filetype must be a string")
    if not isinstance(storage_account_key, str):
        raise ValueError("storage_account_key must be a string")
    if not isinstance(datastore_name, str):
        raise ValueError("datastore_name must be a string")
    if not isinstance(local_folder_path, str):
        raise ValueError("local_folder_path must be a string")
    if not isinstance(destination_path, str):
        raise ValueError("destination_path must be a string")
    if not isinstance(data_asset_name, str):
        raise ValueError("data_asset_name must be a string")
    if not isinstance(data_asset_version, str):
        raise ValueError("data_asset_version must be a string")
    if not isinstance(create_data_asset, bool):
        raise ValueError("create_data_asset must be a boolean")
    if not isinstance (processing_folder, str):
        raise ValueError("processing_folder must be a string")

    logging.info(f"Processing batch {batch_val} for filetype {filetype}")

    batch_df = df[df.batch == batch_val].copy()  # Create a copy to avoid modifying the original slice
    batch_df['filename_without_extension'] = batch_df.filename.apply(lambda x: os.path.splitext(x)[0])
    batch_df['processed'] = False
    batch_df['uploaded'] = False

    if filetype != 'imagepdf':
        try:
            
            success_count, partial_success_count, failure_count, folder_info = doclingserver.batch_processing(
                file_list=batch_df.filepath.tolist(),
                output_dir=f"{local_folder_path}{filetype}/{batch_val}/")
            logging.info(
                f"doclingserver processing: success={success_count}, partial={partial_success_count}, failure={failure_count}"
            )
        except Exception as e:
            logging.error(f"Error in doclingserver.batch_processing: {e}")
            return False  # Return False on doclingserver failure

        try:
            check_upload = upload_folder_to_datastore(
                datastore_name=datastore_name,
                storage_account_key=storage_account_key,
                local_folder_path=f"{local_folder_path}{filetype}/{batch_val}/", 
                destination_path=f"{destination_path}{filetype}/", 
                create_data_asset=create_data_asset,
                data_asset_name=data_asset_name,
                data_asset_version=data_asset_version,
            )
        except Exception as e:
            logging.error(f"Error in upload_folder_to_datastore: {e}")
            return False  # Return False on upload failure

        if check_upload:
            for f in folder_info:
                filename_without_extension = os.path.basename(f)
                matching_rows = batch_df[batch_df['filename_without_extension'] == filename_without_extension]
                if not matching_rows.empty:
                    index_val = matching_rows.index[0]
                    # Use .loc to modify the original DataFrame
                    df.loc[index_val, 'processed'] = True
                    df.loc[index_val, 'uploaded'] = True

                    json_filename = os.path.join(processing_folder, f"files_info/{filetype}_files.json")
                    try:
                        df.to_json(json_filename, orient="records", indent=4)
                        logging.info(f"Saved file info to {json_filename}")
                    except Exception as e:
                        logging.error(f"Error saving JSON: {e}") # keep going even if JSON save fails
            logging.info(f"Finished batch {batch_val}")
            print(f"Finished batch {batch_val}")
            return True
        else:
            return False # upload failed
    else: #filetype == 'imagepdf'
        for file in batch_df.filepath.tolist():
            try:
                doclingoutput,filename = doclingserver.useOCR(file)
                folder_loc,tables_folder = doclingserver.save_output(doclingoutput, f"{local_folder_path}{filetype}/{batch_val}/", filename)
            except Exception as e:
                logging.error(f"Error in doclingserver.useOCR or save_output: {e}")
                return False
            try:
                check_upload = upload_folder_to_datastore(
                    datastore_name=datastore_name,
                    storage_account_key=storage_account_key,
                    local_folder_path=f"{local_folder_path}{filetype}/{batch_val}/", 
                    destination_path=f"{destination_path}{filetype}/", 
                    create_data_asset=create_data_asset,
                    data_asset_name=data_asset_name,
                    data_asset_version=data_asset_version,
                )
            except Exception as e:
                logging.error(f"Error in upload_folder_to_datastore: {e}")
                return False  # Return False on upload failure
            if check_upload:
                filename_without_extension = os.path.basename(filename)
                matching_rows = batch_df[batch_df['filename_without_extension'] == filename_without_extension]
                if not matching_rows.empty:
                    index_val = matching_rows.index[0]
                    # Use .loc to modify the original DataFrame
                    df.loc[index_val, 'processed'] = True
                    df.loc[index_val, 'uploaded'] = True

                    json_filename = os.path.join(processing_folder, f"files_info/{filetype}_files.json")
                    try:
                        df.to_json(json_filename, orient="records", indent=4)
                        logging.info(f"Saved file info to {json_filename}")
                    except Exception as e:
                        logging.error(f"Error saving JSON: {e}") # keep going even if JSON save fails
        logging.info(f"Finished batch {batch_val}")
        print(f"Finished batch {batch_val}")     
        return True
    

def batch_handler(
    processing_folder: str,
    storage_account_key: str,
    datastore_name: str,
    local_folder_path: str,
    destination_path: str,
    data_asset_name: str,
    data_asset_version: str,
    create_data_asset: bool = True,
) -> None:
    """
    Handles the processing of batches of files for different file types.  It reads file
    information from JSON files, iterates through batches, calls the azure_process_batch
    function, and logs the processed batches.

    Args:
        processing_folder (str): The path to the folder containing the 'files_info'
            subdirectory with JSON files and where batch logs will be saved.
        storage_account_key (str): The key for the Azure Storage account.
        datastore_name (str): The name of the Azure datastore.
        local_folder_path (str): The local path where files are stored.
        destination_path (str): The destination path in Azure Blob Storage.
        data_asset_name (str): The name of the data asset.
        data_asset_version (str): The version of the data asset.
        create_data_asset (bool, optional): Whether to create a data asset. Defaults to True.

    Raises:
        ValueError: If required arguments are missing or invalid.
        FileNotFoundError: If the 'files_info' directory does not exist.
        OSError: If there is an error creating the batch log directory.
        Exception: Any exceptions raised by called functions (e.g., read_json_files_to_dfs, azure_process_batch).
    """
    # Input Validation
    if not isinstance(processing_folder, str):
        raise ValueError("processing_folder must be a string")
    if not isinstance(storage_account_key, str):
        raise ValueError("storage_account_key must be a string")
    if not isinstance(datastore_name, str):
        raise ValueError("datastore_name must be a string")
    if not isinstance(local_folder_path, str):
        raise ValueError("local_folder_path must be a string")
    if not isinstance(destination_path, str):
        raise ValueError("destination_path must be a string")
    if not isinstance(data_asset_name, str):
        raise ValueError("data_asset_name must be a string")
    if not isinstance(data_asset_version, str):
        raise ValueError("data_asset_version must be a string")
    if not isinstance(create_data_asset, bool):
        raise ValueError("create_data_asset must be a boolean")

    files_info_path = processing_folder+"files_info/"
    if not os.path.exists(files_info_path):
        raise FileNotFoundError(f"Directory 'files_info' not found at {files_info_path}")

    files_df = read_json_files_to_dfs(files_info_path)  # Changed variable name

    # batch_logs_path = os.path.join(processing_folder, "files_info", "batch_logger", "batch_logs.json")
    if os.path.exists(processing_folder+"files_info/batch_logger/batch_logs.json"):
        batch_logs = pd.read_json(processing_folder+"files_info/batch_logger/batch_logs.json")
    else:
        batch_logs = pd.DataFrame(columns=['filetype', 'batch'])

    for filetype in files_df.keys():  
        logging.info(f"Processing {filetype} files")
        print(f"Processing {filetype} files")
        batches = files_df[filetype].batch.unique()

        for batch_val in batches:
            if batch_val in list(batch_logs[batch_logs.filetype == filetype].batch): # use .values
                logging.info(f"Batch {batch_val} for {filetype} already processed, skipping.")
                print(f"Batch {batch_val}  for {filetype} already processed , skipping")
                check = False
                continue  # Use continue for clarity

            else:
                logging.info(f"Starting batch {batch_val} for {filetype}")
                print(f"Starting batch {batch_val}")
                df = files_df[filetype]

                check = azure_process_batch(
                    df=df,
                    batch_val=batch_val,
                    filetype=filetype,
                    datastore_name=datastore_name,
                    storage_account_key=storage_account_key,
                    local_folder_path=local_folder_path,
                    destination_path=destination_path,
                    create_data_asset=create_data_asset,
                    data_asset_name=data_asset_name,
                    processing_folder=processing_folder,  # Pass processing_folder
                    data_asset_version=data_asset_version,
                )

                if check:
                    batch_logs.loc[len(batch_logs)] = [filetype, batch_val]
                    try:
                        os.makedirs(os.path.join(processing_folder, "files_info/batch_logger/"), exist_ok=True)
                        json_filename = os.path.join(processing_folder, f"files_info/batch_logger/batch_logs.json")
                        batch_logs.to_json(json_filename, orient="records", indent=4)
                        logging.info(f"Saved batch log to {json_filename}")
                    except OSError as e:
                        raise OSError(f"Error saving batch logs to {json_filename}: {e}") from e
                else:
                    logging.error(f"Processing batch {batch_val} for {filetype} failed.")
                    print(f"Processing batch {batch_val} for {filetype} failed.")
                    # Decide if you want to continue processing other batches/filetypes
                    # or raise an exception here.  For now, I'll continue.
    logging.info("Batch processing completed.")
    print("Batch processing completed.")