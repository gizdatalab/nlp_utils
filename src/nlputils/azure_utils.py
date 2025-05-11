import os
import logging
from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential
from azureml.fsspec import AzureMachineLearningFileSystem
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from typing import Optional, Dict, List, Any
from azure.ai.ml.entities import Data
from azure.ai.ml.constants import AssetTypes


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
        datastore_name (str): The name of the datastore to upload to.
        storage_account_key (str): storage account acccess key of storage account to which datastore point
        local_folder_path (str): The path to the local folder in the workspace's file system.
        destination_path (str): The path within the datastore where the folder will be uploaded. ex: "processed/
        create_data_asset (bool, optional): Whether to create a data asset after uploading. Defaults to False.
        data_asset_name (Optional[str], optional): The name of the data asset to create. Required if create_data_asset is True. Defaults to None.
        data_asset_version (Optional[str], optional): The version of the data asset.  Defaults to None.
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

