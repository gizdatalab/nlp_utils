import os
import logging
from requests import post, get
import json
import pandas as pd
from ast import literal_eval
from io import StringIO
import logging
from typing import Callable, Dict, List, Optional, Text, Tuple, Union, Literal
import glob
import time
import numpy as np
import re
import docker
from ....nlputils.utils import check_if_imagepdf, get_config, get_files, open_file, get_page_count
server_config='../axaserver/defaultConfig.json'
this_dir, this_filename = os.path.split(__file__)
server_config = os.path.join(this_dir, "defaultConfig.json")

def check_input_file(file_path:str)->bool:
    """
    check for if file type supported by axaparsr
    """
    supported_filetype = ['.pdf', '.jpg', '.jpeg', '.png', '.tiff',
                        '.tif', '.docx', '.xml', '.eml', '.json']
    
    document_name, extension_type = os.path.splitext(os.path.basename(file_path))
    
    if extension_type in supported_filetype:
        if os.path.isfile(file_path):
            if (extension_type == '.pdf') & check_if_imagepdf(file_path):
                logging.warning(f"""{file_path} is of type imagepdf and using inbuilt 
                        tesseract-ocr""")
            return True
        else:
            logging.error("File not found")
            return False
    else:
        logging.error(f"{file_path} filetype not supported, should be of {supported_filetype}") 
        return False


def send_doc(url="http://localhost:3001", 
            file_path :str ="", 
            server_config:str=server_config, 
            authfile:str = "")->dict:

    """
    Make the post request to axaserver listening port:3001

    Params
    -------------
    - url: either the localhost or huggingface hosted server acceptible right now
    - file_path: file to be sent to server
    - server_config:filepath to configs for the axaserver to be used for the this document
    - authfile: private server on huggingface need auth-token

    Return
    --------------
    - filename: name of file sent to server
    - config: config file used by server to process the input file
    - status_code: status of the post request made to server
    - server_response: request-id(unique) of the document processing request made to server

    """
    # we need it if using the private server on HF
    if url != "http://localhost:3001":
        configs = get_config(configfile_path= authfile)
        try:
            url = configs.get("axaserver","api")
            token = configs.get("axaserver","token")
            headers = {
                        "Authorization": f"Bearer {token}"}
        except Exception as e:
            logging.warning(e)
    else:
        headers = None
    if check_input_file(file_path):
        try:
            packet = {
            'file': (file_path, open(file_path, 'rb'), 'application/pdf'),
            'config': (server_config, open(server_config, 'rb'), 'application/json')
            }
            r = post(url + "/api/v1/document", headers=headers, files=packet)
            return {
            'filename': os.path.basename(file_path),
            'config': server_config,
            'status_code': r.status_code,
            'server_response': r.text}
        except Exception as e:
            logging.error(e)
            return {
            'filename': os.path.basename(file_path),
            'config': server_config,
            'status_code': 403,
            'server_response': None}


def get_status(url: str = "http://localhost:3001",
               request_id: str = "", authfile:str = "")->dict:
    """Get the status of a particular request using its ID

    Params
    ------------------
    - url:  either the localhost or huggingface hosted server acceptible right now
    - request_id: The ID of the request to be queried
    - authfile: private server on huggingface need auth-token

    Return
    ------------------
    requests status

    """
    if url != "http://localhost:3001":
        configs = get_config(configfile_path= authfile)
        try:
            url = configs.get("axaserver","api")
            token = configs.get("axaserver","token")
            headers = {
                        "Authorization": f"Bearer {token}"}
        except Exception as e:
            logging.warning(e) 
    else:
        headers = None
    
    if request_id == "":
        raise Exception('No request ID provided')

    r = get('{}/api/v1/queue/{}'.format(url, request_id), headers=headers)

    return r


def get_json(url: str = "http://localhost:3001",
               request_id: str = "", authfile:str = "")->dict:
    """Get the standard json output (of axaparsr) for request ID

    Params
    ---------------
    - url: url of the server 
    - request_id: The ID of the request to be queried with the server
    - authfile: The server address where the query is to be made

    Return
    ----------------
    if json output is ready will send the raw json output or else dict with key values 
    (request_id, server_status)

    """
    if url != "http://localhost:3001":
        configs = get_config(configfile_path= authfile)
        try:
            url = configs.get("axaserver","api")
            token = configs.get("axaserver","token")
            headers = {
                        "Authorization": f"Bearer {token}"}
        except Exception as e:
            logging.warning(e) 
    else:
        headers = None
    
    if request_id == "":
        raise Exception('No request ID provided')
    
    # get the server status on the request id before we make the call
    server_status = get_status(url=url,request_id=request_id,authfile=authfile).status_code
    if  server_status==201:
        r = get('{}/api/v1/json/{}'.format(url, request_id), headers=headers)
        if r.text != "":
            return r.json()
        else:
            return {'request_id': request_id, 'server_response': r.json()}
    else:
        return {'request_id': request_id, 'server_response': server_status}
    
  
def get_simplejson(url: str = "http://localhost:3001",
               request_id: str = "", authfile:str = "")->dict:
    """Get the simple-json output (of axaparsr) for request ID

    Params
    ---------------
    - url: url of the server 
    - request_id: The ID of the request to be queried with the server
    - authfile: The server address where the query is to be made

    Return
    ----------------
    if simple-json output is ready will send the raw json output or else dict with key values 
    (request_id, server_status)

    """
    if url != "http://localhost:3001":
        configs = get_config(configfile_path= authfile)
        try:
            url = configs.get("axaserver","api")
            token = configs.get("axaserver","token")
            headers = {
                        "Authorization": f"Bearer {token}"}
        except Exception as e:
            logging.warning(e) 
    else:
        headers = None
    
    if request_id == "":
        raise Exception('No request ID provided')
    
    # get the server status on the request id before we make the call
    server_status = get_status(url=url,request_id=request_id,authfile=authfile).status_code
    if  server_status==201:
        r = get('{}/api/v1/simple-json/{}'.format(url, request_id), headers=headers)
        if r.text != "":

            return r.json()
        else:
            return {'request_id': request_id, 'server_response': r.json()}
    else:
        return {'request_id': request_id, 'server_response': server_status}
    
    
def get_markdown(url: str = "http://localhost:3001",
               request_id: str = "", authfile:str = "")->dict:
    """Get the markdown output (of axaparsr) for request ID

    Params
    ---------------
    - url: url of the server 
    - request_id: The ID of the request to be queried with the server
    - authfile: The server address where the query is to be made

    Return
    ----------------
    if markdown output is ready will send the raw markdown output or else dict with key values 
    (request_id, server_status)

    """
    if url != "http://localhost:3001":
        configs = get_config(configfile_path= authfile)
        try:
            url = configs.get("axaserver","api")
            token = configs.get("axaserver","token")
            headers = {
                        "Authorization": f"Bearer {token}"}
        except Exception as e:
            logging.warning(e) 
    else:
        headers = None
    
    if request_id == "":
        raise Exception('No request ID provided')
    
    # get the server status on the request id before we make the call
    server_status = get_status(url=url,request_id=request_id,authfile=authfile).status_code
    if  server_status==201:
        r = get('{}/api/v1/markdown/{}'.format(url, request_id), headers=headers)
        if r.text != "":
            return r.text
        else:
            return {'request_id': request_id, 'server_response': r.text}
    else:
        return {'request_id': request_id, 'server_response': server_status}    
    

def get_text(url: str = "http://localhost:3001",
               request_id: str = "", authfile:str = "")->dict:
    """Get the raw text output (of axaparsr) for request ID

    Params
    ---------------
    - url: url of the server 
    - request_id: The ID of the request to be queried with the server
    - authfile: The server address where the query is to be made

    Return
    ----------------
    if text output is ready will send the raw text output or else dict with key values 
    (request_id, server_status)

    """
    if url != "http://localhost:3001":
        configs = get_config(configfile_path= authfile)
        try:
            url = configs.get("axaserver","api")
            token = configs.get("axaserver","token")
            headers = {
                        "Authorization": f"Bearer {token}"}
        except Exception as e:
            logging.warning(e) 
    else:
        headers = None
    
    if request_id == "":
        raise Exception('No request ID provided')
    
    # get the server status on the request id before we make the call
    server_status = get_status(url=url,request_id=request_id,authfile=authfile).status_code
    if  server_status==201:
        r = get('{}/api/v1/text/{}'.format(url, request_id), headers=headers)
        if r.text != "":
            return r.text
        else:
            return {'request_id': request_id, 'server_response': r.text}
    else:
        return {'request_id': request_id, 'server_response': server_status}      
    

def get_tables_list(url: str = "http://localhost:3001",
               request_id: str = "", authfile:str = "")->dict|list:
    """Get the tables list for request ID

    Params
    ---------------
    - url: url of the server 
    - request_id: The ID of the request to be queried with the server
    - authfile: The server address where the query is to be made

    Return
    ----------------
    if output is ready will send the list of tuple(page number, table number) or else dict with key values 
    (request_id, server_status)

    """

    if url != "http://localhost:3001":
        configs = get_config(configfile_path= authfile)
        try:
            url = configs.get("axaserver","api")
            token = configs.get("axaserver","token")
            headers = {
                        "Authorization": f"Bearer {token}"}
        except Exception as e:
            logging.warning(e) 
    else:
        headers = None
    
    if request_id == "":
        raise Exception('No request ID provided')
    
    # get the server status on the request id before we make the call
    server_status = get_status(url=url,request_id=request_id,authfile=authfile).status_code
    if  server_status==201:
        r = get('{}/api/v1/csv/{}'.format(url, request_id), headers=headers)
        
        if r.text != "":
            return [(table.rsplit('/')[-2], table.rsplit('/')[-1])
                    for table in literal_eval(r.text)]
        else:
            return {'request_id': request_id, 'server_response': r.text}
    else:
        return {'request_id': request_id, 'server_response': server_status}
    

def get_table(url: str = "http://localhost:3001",
               request_id: str = "", authfile:str = "",
               page=None,table=None,seperator=";",
               column_names: list = None):
    """ Get a particular table for a request id

    Params
    ---------------
    - request_id: The request to be queried to get a document.
    - page: The page number on which the queried table exists.
    - table: The table number to be fetched.
    - seperator: The seperator to be used between table cells (default ';')
    - url: The server address which is to be queried.
    - column_names: The headings of the table searched (column titles)

    Return
    ----------------
    if output is ready will send the table as dataframe or else dict with key values 
    (request_id, server_status)

    """
    if url != "http://localhost:3001":
        configs = get_config(configfile_path= authfile)
        try:
            url = configs.get("axaserver","api")
            token = configs.get("axaserver","token")
            headers = {
                        "Authorization": f"Bearer {token}"}
        except Exception as e:
            logging.warning(e) 
    else:
        headers = None
    
    if request_id == "":
        raise Exception('No request ID provided')
    
    # get the server status on the request id before we make the call
    server_status = get_status(url=url,request_id=request_id,authfile=authfile).status_code
    if  server_status==201:
        if page is None or table is None:
            raise Exception('No Page or Table number provided')
        else:
            r = get('{}/api/v1/csv/{}/{}/{}'.format(url,request_id,page,table),headers=headers)

            if r.text != "":
                try:
                    df = pd.read_csv(
                        StringIO(
                            r.text),
                        sep=seperator,
                        names=column_names)
                    df.loc[:, ~df.columns.str.match('Unnamed')]
                    df = df.where((pd.notnull(df)), " ")
                    return df
                except Exception:
                    return r.text
            else:
                return r.text
    else:
        return {'request_id': request_id, 'server_response': server_status}


def send_documents_batch(batch: list,
            url="http://localhost:3001", 
            server_config:str=server_config, 
            authfile:str = "",
           ) -> list:
        """
        takes a batchfile i.e list of documents(filepaths) and send thems to server
        and returns back the status and id of each (in a batch) request made to server.

        Params
        -------------
        - batch: list of documents(filepaths) to be sent to server
        - url: either the localhost or huggingface hosted server acceptible right now
        - server_config:filepath to configs for the axaserver to be sued for this 
                      batch of files
        - authfile: private server on huggingface need auth-token

        Return
        --------------
        - responses: list of dict with dictionary containing
        {filename: name of file sent to server, config: config file used by server to process the input file,
        status_code: status of the post request made to server,
        server_response: request-id(unique) of the document processing request made to server}


        """
        if url != "http://localhost:3001":
            configs = get_config(configfile_path= authfile)
            try:
                url = configs.get("axaserver","api")
                token = configs.get("axaserver","token")
                headers = {
                            "Authorization": f"Bearer {token}"}
            except Exception as e:
                logging.warning(e)
                return
        else:
            headers = None
        responses = []
        
        # send files in batch to server
        for file in batch:
            responses.append({**send_doc(url=url,file_path=file,server_config=server_config,
                                      authfile=authfile),**{'file_path':file}})
        return responses


def download_files(request_id, folder_location, filename,authfile= ""):

    if authfile =="":
        if get_status(request_id=request_id).status_code == 201:
            if not os.path.exists(folder_location + f"{request_id}/"):
                os.makedirs(folder_location + f"{request_id}/")
                new_path = folder_location + f"{request_id}/"

                try:

                    r_markdown = get_markdown(request_id=request_id)
                    with open(new_path + f'{filename}.md', 'w') as file:
                        file.write(r_markdown)
                    
                    r_text = get_text(request_id=request_id)
                    with open(new_path+f'{filename}.txt', 'w') as file:
                        file.write(r_text)
                    
                    r_json = get_json(request_id=request_id)
                    with open(new_path + f'{filename}.json', 'w') as file:
                        json.dump(r_json, file)  

                    tables_list = get_tables_list(request_id=request_id)
                    os.makedirs(folder_location + f"{request_id}/tables/")
                    new_path_table = folder_location + f"{request_id}/tables/"
                    for val in tables_list:
                        df = get_table(request_id=request_id, page=val[0], table=val[1])
                        df.to_csv(new_path_table+f"{val[0]}_{val[1]}.csv")
                    try:
                        r_simplejson = get_simplejson(request_id=request_id)
                        with open(new_path+f'{filename}.simple.json', 'w') as file:
                            json.dump(r_simplejson, file)
                    except Exception as e:
                        logging.error(e)
                    
                    return new_path
                except Exception as e:
                    logging.error(e)

                
            
            else: logging.warning("folder already exists")
        
    else:
        if get_status(request_id=request_id,url="",authfile=authfile).status_code == 201:
            if not os.path.exists(folder_location + f"{request_id}/"):
                os.makedirs(folder_location + f"{request_id}/")
                new_path = folder_location + f"{request_id}/"

                try:

                    r_markdown = get_markdown(request_id=request_id,url="",authfile=authfile)
                    with open(new_path + f'{filename}.md', 'w') as file:
                        file.write(r_markdown)
                    
                    r_text = get_text(request_id=request_id,url="",authfile=authfile)
                    with open(new_path+f'{filename}.txt', 'w') as file:
                        file.write(r_text)
                    
                    r_json = get_json(request_id=request_id,url="",authfile=authfile)
                    with open(new_path + f'{filename}.json', 'w') as file:
                        json.dump(r_json, file)  

                    tables_list = get_tables_list(request_id=request_id,url="",authfile=authfile)
                    os.makedirs(folder_location + f"{request_id}/tables/")
                    new_path_table = folder_location + f"{request_id}/tables/"
                    for val in tables_list:
                        df = get_table(request_id=request_id,url="",authfile=authfile, page=val[0], table=val[1])
                        df.to_csv(new_path_table+f"{val[0]}_{val[1]}.csv")
                    try:
                        r_simplejson = get_simplejson(request_id=request_id,url="",authfile=authfile)
                        with open(new_path+f'{filename}.simple.json', 'w') as file:
                            json.dump(r_simplejson, file)
                    except Exception as e:
                        logging.error(e)
                    return new_path
                except Exception as e:
                    logging.error(e)

                
            
            else: logging.warning("folder already exists")


def get_serverconfig(config_type:Literal['default','largepdf','minimal','reduced','ocr_reduced','ocr']):
    """
    get server config type depending on literal value passed 
    """
    config_list  = ['default','largepdf','minimal','reduced','ocr_reduced','ocr']
    config_map = {'default':os.path.join(this_dir, "defaultConfig.json"),
                  'largepdf':os.path.join(this_dir, "largePDFConfig.json"),
                  'minimal':os.path.join(this_dir, "minimalConfig.json"),
                  'reduced':os.path.join(this_dir, "reducedNoHeadingConfig.json"),
                  'ocr_reduced':os.path.join(this_dir, "OCRNoTableConfig.json"),
                  'ocr':os.path.join(this_dir, "OCRConfig.json")}

    if config_type not in config_list:
        logging.error(f"config acceptable values are {config_list}")
        return None
    else:
        server_file = config_map[config_type]
        return server_file


class axaBatchProcessingLocal:
    def __init__(self,container_id:str='',
                 config:Literal['default','ocr','largepdf','minimal','reduced','ocr_reduced']='default',
                 batch_files:list=[]):
        """
        Initialize axaBatchProcessingLocal with a list of documents and container id of 
        axaparsr to process the documents in semi-automated manner.

        Params
        -------------
        - container_id: ID of the container running axaparsr locally
        - config: axaparsr server config to be applied to the whole documents set
        - batch_files: list of all documents to be processed
        """
        
        logging.basicConfig(level=logging.DEBUG,
                            format='%(name)s - %(levelname)s - %(message)s')
        
        # initialize the class variables
        self.batch_files = None
        self.server_file = None
        self.container_id = None
        
        # check for which initial class var are provided by user
        # use user provided init params 
        if container_id == '':
            logging.error("Container ID required")
        else:
            self.container_id = container_id
        
        server_file = get_serverconfig(config)
        if server_file:
            self.server_file = server_file

        if len(batch_files) == 0:
            logging.error("pass the non-empty files list")
        else:
            self.batch_files = batch_files

    
    def set_batch_params(self, batch_size:int=5, batch_wait_time:int=300, dynamic_wait_time:bool = False,
                         dynamic_multiplier=9):
        """
        Set the parameters to be used for batch processing

        Params
        ---------------
        - batch_size: this will be size of inner small batches to process the list of 
                    documents the optimal size depends on memory and number of cores being
                    used by axaparsr
        - batch_wait_time: this will be wait time till inner batch will be allowed to be 
                    processed by axaparsr, till second batch is pushed 
                    (before the next batch is pushed, the container is restarted)
        """
        self.index_end = len(self.batch_files)
        self.batch_start = 0
        self.batch_size = batch_size
        self.batch_end = self.batch_start + self.batch_size
        self.batch_id = 0
        self.sleep_time = batch_wait_time
        self.dynamic_wait_time = dynamic_wait_time
        self.dynamic_multiplier = dynamic_multiplier
    
    
    def processing(self,save_to_folder:str = ''):
        """
        start the batch processing, will save all output to the 'save_to_folder'

        Params
        ------------
        - save_to_folder: the local of folder where to store all the outputs

        Return 
        -----------
        df: dataframe with info on each file
        """

        if not os.path.exists(save_to_folder):
            os.makedirs(save_to_folder)
        if not os.path.exists(save_to_folder+'tmp/'):
            os.makedirs(save_to_folder+'tmp/')
        
        # instantiate the docker container client
        try:
            client = docker.from_env()
        except Exception as e:
            logging.error("docker not activated")


        
        # loop thorugh the documents list, create the batch and process them
        while self.index_end > self.batch_start:
            if self.batch_end <=self.index_end:
                batch_file = self.batch_files[self.batch_start:self.batch_end]
            else:
                batch_file = self.batch_files[self.batch_start:]
            # send the batch to server
            batch = f"batch_{self.batch_id}"
            
            if os.path.isdir(f'{save_to_folder}tmp/{batch}'):
                logging.warning(f'{save_to_folder}tmp/{batch}  exists')
                self.batch_id +=1
                self.batch_start = self.batch_end
                self.batch_end = self.batch_start + self.batch_size
            else:
                batch_post = send_documents_batch(batch=batch_file,server_config=self.server_file)
                # create the dataframe of repsonses and save it as batch file
                # batch_id is auto-generated sequentially these batches will be saved in tmp folder
                # within the 'save_to_folder' directory
                df = pd.DataFrame(batch_post)
                jsonfile = df.to_json(orient="records")
                parsed = json.loads(jsonfile)
                with open(f'{save_to_folder}tmp/{batch}.json', 'w') as file:
                    json.dump(parsed, file, indent=4)

                # wait time for inner batch to be processed
                if self.dynamic_wait_time == False:
                    time.sleep(self.sleep_time)
                else:
                    page_count = max([get_page_count(f) for f in batch_file])
                    time.sleep(page_count*self.dynamic_multiplier)

                # get status code of succesfully accepted request
                df['status'] = df.apply(lambda x: get_status(request_id=x['server_response'])\
                                        .status_code if x['status_code']==202 else None,axis=1 )
                
                # download inner batch and save to tmp sub-dir in 'save_to_folder' location
                root_folder = f"{save_to_folder}tmp/{batch}/"
                df['path_to_docs'] = df.apply(lambda x: download_files(x['server_response'],root_folder,
                                            os.path.splitext(os.path.basename(x['filename']))[0]),axis=1)
                jsonfile = df.to_json(orient="records")
                parsed = json.loads(jsonfile)
                with open(f'{save_to_folder}tmp/{batch}.json', 'w') as file:
                    json.dump(parsed, file, indent=4)
        
                logging.info("batch",self.batch_id,"done")

                # restart the container to clear cache
                container = client.containers.get(self.container_id)
                container.stop()
                container.start()

                # increase the batch iteration value and update related values
                self.batch_id +=1
                self.batch_start = self.batch_end
                self.batch_end = self.batch_start + self.batch_size
                # sleep time to allow the container to be up and running
                time.sleep(20)
        logging.info("jobs completed")
        batch_files = glob.glob(save_to_folder+'tmp/*.json')
        df = pd.concat([pd.read_json(file) for file in batch_files], ignore_index=True)
        df = df.replace({float('nan'): None})
        df['simple_json_download_successful'] = df.apply(lambda x: 
                                                os.path.isfile(x['path_to_docs']+"/"+
                                                    os.path.splitext(x['filename'])[0] +".simple.json" ) 
                                                if x['path_to_docs'] is not None else False,
                                                axis=1)

        return df


class axaBatchProcessingHF:
    def __init__(self,authfile,
                 config:Literal['default','ocr','largepdf','minimal','reduced','ocr_reduced']='default',
                 batch_files:list=[]):
        """
        Initialize axaBatchProcessingLocal with a list of documents and container id of 
        axaparsr to process the documents in semi-automated manner.

        Params
        -------------
        - container_id: ID of the container running axaparsr locally
        - config: axaparsr server config to be applied to the whole documents set
        - batch_files: list of all documents to be processed
        """
        
        logging.basicConfig(level=logging.DEBUG,
                            format='%(name)s - %(levelname)s - %(message)s')
        
        # initialize the class variables
        self.batch_files = None
        self.server_file = None
        self.authfile = authfile
        self.df_placeholder = []
        self.current_batch = []
        

        if len(batch_files) == 0:
            logging.error("pass the non-empty files list")
        else:
            self.batch_files = batch_files
        
        server_file = get_serverconfig(config)
        if server_file:
            self.server_file = server_file
        
    
    def set_batch_params(self, batch_size:int=10, batch_wait_time:int=120):
        """
        Set the parameters to be used for batch processing

        Params
        ---------------
        - batch_size: this will be size of inner small batches to process the list of 
                    documents the optimal size depends on memory and number of cores being
                    used by axaparsr
        - batch_wait_time: this will be wait time till inner batch will be allowed to be 
                    processed by axaparsr, till second batch is pushed 
                    (before the next batch is pushed, the container is restarted)
        """
        self.batch_size = batch_size
        self.sleep_time = batch_wait_time
        
    
    def _get_batch_status(self,response_file):
        for f in response_file:
            server_status = get_status(url=None, authfile=self.authfile, request_id=f['server_response'])
            f['status'] = server_status.status_code
        return response_file

    def processing(self, save_to_folder:str = ''):
        if not os.path.exists(save_to_folder):
            os.makedirs(save_to_folder)
        if not os.path.exists(save_to_folder+'tmp/'):
            os.makedirs(save_to_folder+'tmp/')


        while self.batch_files:
        # Add files to the current batch until it's full or there are no more files to process
            while len(self.current_batch) < self.batch_size and self.batch_files:
                r = send_doc(url="",file_path=self.batch_files[0],server_config=self.server_file,
                                        authfile=self.authfile)
                r['file_path'] = self.batch_files[0]
                self.current_batch.append(r)
                self.batch_files.pop(0)

            # wait time 
            time.sleep(self.sleep_time)
            # Simulate processing of some files in the batch
            self.current_batch = self._get_batch_status(response_file=self.current_batch)
            for i,f in enumerate(self.current_batch):
                if f['status'] == 201:
                    self.current_batch[i]['path_to_docs'] = download_files(f['server_response'],save_to_folder + 'tmp/',
                                                    os.path.splitext(os.path.basename(f['filename']))[0], authfile=self.authfile)
                    self.df_placeholder.append(self.current_batch[i])
                    self.current_batch.pop(i)
            
            with open(save_to_folder + 'tmp/hf_batch_files.json', 'w') as file:
                json.dump(self.df_placeholder, file, indent=4)
                
            # Add more files to the batch if it's not full
            while len(self.current_batch) < self.batch_size and self.batch_files:
                r = send_doc(url="",file_path=self.batch_files[0],server_config=self.server_file,
                                            authfile=self.authfile)
                r['file_path'] = self.batch_files[0]
                self.current_batch.append(r)
                self.batch_files.pop(0)
            self.current_batch = self._get_batch_status(response_file=self.current_batch)
            with open(save_to_folder + 'tmp/current_batch.json', 'w') as file:
                json.dump(self.current_batch, file, indent=4)
        logging.info("total files processed:", len(self.df_placeholder))
        logging.info("waiting for last batch")

        while self.current_batch:
            self.current_batch = self._get_batch_status(response_file=self.current_batch)
            for i,f in enumerate(self.current_batch):
                if f['status'] == 201:
                    self.current_batch[i]['path_to_docs'] = download_files(f['server_response'],save_to_folder + 'tmp/',
                                                    os.path.splitext(os.path.basename(f['filename']))[0], authfile=self.authfile)
                    self.df_placeholder.append(self.current_batch[i])
                    self.current_batch.pop(i)
            with open(save_to_folder + 'tmp/current_batch.json', 'w') as file:
                json.dump(self.current_batch, file, indent=4)
            with open(save_to_folder + 'tmp/hf_batch_files.json', 'w') as file:
                json.dump(self.df_placeholder, file, indent=4) 
            time.sleep(self.sleep_time)
        
        logging.info("jobs completed")
        for f in self.df_placeholder:
            f['simple_json_download_successful'] = os.path.isfile(f['path_to_docs']+"/"+
                                            os.path.splitext(f['filename'])[0] +".simple.json" ) 

        with open(save_to_folder + 'tmp/hf_batch_files.json', 'w') as file:
            json.dump(self.df_placeholder, file, indent=4)   

        logging.info("jobs completed") 

        return pd.DataFrame(self.df_placeholder)
        

def create_axa_batches(df):
    """
    returns the batches based on page_count thresholds [page_count<=10, 10<page_count<=25,
    25<page_count<=50, 50<page_count<=100, page_count>100]

    Returns
    ---------------
    placeholder:dic where  key = 'verysmall'|'small'|'medium'|'large'|'verlarge', 
            value = [dataframe of file list,minibatch size,minibatch wait time, config type to be used]
    
    """
    partition_thresholds = {'verysmall':[10,10,120,'defaut'],'small':[25,10,300,'default'],'medium':[50,8,600,'default'],'large':[150,5,600,'largepdf'],}
    nan_df = df[df.page_count.isna()]
    df = df[df.page_count.notna()].reset_index(drop=True)
    placeholder= {}
    for key, val in partition_thresholds.items():
        placeholder[key] = [df[df.page_count <=val[0]].reset_index(drop=True),val[1],val[2]]
        df = df[df.page_count >val[0]].reset_index(drop=True)
    
    # for very large batch size select the dynamic wait time option i.e why wait-time is passed None
    placeholder['verylarge'] = [pd.concat([nan_df,df],ignore_index=True),3,None,'largepdf']

    return placeholder


def simple_json_parsr(filepath):
    """
    takes filepath and returns a well formated output from simple-json file

    Return:
    -----------------------
    page_wise_doc: Dictionary with two pairs of key,values
        - page_list:list of page, where each page = {'page':page number,
                                                    'content':sorted list of elements}
                                where element = {'type':type of element like table/paragraph etc,
                                                    'content': actual content of element,
                                                    'level':Optional, heading level,
                                                    'columns':Optional, column header for table}
        - type_list: list of type of unique elements found for whole doc
            

    """
    simple_json = open_file(filepath)
    
    def page_wise_contruct(simple_json):
        """
        takes simple-json and contruct list of pages
        """
        pages= []
        type_list = set()
        page = {}
        for i in simple_json: 
            if page:
                if page['page'] == i['page']+1:
                    type_list.add(i['type'])
                    del i['page']
                    page['content'].append(i)
                else:
                    pages.append(page)
                    page={'page':i['page']+1,'content':[]}
                    type_list.add(i['type'])
                    del i['page']
                    page['content'].append(i)
            else:
            
                page={'page':i['page']+1,'content':[]}
                type_list.add(i['type'])
                del i['page']
                page['content'].append(i)
        return {'page_list':pages,'unique_elements_type':type_list}
    
    page_wise_doc = page_wise_contruct(simple_json)
    pages= page_wise_doc['page_list']

    def check_column_header(string_list):
        """
        check if the first entry in content type table is valid column header or not 
        returns tuple (bool,listofstring)

        """
        match = [bool(re.search(r'\*\*(.*?)\*\*', str(text))) for text in string_list]
        if sum(match)/len(match)>0.6:
            return True, [re.sub(r'\*\*', '', str(text)) for text in string_list]
        else:
            return False, [re.sub(r'\*\*', '', str(text)) for text in string_list]
        
    def table_format(pages):
        """
        iterate through pages, and if table exist then perform formatting in two respects:
        1. if the column header is not defiend then fetch the column header from previous page
        2. Drop the irrelvant(NA) rows/columns

        """
        page_cache= 0
        table_cache= []
        for page in pages:
            for element in page['content']:
                if element['type'] == 'table':
                    # check if proper column header,axaparsr table headers exis as list of 
                    # bold markdown strings
                    if_col_header, col_header = check_column_header(element['content'][0])
                    if if_col_header:
                        # add separate element[key]
                        element['columns'] = col_header
                        # remove item 0 as this is now column header
                        element['content'].pop(0)
                        table_cache = col_header
                        page_cache = page['page']
                    else:
                        # if not proper column header exist, then check if len of column headers
                        # in table in previous table is of same length.
                        if ((page['page'] == page_cache +1) & (len(table_cache)==len(col_header))):
                            # assign the column headers from previous cached results
                            element['columns'] = table_cache
                            page_cache = page['page']
                        else:
                            # if none works then just add dummy column names
                            element['columns'] = [f'column_{i}' for i in range(len(col_header)) ]
                            table_cache = col_header
                            page_cache = page['page']
        # once more loop through to clean the table for NA/nan
        for page in pages:
            for element in page['content']:
                if element['type'] == 'table':
                    tmp = pd.DataFrame(data = element['content'],columns=element['columns'])
                    tmp.dropna(axis=0, how='all',inplace=True)
                    tmp.dropna(axis=1, how='all',inplace=True)
                    element['columns'] = list(tmp.columns)
                    element['content'] = tmp.values.tolist()
        return pages

    pages = table_format(pages)
    page_wise_doc['page_list'] = pages

    return page_wise_doc 


def get_pagewise_text(filepath):
    """constructs page wise raw text from simple-json, 
    returns the list of dictionary with page_number and content """

    tmp = simple_json_parsr(filepath)
    page_wise_output = tmp['page_list']
    
    for page in page_wise_output:
        tmp = []
        for element in page['content']:
            tmp.append(str(element['content']))
        page['content'] = "\n".join(tmp)

    return page_wise_output


class ParsrOutputInterpreter:
    """Functions to interpret Parsr's raw JSON file (not simple json), enabling
    access to the underlying document content 
    """

    def __init__(self, object=None):
        """
        - object: the Parsr JSON file to be loaded
        """
        logging.basicConfig(level=logging.DEBUG,
                            format='%(name)s - %(levelname)s - %(message)s')
        self.object = None
        self.page_count = None
        if object is not None:
            self.load_object(object)
        self.page_count = len(self.object['pages'])
        self.get_text_elements_list = ['word', 'line', 'character', 'paragraph', 'heading']

    
    def load_object(self, object):
        self.object = object


    def get_page_raw(self, page_number: int):
        """Get a particular page raw json in a document
        
        Params
        ---------
        page_number: The page number to be searched

        Return
        -------------
        raw json file corresponding to the page number
        """
        for p in self.object['pages']:
            if p['pageNumber'] == page_number:
                return p
        logging.error("Page {} not found".format(page_number))
        return None


    def __get_text_objects(self, page_number:int=None, text_elements:list=["paragraph"]):
        """
        Get the specified text elements from the page

        Params
        -------------
        page_number: the page from which text elements need to be searched
        text_elements: list of text_elements which need to be extracted

        Return
        ----------
        list of text elements
        
        """
        texts = []
        if page_number is not None:
            page = self.get_page_raw(page_number)
            if page is None:
                logging.error(
                    "Cannot get text elements for the requested page; Page {} not found".format(page_number))
                return None
            else:
                for element in page['elements']:
                    if element['type'] in text_elements:
                        texts.append(element)
        else:
            texts = self.__text_objects_none_page(text_elements)

        return texts


    def __text_objects_none_page(self, text_elements):
        texts = []
        for page in self.object['pages']:
            for element in page['elements']:
                if element['type'] in text_elements:
                    texts.append(element)
        return texts


    def __text_from_text_object(self, text_object: dict) -> str:
        """
        Get the text from text_element
        """
        result = ""
        if (text_object['type'] in ['paragraph', 'heading']) or (
                text_object['type'] in ['line']):
            for i in text_object['content']:
                result += self.__text_from_text_object(i)
        elif text_object['type'] in ['word']:
            if isinstance(text_object['content'], list):
                for i in text_object['content']:
                    result += self.__text_from_text_object(i)
            else:
                result += text_object['content']
                result += ' '
        elif text_object['type'] in ['character']:
            result += text_object['content']
        return result


    def get_text_elements(self, page_number: int = None, 
                          text_elements:list = ["paragraph"]) -> dict:
        """Get the entire text from a particular page.

        Params
        ------------
        page_number: page from which text elements need to be extracted
        text_elements: list of text_elments which need to be extracted


        Return
        ---------------
        text_list: dictionary with key=element type (only first order), and value = list 
                    of those element types.



        """
        text_list = {}
        for text_element in text_elements:
            text_element_list = []
            for text_obj in self.__get_text_objects(page_number=page_number,text_elements=[text_element]):
                final_text = ""
                final_text += self.__text_from_text_object(text_obj)
                if text_element == "heading":
                    text_element_list.append((final_text, text_obj["level"]))
                else:
                    text_element_list.append(final_text)
            text_list[text_element]= text_element_list
            
        return text_list
    
    def get_sorted_text(self, page_number: int = None, text_elements:list = ["paragraph"]) -> list:
        """Get the entire text from a particular page

        - page_number: The page number from which all the text is to be
        extracted
        """
        text_list = []
        # for text_element in text_elements:
        #     text_element_list = []
        for text_obj in self.__get_text_objects(page_number=page_number,text_elements=text_elements):
            final_text = ""
            final_text += self.__text_from_text_object(text_obj)
            text_list.append((text_obj["type"], final_text))
        return text_list


def get_tables_markdown(tables_path, filename, sanitize =False, count_limit = 400):
    tables = glob.glob(tables_path+ "*")
    if not sanitize:
        table_list = [[(os.path.splitext(os.path.basename(x))[0]).split("_")[0],
                    pd.read_csv(x, index_col=0)] for x in tables]        
        table_list = [{'page':table[0], 'table':table[1].to_markdown()} for table in table_list]
        placeholder = []
        for table in table_list:
            placeholder.append({'content':table['table'], 'metadata':{'page':table['page'],
                                    'document_name':filename,'headings':[],'type':'table'}})
    else:
        table_list = [[(os.path.splitext(os.path.basename(x))[0]).split("_")[0],
                    pd.read_csv(x, index_col=0)] for x in tables]
        new_tables_list = []
        for table in table_list:
            interim_list = table_sanitize(table[1],token_limit=count_limit)
            for tab in interim_list:
                new_tables_list.append([table[0],tab])
        
        new_tables_list = [{'page':table[0], 'table':table[1].to_markdown()} for table in new_tables_list]
        placeholder = []
        for table in new_tables_list:
            placeholder.append({'content':table['table'], 'metadata':{'page':table['page'],
                                    'document_name':filename,'headings':[],'type':'table'}})
    
    return placeholder
