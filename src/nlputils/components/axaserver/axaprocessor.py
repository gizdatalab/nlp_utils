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
import docker
from ....nlputils.utils import check_if_imagepdf, get_config
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
                        tesseract-ocr (eng). Suggest to use the useOCR module instead""")
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
            r = get('{}/api/v1/csv/{}/{}/{}'.format(url,request_id,page,table))

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

    
    def set_batch_params(self, batch_size:int=5, batch_wait_time:int=300):
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
    
    
    def processing(self,save_to_folder:str = ''):
        """
        start the batch processing, will save all output to the 'save_to_folder'

        Params
        ------------
        - save_to_folder: the local of folder where to store all the outputs
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
            time.sleep(self.sleep_time)

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
        

class ParsrOutputInterpreter:
    """Functions to interpret Parsr's resultant JSON file, enabling
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


def splitter(json_file, headings_level, filename, page_start=0, formats = ['paragraph','list','table']):
    index = 0
    paragraphs = []
    headings = []
    table_of_contents = []

    for item in json_file:
        if item['type'] == 'heading':
            if len(headings) <headings_level:
                headings.append(item)
            else:
                headings.pop(0)
                headings.append(item)
        if item['type'] == 'tableOfContent':
            table_of_contents.append({'content':item['content'],
                                      'page':item['page']})
        if item['type'] in formats:
            metadata = {}
            if headings:
                placeholder = []
                for i,heading in enumerate(reversed(headings)):
                    placeholder.append({f'headings_{i}':{'content':heading['content'], 'page':heading['page']}})
            else:
                placeholder = []
            if placeholder:
                metadata['headings'] =  placeholder   
            else:
                 metadata['headings'] = []              
            metadata['page'] = item['page']
            metadata['document_name'] = filename
            paragraphs.append({'content':item['content'],'metadata':metadata})
            metadata['type'] = item['type']
            metadata['index'] = index
            index+=1
    
    return {'paragraphs':paragraphs, 'table_of_contents':table_of_contents}


def table_sanitize(df, token_limit = 400):
    print(df)
    placeholder = []
    def sanitize_table(df=df,token_limit = token_limit):
        df['token_count'] = None
        for i in range(len(df)):
            df.loc[i,'token_count'] =  len(" ".join(str(x) for x in list(df.loc[[i]].values.flatten())).split())
            df = df[df.token_count !=0].reset_index(drop=True)
        df['agg_token_count'] = df.token_count.cumsum()
        if df.iloc[-1]['agg_token_count'] <= token_limit:
            placeholder.append(df)
            return
        else:
            index_val = list(filter(lambda i: i > token_limit, df.agg_token_count.to_list()))[0]
            index_val = df.agg_token_count.to_list().index(index_val)
            placeholder.append(df.iloc[:index_val,:])
            sanitize_table(df.iloc[index_val:,:].reset_index(drop=True))
            return
    sanitize_table()
    return placeholder


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

# def paragraph_sanitize(paragraphs, lower_threshold, upper_threshold, tokenizer):
#     new_paragraphs = []
#     for para in paragraphs:
#         tokenized_para = 
#         if len(tokenizer.encode(str(para['content']))) > lower_threshold:
#             new_paragraphs.append(para)
#         if len()
#         if para['metadata']['headings']:
#             headings_content = [val['content'] for key,val in para['metadata']['headings'].items()]
        
        

        
        # files = glob.glob(folder + "*")
        # if len(files)  == 0:
        #     logging.error("folder {} is empty".format(folder))
        #     return
            
        # filemask = [check_input_file(file) for file in files]
        # files = list(np.array(files)[filemask])