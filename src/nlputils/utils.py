import fitz
import logging
import configparser
import glob
import docx2pdf
import pandas as pd
import os
from tqdm import tqdm

def check_if_imagepdf(file_path:str)->bool | None:  
    """
    check for if the file is normal pdf or scanned/image pdf
    will return either True/False or None if some error occurs in opening file
    """
    text = ""
    try:
        doc = fitz.open(file_path)
        for page in doc:
            text += page.get_text()
        if text == "":
            return True
        else:return False
    except Exception as e:
        logging.error(e)
        logging.warning(f"Error caused by File:{file_path}")
        return None


def get_page_count(file_path:str)->int | None:
    """ returns the count of page in file (only pdf,docx)"""
    try:
        doc = fitz.open(file_path)
        return len(doc)
    except Exception as e:
        logging.error(e)
        return None


def get_files(root_folder:str,file_extensions=['pdf','docx'])->dict | None:
    """returns the files with extension provided in the root folder, 
        does the search recursively
       
        Params
        ----------
        root_folder: root directory on which the file search will be carried out 
                    recursively including sub-directories
        file_extensions: file-extensions which will be considered in root dir, if the 
                    file_extensions = "*" is given then all files will be considered

        Return
        --------
        file_list: Dict with key as file-extension type and values as list of files
        
    """
    try:
        if file_extensions == "*":
            file_list = glob.glob(f'{root_folder}/**/*', recursive=True)
            return {'allfiles':file_list}
        else:
            file_list = {f:glob.glob(f'{root_folder}/**/*.{f}', recursive=True) for f in file_extensions}
            return file_list
    except Exception as e:
        logging.error(e)
        return


def convertfile(docx_path, pdf_path):
    """
    convert docx file to pdf and save it to 'pdf_path'
    """
    try:
        docx2pdf.convert(docx_path, pdf_path)
        return pdf_path
    except Exception as e:
        print(e)
        return None


def convert_docxfiles(docx_list:list, pdf_path:str = ""):
    """
    convert all docx files in list to pdf and saves them to new dir or in a sub-dir 
    'docx_to_path' in base dir. Alternatively you can call docx2pdf.convert('dirname') to 
    convert all docx files to pdf in same dir.

    """
    #files_df = pd.DataFrame(docx_list, columns=['docx_path'])
    #files_df['filename'] = files_df.docx_path.apply(lambda x: os.path.splitext(os.path.basename(x))[0])


    if pdf_path != "":
        if not os.path.exists(pdf_path):
            os.makedirs(pdf_path)
    pdf_list= []
    for file in tqdm(docx_list):
        new_path = os.path.dirname(file) + '/docx_to_pdf/'
        if not os.path.exists(new_path):
            os.makedirs(new_path)
        new_path = new_path + os.path.splitext(os.path.basename(file))[0] + '.pdf'
        pdf_list.append(convertfile(file,new_path))
    
    return pdf_list

### delete this
def get_config(configfile_path:str)->object:
    """
    configfile_path: file path of .cfg file

    """

    config = configparser.ConfigParser()

    try:
        config.read_file(open(configfile_path))
        return config
    except:
        logging.warning("config file not found")