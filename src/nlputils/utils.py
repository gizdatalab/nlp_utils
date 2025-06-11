import fitz
import logging
import configparser
import glob
import docx2pdf
import pandas as pd
import json
import os
from tqdm import tqdm
import subprocess
import re
import sys

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


def find_files_by_extension(search_path, extension, recursive=False):
    """
    Finds files with a specific extension in a given directory.

    Args:
        search_path (str): The path to search in.
        extension (str): The file extension to search for (e.g., "txt", "py").
        recursive (bool, optional): If True, search recursively in subfolders.
            Defaults to False.

    Returns:
        list: A list of absolute paths to the found files.
    """
    found_files = []
    for root, _, files in os.walk(search_path):
        for file in files:
            if file.lower().endswith(extension.lower()):
                found_files.append(os.path.join(root, file))
        if not recursive:
            break  # Stop after the top-level directory
    return found_files

    
def get_files(root_folder:str,file_extensions=['pdf','docx'], recursive=True)->dict | None:
    """returns the files with extension provided in the root folder, 
        use recursive flag to do search recursively or not, file extension are case-sensitive
       
        Params
        ----------
        - root_folder: root directory on which the file search will be carried out 
                    by defualt will do recursively including sub-directories
        - file_extensions: file-extensions which will be considered in root dir, if the 
                    file_extensions = "*" is given then all files will be considered
        - recursive: to search recursively in sub-dir or not, defualt =True  


        Return
        --------
        file_list: Dict with key as file-extension type and values as list of files
        
    """
    try:
        if file_extensions == "*":
            file_list = glob.glob(f'{root_folder}/**/*', recursive=recursive)
            return {'allfiles':file_list}
        else:
            file_list = {f:glob.glob(f'{root_folder}/**/*.{f}', recursive=recursive) for f in file_extensions}
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


def open_file(filepath):
    with open(filepath) as file:
        simple_json = json.load(file)
    return simple_json


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


def is_gibberish(text):
    """ Function to check if a text is gibberish
    """
    if len(text) == 0:
        return True
 
    # Calculate the ratio of non-alphanumeric characters to total characters
    non_alpha_ratio = len(re.findall(r'\W', text)) / len(text)
   
    # Calculate the ratio of whitespace characters to total characters
    whitespace_ratio = len(re.findall(r'\s', text)) / len(text)
 
    # Check for the presence of common English and German words
    common_words = {
        'the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have', 'I',  # English words
        'der', 'die', 'und', 'in', 'den', 'von', 'zu', 'mit', 'das', 'auf', 'ist'  # German words
    }
    words = set(re.findall(r'\b\w+\b', text.lower()))  # Find words using regex
    if not words:
        return True
 
    common_word_ratio = len(common_words.intersection(words)) / len(words) if words else 0
   
    # Check average word length
    word_lengths = [len(word) for word in words]
    avg_word_length = sum(word_lengths) / len(word_lengths) if word_lengths else 0
   
    # Heuristic: adjust thresholds as needed
    if non_alpha_ratio > 0.5 or avg_word_length > 30 or whitespace_ratio > 0.3:
        return True
    return False


def kill_soffice_bin_subprocess_method(force: bool = False) -> int:
    """
    Kills the soffice.bin process using OS-specific shell commands.
    This method is OS-dependent.

    Args:
        force (bool): If True, uses the force kill command.

    Returns:
        int: 0 if no processes were found or terminated, 1 if successful, -1 on error.
    """
    killed_count = 0
    if sys.platform.startswith('linux') or sys.platform == 'darwin': # Linux or macOS
        try:
            # Find PID(s)
            pids_output = subprocess.run(['pgrep', 'soffice.bin'], capture_output=True, text=True, check=False)
            pids = pids_output.stdout.strip().split('\n')
            pids = [p for p in pids if p] # Filter out empty strings

            if not pids:
                print("soffice.bin process not found.")
                return 0

            for pid in pids:
                command = ['kill', pid]
                if force:
                    command = ['kill', '-9', pid] # Force kill
                print(f"Attempting to {'force ' if force else ''}kill soffice.bin with PID {pid}...")
                result = subprocess.run(command, capture_output=True, text=True)

                if result.returncode == 0:
                    print(f"Successfully sent kill signal to PID {pid}.")
                    killed_count += 1
                else:
                    print(f"Failed to kill PID {pid}. Error: {result.stderr.strip()}")
            return killed_count
        except FileNotFoundError:
            print("Error: 'pgrep' or 'kill' command not found. Ensure they are in your PATH.")
            return -1
        except Exception as e:
            print(f"An error occurred on Linux/macOS: {e}")
            return -1

    elif sys.platform == 'win32': # Windows
        try:
            command = ['taskkill', '/IM', 'soffice.bin']
            if force:
                command.append('/F') # Force kill

            print(f"Attempting to {'force ' if force else ''}kill soffice.bin on Windows...")
            result = subprocess.run(command, capture_output=True, text=True)

            if result.returncode == 0:
                print("Successfully sent kill signal to soffice.bin.")
                # taskkill doesn't give precise count, but it usually works if returncode is 0
                return 1
            elif "No tasks are running" in result.stdout:
                print("soffice.bin process not found.")
                return 0
            else:
                print(f"Failed to kill soffice.bin. Error: {result.stderr.strip()}")
                return -1
        except FileNotFoundError:
            print("Error: 'taskkill' command not found. Ensure it is in your PATH.")
            return -1
        except Exception as e:
            print(f"An error occurred on Windows: {e}")
            return -1
    else:
        print(f"Unsupported operating system: {sys.platform}")
        return 0



# Function to convert PPT/PPTX to PDF on Linux

def convert_doc_to_pdf_linux(doc_path, pdf_path, libreoffice_path:str = '/usr/bin/soffice', timeout =100):
    """Convert the docx file to pdfusing libreoffice """
    libreoffice_path = libreoffice_path
    try:
        subprocess.run([libreoffice_path, '--headless', '--convert-to', 'pdf', doc_path, '--outdir', os.path.dirname(pdf_path)], timeout = timeout)
        logging.info(f'Converted {doc_path} to {pdf_path}')
        print(f'Converted {doc_path} to {pdf_path}')
        return pdf_path
    except Exception as e:
        logging.error(e)
        print(e)
        kill_val = kill_soffice_bin_subprocess_method(force=True)
        print(f"Process killing {kill_val}")
        return None
