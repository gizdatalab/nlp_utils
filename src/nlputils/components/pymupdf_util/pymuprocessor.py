import pymupdf
import pymupdf4llm
import os
import logging
from ....nlputils.utils import get_files, open_file
from langchain.text_splitter import MarkdownTextSplitter
from langchain_text_splitters import RecursiveCharacterTextSplitter

def create_markdown(filepath, folder_location, filename):
    """
    reads file from filepath and converts it to page-wise markdown

    Params
    --------------
    - filepath: filepath to pdf/other supported file formats
    - folder_location: location where to save the output
    - filename: filename to be used to create the dir within folder_location

    Returns
    ----------------
    - new_path: path to where all page-wise markdown files will be saved
    
    """
    try:
        with pymupdf.open(filepath) as doc:
            # convert file to markdown text
            md_text = pymupdf4llm.to_markdown(doc, page_chunks=True)

            try:

                if not os.path.exists(folder_location + f"tmp/{filename}/markdown/"):
                    os.makedirs(folder_location + f"tmp/{filename}/markdown/")
                    new_path = folder_location + f"tmp/{filename}/markdown/"
                else:
                    new_path = folder_location + f"tmp/{filename}/markdown/"
            except Exception as e:
                logging.error(e)
                return None
                 

            # iterate through the pages and save each file as markdown
            try:
                for id,page in enumerate(md_text):

                    with open(new_path + f'{id}.md', 'w') as file:
                            file.write(page['text'])
            except Exception as e:
                logging.error(e)
            
        return new_path
    except Exception as e:
        logging.error(e)
        logging.warning(f"file corrupt {filepath}")
        return None


def useOCR_create_text(filepath, tessdata, folder_location, filename, dpi=300):
    """
    reads file from filepath and converts it to page-wise text_file using OCR

    Params
    --------------
    - filepath: filepath to pdf/other supported file formats
    - folder_location: location where to save the output
    - filename: filename to be used to create the dir within folder_location
    - dpi: highher the dpi value better the resolution for text extraction

    Returns
    ----------------
    - new_path: path to where all page-wise markdown files will be saved

    """
    
    try:
        doc = pymupdf.open(filepath)
        try:
            if not os.path.exists(folder_location + f"tmp/{filename}/txt/"):
                os.makedirs(folder_location + f"tmp/{filename}/txt/")
                new_path = folder_location + f"tmp/{filename}/txt/"
            else:
                new_path = folder_location + f"tmp/{filename}/markdown/"
        except Exception as e:
            logging.error(e)
            return None


        # iterate through pages
        try:
            for id,page in enumerate(doc):
                # ocr the page and save page output as markdown
                full_tp = page.get_textpage_ocr(tessdata = tessdata, flags=0, dpi=dpi, full=True)
                
                with open(new_path + f'{id}.txt', 'w') as file:
                        file.write(page.get_text(textpage=full_tp))
        except Exception as e:
            logging.error(e)       
        return new_path
    except Exception as e:
        logging.error(e)
        logging.warning(f"file corrupt {filepath}")
        return None

def create_chunks(folder_location, filename, overlap=10, chunk_size=800, file_extension = 'md', page_level_chunk = False):
    """
    read the files in folder-location and create_chunks using splitters from langchain

    Params
    -----------------
    folder_location: location of folder where all page-wise files are
    overlap: overlap size, this value is character based
    chunk_size: size of each para chunk to be created, this value is character based
    file_extension: file extension of type to be used either 'md' or 'txt'


    Returns
    -------------
    - chunks_placeholder: list of chunks where each chunk is dictionary {'content':actual content,
    'metadata': dictionary with info on page and filename}

    """
    chunks_placeholder = []
     
    pages = get_files(folder_location,file_extensions=[file_extension])
    pages = pages[file_extension]
    # sort the pages
    pages.sort(key=lambda f: int(''.join(filter(str.isdigit, f))))

    if page_level_chunk == True:
        for page in pages:
            with open(page, 'r') as f:
                chunk = f.read()
            chunks_placeholder.append({'content':chunk, 
                                'metadata':{'page':int(os.path.splitext(os.path.basename(page))[0]) + 1,
                                'filename':filename}})

        return {'paragraphs':chunks_placeholder}
    else:
        # define the splitter type based on file_extension
        if file_extension == 'md':
            splitter = MarkdownTextSplitter(chunk_size=chunk_size, chunk_overlap=overlap)
        else:
            splitter = RecursiveCharacterTextSplitter(
                    chunk_size=chunk_size,
                    chunk_overlap=overlap,
                    length_function=len,
                    is_separator_regex=False,
                )
        # iterate through the pages
        for page in pages:
            with open(page, 'r') as f:
                markdown_string = f.read()
            page_chunks = splitter.split_text(markdown_string)
            # iterate through page_chunks
            for j,chunk in enumerate(page_chunks):
                chunks_placeholder.append({'content':chunk,
                                        'metadata':{'page':int(os.path.splitext(os.path.basename(page))[0]) +1,
                                                    'filename':filename}})    
        return {'paragraphs':chunks_placeholder}



