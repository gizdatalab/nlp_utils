from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.document import ConversionResult
from docling.datamodel.settings import settings
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.chunking import HybridChunker
from docling.datamodel.pipeline_options import (
    AcceleratorDevice,
    AcceleratorOptions,
    PdfPipelineOptions,
    EasyOcrOptions,
    OcrMacOptions,
    PdfPipelineOptions,
    TesseractCliOcrOptions,
    TesseractOcrOptions,
)
from docling.datamodel.base_models import InputFormat
from docling_core.types import DoclingDocument
import logging
import os
import json
import pandas as pd
from pathlib import Path
from typing import Iterable
import yaml


def send_doc(file_path:str):
    """ this is to process single file and get the doclingDocument

    Params
    -----------------
    - file_path: the path to file
    
    Returns
    ------------------
    - result: DoclingDocument created by convertor
    - filename

      """
    try:
        source = file_path
        converter = DocumentConverter()
        result = converter.convert(source)
    except Exception as e:
        logging.warning(e)
        return

    # extracting filename from filepath
    filename = os.path.splitext(os.path.basename(file_path))[0]

    return result, filename

def get_tables(doclingDoc:DoclingDocument, folder_location:str, filename:str):
    """use the putput from send_doc(Docling.Document) and then fetch the tables
      from it
      
    Params
    ---------------------
    - doclingDoc: this is DoclingDocument created using convertor for a prticular file
    - folder_location: folder location where the output for a file will be saved.
                        Only parent folder location required the sub-dir forfile will 
                        be created
                        Ex: pass folder location as "../folder1/", the output will be saved
                          automatically to "../folder1/filename/tables/"
    - filename


    Returns
    -------------------------
    save_to_folder: The output is saved and the location for the tables folder is returned

    """
    # we will save the tables in separate folder
    save_to_folder = Path(folder_location + filename + "/tables")
    try:
        # create the tables folder
        save_to_folder.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logging.warning(e)

    # Export tables
    for table_ix, table in enumerate(doclingDoc.document.tables):
        table_df: pd.DataFrame = table.export_to_dataframe()

        # Save the table as csv
        try:
            element_csv_filename = save_to_folder / f"{table_ix+1}.csv"
            table_df.to_csv(element_csv_filename)
        except Exception as e:
            logging.error(e)
        # Save the table as html
        try:
            element_html_filename = save_to_folder / f"{table_ix+1}.html"
            with element_html_filename.open("w", encoding="utf-8") as fp:
                fp.write(table.export_to_html())
        except Exception as e:
            logging.error(e)

    return save_to_folder

def save_output(doclingDoc, folder_location, filename):
    """ use Docling.Document all the outputs including markdown, text,tables etc
    
     Params
     ------------------------------ 
     - doclingDoc: the docling.document created for a file using convertor
     - folder_location: only the parent location of folder, sub-dir for file will 
                        be created automatically.
                        Ex: pass folder location as "../folder1/", but output will be saved
                         to "../folder1/filename/"
     - filename


     Returns
     ---------------------------
     - save_to_folder: Folder where all the outputs are saved for a file
     - tables_path: tables folder which is sub-dir in save_to_folder
     """
    save_to_folder = Path(folder_location + filename)
    try:
        save_to_folder.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logging.warning(e)

    # save the tables
    tables_path = get_tables(doclingDoc=doclingDoc,folder_location=folder_location,
                             filename=filename)
    try:
        # Export Docling Json
        with (save_to_folder / f"{filename}.json").open("w", encoding="utf-8") as fp:
            fp.write(json.dumps(doclingDoc.document.export_to_dict()))
    except Exception as e:
        logging.info(e)

    # Export Text format:
    try:
        with (save_to_folder / f"{filename}.txt").open("w", encoding="utf-8") as fp:
            fp.write(doclingDoc.document.export_to_text())
    except Exception as e:
        logging.info(e)

    # Export Markdown format:
    try:
        with (save_to_folder / f"{filename}.md").open("w", encoding="utf-8") as fp:
            fp.write(doclingDoc.document.export_to_markdown())
    except Exception as e:
        logging.info(e)

    # Export Document Tags format: this gives a document structure info
    try:
        with (save_to_folder / f"{filename}.doctags").open("w", encoding="utf-8") as fp:
            fp.write(doclingDoc.document.export_to_document_tokens())
    except Exception as e:
        logging.info(e)


    return save_to_folder, tables_path

def export_documents(
    conv_results: Iterable[ConversionResult],output_dir:str,
):
    """ uses the iterable output from docling for mutliple docs and save the output for 
    all the documents 
    
    Params
    -----------------------------
    - conv_results: an iterator which contains the Docling.document output for each file in batch
    - output_dir: the parent folder where output for ech file willbe saved
                Ex: if output_dir = "../folder1/' the putputs for file are saved to 
                '../folder1/filename1/', '../folder1/filename2/' etc
        
    Returns
    --------------------------
    - success_count: the count of succesfully converted files
    - partial_success_count: the count of partially succesfully converted docs
    - failure_count: the count of failed conversion
    - folder_info: the folder location of eahc converted docs
    
        
    """

    # placeholder for tracking
    success_count = 0
    failure_count = 0
    partial_success_count = 0
    folder_info = []
    
    # iterate through the docling.documents for the documents
    for conv_res in conv_results:
        if conv_res.status == ConversionStatus.SUCCESS:
            success_count += 1
            doc_filename = conv_res.input.file.stem

            # save the output from for particular doc
            a,b = save_output(doclingDoc=conv_res,folder_location=output_dir,
                        filename=doc_filename)
            folder_info.append(a)

        elif conv_res.status == ConversionStatus.PARTIAL_SUCCESS:
            logging.info(
                f"Document {conv_res.input.file} was partially converted with the following errors:"
            )
            for item in conv_res.errors:
                logging.info(f"\t{item.error_message}")
            partial_success_count += 1
        else:
            logging.info(f"Document {conv_res.input.file} failed to convert.")
            failure_count += 1

    logging.info(
        f"Processed {success_count + partial_success_count + failure_count} docs, "
        f"of which {failure_count} failed "
        f"and {partial_success_count} were partially converted."
    )
    return success_count, partial_success_count, failure_count, folder_info

def batch_processing(file_list:list, output_dir:str, num_threads=8):
    """
    take the file list and processes and saves the outputs of each file, recommended to use
    for docx and normal pdf. For imagepdf use 'useOCR'

    Params
    ------------------------------
    - file_list: the list of file-paths
    - outout_dir: the folder location to save the output for the whole batch
                Ex: if output_dir = "../folder1/' the putputs for file are saved to 
                '../folder1/filename1/', '../folder1/filename2/' etc
    - num_threads: Docling.Document cannot parallelize the document processing but you can 
                    tell how many threads/logical-processors in CPU can be used, higher the
                    number better the CPU utilization (but limits the usage of machine for
                     other tasks)
        
    Returns
    -------------------
    chekc the Return for export_documents as it uses the same internally 

    """
    logging.basicConfig(level=logging.INFO)
    """batch processing of multiple docs"""
    # device to be used, if GPU then will be used
    accelerator_options = AcceleratorOptions(
        num_threads=num_threads, device=AcceleratorDevice.AUTO
    )
    # declaring the pipeline 
    pipeline_options = PdfPipelineOptions()
    # adding the device accelration info
    pipeline_options.accelerator_options = accelerator_options
    # if image then perform ocr
    pipeline_options.do_ocr = True
    # to extract or not to extaract the table structural info 
    pipeline_options.do_table_structure = True
    pipeline_options.table_structure_options.do_cell_matching = True

    # create document convertor
    doc_converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )

    # process all docs, creates the iterable
    conv_results = doc_converter.convert_all(
        file_list,
        raises_on_error=False,  # to let conversion run through all and examine results at the end
    )

    # calling Export_documents 
    success_count, partial_success_count, failure_count, folder_info = export_documents(
        conv_results, output_dir
    )

    if failure_count > 0:
        raise RuntimeError(
            f"The example failed converting {failure_count} on {len(file_list)}."
        )

    return success_count, partial_success_count, failure_count, folder_info


def useOCR(file_path, num_threads=8):
    """
    this is specifically to be used for image pdfs, however for imagepdfs its good to do 
    the iteration one at a time

    Params
    ---------------------
    - file_path: path of file
    - num_threads: Docling.Document cannot parallelize the document processing but you can 
                    tell how many threads/logical-processors in CPU can be used, higher the
                    number better the CPU utilization (but limits the usage of machine for
                     other tasks)


    Returns
    ---------------------------
    - result: Docling.Document for a file
    - filename 
    
    """
    
    input_doc = Path(file_path)
    # device to be used, if GPU then will be used
    accelerator_options = AcceleratorOptions(
        num_threads=num_threads,device=AcceleratorDevice.AUTO
    )


    # Set lang=["auto"] with a tesseract OCR engine: TesseractOcrOptions, TesseractCliOcrOptions
    #ocr_options = TesseractOcrOptions(lang=["auto"])
    #ocr_options = TesseractCliOcrOptions(lang=["auto"])
    ocr_options = EasyOcrOptions(force_full_page_ocr=True)

    # declare the pipieline for OCR with OCR options
    pipeline_options = PdfPipelineOptions(
        do_ocr=True, force_full_page_ocr=True, ocr_options=ocr_options
    )
    # update the accelarotor options for OCR, use GPU
    pipeline_options.accelerator_options = accelerator_options

    # define the convertor
    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pipeline_options,
            )
        }
    )
    
    # convert doc
    result = converter.convert(input_doc)
    # extract filename
    filename = os.path.splitext(os.path.basename(file_path))[0]

    return result, filename

def hybrid_chunking(folder_location,embed_model_id, max_tokens= None, exclude_metadata: dict = None):
    """
    this is adaptation of hybrid chunking (headings) imlemented for docling.Document

    Params
    --------------------
    - folder_location: folder location where output for a file is saved
                        Ex: "../folder1/filename1" notice no "/" in the end
    - embed_model_id: model id from hugging face to be used for emebdding (the tokenizer of same
                        model will be used to do the chunking)
    - max_tokens: while the max_token info can be fetched from model id but you can set the 
                    token limit for chunking too
    
                    
    Returns
    -------------
    - location to the chunks file
    
    """

    # Validate exclude_metadata format
    if exclude_metadata is not None:
        if not isinstance(exclude_metadata, dict):
            raise TypeError("exclude_metadata must be a dictionary with metadata keys and values to exclude.")
        for k, v in exclude_metadata.items():
            if not isinstance(k, str):
                raise ValueError("Keys in exclude_metadata must be strings and should be one of the following: filename, page, content_layer, label, heading or chunk_length")
            if not isinstance(v, (str, list)):
                raise ValueError("Values in exclude_metadata must be a string or a list of strings.")

    # extracting filename
    filename = os.path.basename(folder_location)

    # definign path for docling.Document within folder
    doc_path = folder_location + "/" + filename + ".json"
    # read file
    try:
        with Path(doc_path).open("r", encoding="utf-8") as fp:
            doc_dict = json.load(fp)
        doc = DoclingDocument.model_validate(doc_dict)
    except Exception as e:
        logging.error("corrupt")
        return None
    # define chunker
    if max_tokens is None:
        chunker = HybridChunker(tokenizer=embed_model_id)
    else:
        chunker = HybridChunker(tokenizer=embed_model_id, max_tokens=max_tokens)
    
    # chunking
    chunk_iter = chunker.chunk(doc)
    chunks = list(chunk_iter)

    # create chunks lsit with metadata info
    paragraphs = []
    for i,chunk in enumerate(chunks):

        ser_txt = chunker.serialize(chunk=chunk)

        # Retrieve the relevant metadata
        
        metadata = {
            'filename':chunk.meta.origin.filename,
            'page':chunk.meta.doc_items[0].prov[0].page_no,
            'content_layer': chunk.meta.doc_items[0].content_layer,
            'label': chunk.meta.doc_items[0].label,
            'heading': chunk.meta.headings,
            'chunk_length': len(ser_txt.split())
            }

        # Exclusion criteria 
        if exclude_metadata: 

            skip = False

            # Check every exclusion criteria
            for k, v in exclude_metadata.items(): 
                if isinstance(v, list):
                    if metadata.get(k) in v: 
                        skip = True
                        break
                else: 
                    if metadata.get(k) == v: 
                        skip = True
                        break
            
            # If criteria is met, move to next paragraph
            if skip: 
                continue 
          
        # if no exclusion criteria is met, append paragraph
        paragraphs.append({'content':ser_txt,
                            'metadata': metadata})
    
    # save the chunks   
    chunks_list = {'paragraphs':paragraphs}
    with open(folder_location+ "/chunks.json", 'w') as file:
        json.dump(chunks_list, file)

    return folder_location+ "/chunks.json"
    

def hybrid_chunking_memory(filename, doc_dict, embed_model_id, max_tokens= None, exclude_metadata: dict = None):
    """
    this is adaptation of hybrid chunking (headings) imlemented for docling.Document

    Params
    --------------------
    - folder_location: folder location where output for a file is saved
                        Ex: "../folder1/filename1" notice no "/" in the end
    - embed_model_id: model id from hugging face to be used for emebdding (the tokenizer of same
                        model will be used to do the chunking)
    - max_tokens: while the max_token info can be fetched from model id but you can set the 
                    token limit for chunking too
    
                    
    Returns
    -------------
    - location to the chunks file
    
    """

    # Validate exclude_metadata format
    if exclude_metadata is not None:
        if not isinstance(exclude_metadata, dict):
            raise TypeError("exclude_metadata must be a dictionary with metadata keys and values to exclude.")
        for k, v in exclude_metadata.items():
            if not isinstance(k, str):
                raise ValueError("Keys in exclude_metadata must be strings and should be one of the following: filename, page, content_layer, label, heading or chunk_length")
            if not isinstance(v, (str, list)):
                raise ValueError("Values in exclude_metadata must be a string or a list of strings.")

    # extracting filename
    # filename = os.path.basename(folder_location)

    # # definign path for docling.Document within folder
    # doc_path = folder_location + "/" + filename + ".json"
    # # read file
    try:
    #     with Path(doc_path).open("r", encoding="utf-8") as fp:
    #         doc_dict = json.load(fp)
        doc = DoclingDocument.model_validate(doc_dict)
    except Exception as e:
        logging.error("corrupt")
        print("corrupt")
        return None
    # define chunker
    if max_tokens is None:
        chunker = HybridChunker(tokenizer=embed_model_id)
    else:
        chunker = HybridChunker(tokenizer=embed_model_id, max_tokens=max_tokens)
    
    # chunking
    chunk_iter = chunker.chunk(doc)
    chunks = list(chunk_iter)

    # create chunks lsit with metadata info
    paragraphs = []
    for i,chunk in enumerate(chunks):

        ser_txt = chunker.serialize(chunk=chunk)

        # Retrieve the relevant metadata
        try:
            tmp_filename = chunk.meta.origin.filename
        except Exception as e:
            logging.error(e)
            tmp_filename = None
        
        try:
            tmp_page_no = chunk.meta.doc_items[0].prov[0].page_no
        except Exception as e:
            logging.error(e)
            tmp_page_no = None
        
        try:
            tmp_content_layer = chunk.meta.doc_items[0].content_layer.value
        except Exception as e:
            logging.error(e)
            tmp_content_layer = None

        try:
            tmp_label = chunk.meta.doc_items[0].label.value
        except Exception as e:
            logging.error(e)
            tmp_label = None

        try:
            tmp_heading = chunk.meta.headings
        except Exception as e:
            logging.error(e)
            tmp_heading = None
        
        try:
            tmp_chunk_length = len(ser_txt.split())
        except Exception as e:
            logging.error(e)
            tmp_chunk_length = None

        metadata = {'filename':tmp_filename,
            'page':tmp_page_no,
            'content_layer': tmp_content_layer,
            'label': tmp_label,
            'heading': tmp_heading,
            'chunk_length': tmp_chunk_length}

        # Exclusion criteria 
        if exclude_metadata: 

            skip = False

            # Check every exclusion criteria
            for k, v in exclude_metadata.items(): 
                if isinstance(v, list):
                    if metadata.get(k) in v: 
                        skip = True
                        break
                else: 
                    if metadata.get(k) == v: 
                        skip = True
                        break
            
            # If criteria is met, move to next paragraph
            if skip: 
                continue 
          
        # if no exclusion criteria is met, append paragraph
        paragraphs.append({'content':ser_txt,
                            'metadata': metadata})
    
    # save the chunks   
    chunks_list = {'paragraphs':paragraphs}
    # # # with open(folder_location+ "/chunks.json", 'w') as file:
    # # #     json.dump(chunks_list, file)

    return chunks_list