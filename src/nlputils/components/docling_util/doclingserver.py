from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.document import ConversionResult
from docling.datamodel.settings import settings
from docling.document_converter import DocumentConverter
import logging
import os
import json
import pandas as pd
from pathlib import Path
from typing import Iterable

def send_doc(file_path):
    """ this is to process single file and get the doclingDocument"""
    try:
        source = file_path
        converter = DocumentConverter()
        result = converter.convert(source)
    except Exception as e:
        logging.warning(e)
        return

    filename = os.path.splitext(os.path.basename(file_path))[0]

    return result, filename


def get_tables(doclingDoc, folder_location, filename):
    save_to_folder = folder_location + "/" + filename + "/tables/" 
    try:
        if not os.path.exists():
            os.makedirs(save_to_folder)
    except Exception as e:
        logging.warning(e)
    
    # Export tables
    for table_ix, table in enumerate(doclingDoc.document.tables):
        table_df: pd.DataFrame = table.export_to_dataframe()

        # Save the table as csv
        element_csv_filename = save_to_folder + f"{table_ix+1}.csv"
        table_df.to_csv(element_csv_filename)

        # Save the table as html
        element_html_filename = save_to_folder + f"{table_ix+1}.html"
        with element_html_filename.open("w") as fp:
            fp.write(table.export_to_html())
    return save_to_folder


def save_output(doclingDoc, folder_location, filename):
    save_to_folder = folder_location + "/" + filename + "/"
    try:
        if not os.path.exists(save_to_folder):
            os.makedirs(save_to_folder)
    except Exception as e:
        logging.warning(e)
    
    tables_path = get_tables(doclingDoc=doclingDoc,folder_location=folder_location,
                             filename=filename)
            
    with (save_to_folder + f"{filename}.json").open("w", encoding="utf-8") as fp:
        fp.write(json.dumps(doclingDoc.document.export_to_dict()))

    # Export Text format:
    with (save_to_folder + f"{filename}.txt").open("w", encoding="utf-8") as fp:
        fp.write(doclingDoc.document.export_to_text())

    # Export Markdown format:
    with (save_to_folder + f"{filename}..md").open("w", encoding="utf-8") as fp:
        fp.write(doclingDoc.document.export_to_markdown())

    # Export Document Tags format:
    with (save_to_folder + f"{filename}.doctags").open("w", encoding="utf-8") as fp:
        fp.write(doclingDoc.document.export_to_document_tokens())
    
    return save_to_folder, tables_path


def export_documents(
    conv_results: Iterable[ConversionResult],
    output_dir:Path,
):
    output_dir.mkdir(parents=True, exist_ok=True)

    success_count = 0
    failure_count = 0
    partial_success_count = 0

    for conv_res in conv_results:
        if conv_res.status == ConversionStatus.SUCCESS:
            success_count += 1
            doc_filename = conv_res.input.file.stem
            save_to_folder = output_dir + "/" + f"{doc_filename}/"
            try:
                if not os.path.exists(save_to_folder):
                    os.makedirs(save_to_folder)
            except Exception as e:
                logging.warning(e)


            save_output(doclingDoc=conv_res,folder_location=save_to_folder,
                        filename=doc_filename)

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
    return success_count, partial_success_count, failure_count


def batch_processing(file_list):
    logging.basicConfig(level=logging.INFO)

    doc_converter = DocumentConverter()

    conv_results = doc_converter.convert_all(
        file_list,
        raises_on_error=False,  # to let conversion run through all and examine results at the end
    )

    success_count, partial_success_count, failure_count = export_documents(
        conv_results, output_dir=Path("scratch")
    )

    if failure_count > 0:
        raise RuntimeError(
            f"The example failed converting {failure_count} on {len(file_list)}."
        )



