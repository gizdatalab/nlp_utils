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


