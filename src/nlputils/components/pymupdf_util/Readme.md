**pymupdf utils** is built to leverage the https://pymupdf.readthedocs.io/en/latest/index.html

Packages: pymupdf=1.24.11, pymupdf4llm=0.0.17

For tessearct installation on azure:
- Install the utils first
- in same environment run  ```sudo apt install tesseract-ocr```
- locate the tessdata (default: "/usr/share/tesseract-ocr/4.00/tessdata), else run ```sudo find / -name "tessdata"``` pass this location to tessdata when using OCR

## Remarks on Document processing using Pymupdf:
- Easy to deploy as it built as package which can be easily installed.
- Detailed Features of Pymupdf: https://pymupdf.readthedocs.io/en/latest/about.html
- Supported output formats inlcude: Markdown (MD), CSV(tables) and TXT. Markdown support for image pdf is not a direct implementation but can be implemented.
- OCR capabilties: Tool can leverage tesseract (support for over ~100 languages) for OCR but table and text formatting is not supported when using OCR, however it support paid services. **Ease to configure the DPI for OCR** which can impact the text extraction from images. However the tessearact needs to be installed separatelly and then need to be made avialabe for use with pymupdf. (https://pymupdf.readthedocs.io/en/latest/installation.html#enabling-integrated-ocr-support)
- Performance
   - **Biggest advantage of tool is speed and slim on compute resources**. 40 document with total 2000 pages takes 90 min for OCR, 150 documents with total 9000 pages ~ 3hrs (for axaserver ~ 8 hrs utlizing 5 cores that is 5 document being processed in parallel)
   - Heading and complex document structure might not perform to expectation.
   - Its ideal if you want to use pdf editing for reconstructing the pdf with programatically edited/annotated pdf.
   - No multiprocessing, but doesnt needs also.
   - Good for Prototyping, and handing user input in chatbot/apps but not for Knowledge base.
  


            
