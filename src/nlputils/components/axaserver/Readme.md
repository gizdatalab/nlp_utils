**axaserver** is the utils built to leverage the https://github.com/axa-group/Parsr from AXA-Group

## Remarks on Document processing using Axaparsr:
- Easy to deploy locally and on cloud environment as its a docker container the dependencies and other issues are well taken care of
- Containerization of application and by leveraging Restful API axaparsr can handle multiple requests. This makes it efficient for large document base processing.
- Supported output formats inlcude: **JSON, Markdown (MD), CSV(tables) and TXT** , there is simple json format constructed from hierarchial axaparsr document format https://github.com/axa-group/Parsr/blob/master/docs/json-output.md
- Input formats supported: **pdf, jpg, jpeg, png, tiff, tif, docx, xml, eml, json**
- OCR capabilties: Tool can leverage **tesseract** (support for over ~100 languages) for OCR but table and text formatting is not supported when using OCR, however it support [paid services](https://github.com/axa-group/Parsr/blob/master/docs/configuration.md#2-extractor-config). However no easy way to configure the DPI for OCR  which can impact the text extraction from images.
- [GUI](https://github.com/axa-group/Parsr/blob/master/docs/gui-guide.md): The tool has graphic User interface which is really good for configuration testing and amking custom config for documents types
- Performance:
   - The tool performs really good especially for complex document structure containing multi page running tables, tables within tables, long documents
   - Heading Detection is performed using the Machine learning module with level identifier of heading, but it seems to over classisy. This step probably is most time consuming step which is there in the tool and if not important can be configured off. More on Configuration [here](https://github.com/axa-group/Parsr/blob/master/docs/configuration.md). *NOTE:It cannot leverage the underlyin GPU to handle fast processing of Heading Detection ML module.*
   - **It takes roughly 1 min for 10 pages on single core, and 2 min for 10 pages with OCR** (using defualt pdf extractor). Most time consuming element inlcude Heading Detection > Table Detection. Since ML is involved in heading detection the processing doesnt scale linearly beyond 200 pages,however in testing even for 600 pages document it took around 1 hour which was expected.
   - Memory issues: The tool built a complex json structure for document which can explode in intermediate process leading to memory crash, this leads to gibberish output for document. If passing 10 document on 10 cores CPU, make sure that you have atleast 16 GB RAM (each document size ~30-40 pages)

# axaprocessor
These are functions build to work with axaparsr in simple manner.
- send_documents_batch (inherits) < send_doc
- there are individual functions to fetch the required output (format) for particular request-id
- axaBatchProcessingLocal:Wrapper class which inherits all functions and does the processing in semi-automated manner on locally deployed server
- axaBatchProcessingHF: Wrapper to work with axaparsr hosted provately on Hugging Face infra.
- Some template config are added within the package:
   - 'default': Standard config to start with
   - 'largepdf': For document more than 200 pages size, or fast processing uses different pdf extractor
   - 'minimal': Removes all document format identifyinfg component and focuses just on getting the text
   - 'ocr': For Image PDFs


Links and Resources from Parsr


#### Parsr Config ####
https://github.com/axa-group/Parsr/blob/master/docs/configuration.md

#### Processing Components in Parsr ####
https://github.com/axa-group/Parsr/tree/master/server/src/processing

#### Parsr Json output ####
https://github.com/axa-group/Parsr/blob/master/docs/json-output.md

#### Parsr simple-json output ####
https://github.com/axa-group/Parsr/blob/master/docs/simple-json-output.md
