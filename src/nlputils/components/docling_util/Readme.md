**doclingserver** is the utils built to leverage the https://ds4sd.github.io/docling/ from IBM Open AI

## Remarks on Document processing using Axaparsr:
- Easy to deploy, and has option to leverage threads to make document processing fast but, it runs the document batch processing sequentially.
- Supported output formats inlcude: **HTML,Markdown,JSON	Lossless serialization of Docling Document,Text, CSV (for tables)**
- [Input formats supported](https://ds4sd.github.io/docling/supported_formats/): **PDF,DOCX, XLSX, PPTX,Markdown,HTML, XHTML, CSV, PNG, JPEG, TIFF, BMP	Image formats**
- OCR capabilties: Has EasyOCR inbuilt which doesnt need to be configured and can be used out of box. Tool can leverage **tesseract** (support for over ~100 languages) including table and text formatting when using OCR, but also has many other options. However no easy way to configure the DPI for OCR  which can impact the text extraction from images.
- Documentation with examples: https://ds4sd.github.io/docling/examples/
- Performance:
   - The tool performs really good especially for complex document structure containing multi page running tables, tables within tables, long documents and performs exactly same or better than axaparsr.
   - Processing times on CPU utilizing 8 threads: 20-30 pages doc: ~1 min, 500-600 pages: ~ 1 hr, OCR for 20 pages: ~10 min
   - It uses many many ML models including for OCR, layout detection, heading etc, therefore on CPU it is slow than compared to axaserver, especially given it run document processing sequentially. However given that we cna leverage the GPU processing can be made fast. Ex: When processing document of 500 pages on CPU it takes ~  1 hr, but with NVIDIA Tesla T4 GPU with 16GB it can perform same in ~ 10 min. However when comparing with pymupdf for OCR 40 documents with 2000 pages it takes ~3.5 hrs on NVIDIA Tesla T4 GPU with 16GB, but the quality of output far exceeds as it has layout and table structures too.
   - AI models: Layout, Table and OCR: Technical Architecture of Docling https://arxiv.org/pdf/2408.09869

