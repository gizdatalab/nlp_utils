import pymupdf

def get_page_count(file_path):
    doc = pymupdf.open(file_path)
    return len(doc)

