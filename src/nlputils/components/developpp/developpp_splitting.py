
import json
import pandas as pd
import numpy as np
import os
import json
import re


def basic_cleaning(s:str, remove_punc:bool = False):

    """
    Performs basic cleaning of text.

    Params
    ----------
    s: string to be processed
    removePunc: to remove all Punctuation including ',' and '.' or not
    
    Returns: processed string: see comments in the source code for more info
    """
    
    # Remove URLs
    s = re.sub(r'^https?:\/\/.*[\r\n]*', ' ', s, flags=re.MULTILINE)
    s = re.sub(r"http\S+", " ", s)

    # Remove new line characters
    s = re.sub('\n', ' ', s) 

    # Remove punctuations
    if remove_punc == True:
      translator = str.maketrans(' ', ' ', string.punctuation) 
      s = s.translate(translator)
    # Remove distracting single quotes and dotted pattern
    s = re.sub("\'", " ", s)
    s = s.replace("..","") 
    
    return s.strip()



def split_text(
    text: str,
    file_name: str,  
    split_by="word",     # 'paragraph', 'sentence', 'word'
    split_length=70,
    split_overlap=0,
    remove_punc= True,
    apply_clean=True,
    respect_sentence_boundary=True  # only applies if split_by="word"
):

    def regex_sent_tokenize(text):
        # Naive rule-based sentence splitter
        sentences = re.split(r'(?<=[.!?])\s+', text)
        return [s.strip() for s in sentences if s.strip()]

    # Splitting
    if split_by == "paragraph":
        chunks = [p.strip() for p in text.split("\n\n") if p.strip()]

    elif split_by == "sentence":
        chunks = regex_sent_tokenize(text)

    elif split_by == "word":
        words = text.split()
        if not respect_sentence_boundary:
            chunks = [
                " ".join(words[i:i+split_length])
                for i in range(0, len(words), split_length - split_overlap or 1)
            ]
        else:
            sentences = regex_sent_tokenize(text)
            chunks = []
            current_chunk = []
            current_len = 0

            for sent in sentences:
                sent_words = sent.split()
                if current_len < split_length:
                    current_chunk.append(sent)
                    current_len += len(sent_words)
                else:
                    # Always ensure we include the sentence that causes us to exceed the split_length
                    current_chunk.append(sent)
                    chunks.append(" ".join(current_chunk))
                    current_chunk = []
                    current_len = 0

            if current_chunk:
                chunks.append(" ".join(current_chunk))
    else:
        raise ValueError(f"Unsupported split_by: {split_by}")

    # Group chunks (for sentence/paragraph modes)
    if split_by in ["sentence", "paragraph"] and split_length > 1:
        grouped = []
        for i in range(0, len(chunks), split_length - split_overlap or 1):
            group = " ".join(chunks[i:i+split_length])
            grouped.append(group)
        chunks = grouped

    # Clean if requested
    if apply_clean:
        chunks = [basic_cleaning(chunk, remove_punc) for chunk in chunks]

    # Create DataFrame with file_name
    df = pd.DataFrame({
        "file_name": [file_name] * len(chunks),
        "text": chunks
    })

    return {
        "paraList": chunks,
        "text": " ".join(chunks),
        "dataframe": df
    }
