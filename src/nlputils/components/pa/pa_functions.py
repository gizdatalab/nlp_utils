"""
================================================================================
PA Specific Functions (ToDo: Check which of them can be replaced by Utils/Developpp functions)
================================================================================

Table of Contents:
------------------
1. remove_first_page (preprocessing)
    Remove the first page of each document in a DataFrame.

2. clean_text (preprocessing)
    Clean up text by removing excess whitespace and special characters.

3. is_gibberish & remove_gibberish_paragraphs (preprocessing)
    Detect and remove gibberish text/rows from a DataFrame.

4. filter_min_text_length (preprocessing)
    Remove rows with too-short text columns.

5. remove_paragraphs_with_afghanistan (preprocessing)
    Remove rows containing "Afghanistan"

6. entity_recognizer & anonymize_entities_in_dataframe (preprocessing)
    Recognize and/or anonymize entities (GLiNER) in text or DataFrames.

7. tensor_to_float (inference)
    Convert tensor objects or tensor-representing strings to floats.

================================================================================
"""

import json
import os
import re
import logging
from typing import Any, Optional, Set, List

import numpy as np
import pandas as pd
import torch

# Set up module-level logger
logger = logging.getLogger(__name__)

# ==============================================================================
# 1. Remove First Page of Each Document
# ==============================================================================

def remove_first_page(df: pd.DataFrame, doc_id_column: str = "document_id", page_column: str = "page") -> pd.DataFrame:
    """
    Removes the first page of each document in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing document data.
        doc_id_column (str): Column indicating the document ID.
        page_column (str): Column indicating the page number.

    Returns:
        pd.DataFrame: DataFrame with first pages of each document removed.

    Note:
        Assumes page numbers are integers and the lowest page per document is the first page.
    """
    if doc_id_column not in df or page_column not in df:
        logger.warning("Specified columns not in DataFrame. Returning DataFrame unchanged.")
        return df.copy()

    # Get indices of the first page for each document
    first_page_idx = df.groupby(doc_id_column)[page_column].idxmin()
    # Drop the first page rows
    cleaned_df = df.drop(first_page_idx).reset_index(drop=True)
    return cleaned_df

# ==============================================================================
# 2. Clean Text
# ==============================================================================

def clean_text(text: Any) -> Any:
    """
    Cleans input text by:
      - Stripping leading/trailing whitespace.
      - Replacing newlines, carriage returns, and tabs with a single space.
      - Collapsing multiple spaces into a single space.

    Args:
        text (Any): The text to be cleaned.

    Returns:
        Any: Cleaned text if input is a string; otherwise, returns the input unchanged.
    """
    if not isinstance(text, str):
        return text

    cleaned = text.strip()
    for char in ('\n', '\r', '\t'):
        cleaned = cleaned.replace(char, ' ')
    cleaned = ' '.join(cleaned.split())
    return cleaned

# ==============================================================================
# 3. Gibberish Detection and Removal
# ==============================================================================

def is_gibberish(text: str) -> bool:
    """
    Determines whether the provided text is likely gibberish using:
      - Ratio of non-alphanumeric characters.
      - Ratio of whitespace.
      - Ratio of common English/German words.
      - Average word length.

    Args:
        text (str): The text to analyze.

    Returns:
        bool: True if likely gibberish, False otherwise.
    """
    if not isinstance(text, str) or len(text.strip()) == 0:
        return True

    total_chars = len(text)
    if total_chars == 0:
        return True

    non_alpha_ratio = len(re.findall(r'\W', text)) / total_chars
    whitespace_ratio = len(re.findall(r'\s', text)) / total_chars
    common_words: Set[str] = {
        'the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have', 'i',
        'der', 'die', 'und', 'in', 'den', 'von', 'zu', 'mit', 'das', 'auf', 'ist'
    }

    words = set(re.findall(r'\b\w+\b', text.lower()))
    if not words:
        return True

    common_word_ratio = len(common_words.intersection(words)) / len(words)
    word_lengths = [len(word) for word in words]
    avg_word_length = sum(word_lengths) / len(word_lengths) if word_lengths else 0

    if non_alpha_ratio > 0.4 or avg_word_length > 30 or whitespace_ratio > 0.2 or common_word_ratio < 0.05:
        return True

    return False

def remove_gibberish_paragraphs(df: pd.DataFrame, text_column: str = 'text') -> pd.DataFrame:
    """
    Removes rows from DataFrame where the specified text column contains gibberish.

    Args:
        df (pd.DataFrame): DataFrame to process.
        text_column (str): Name of text column.

    Returns:
        pd.DataFrame: Cleaned DataFrame with gibberish paragraphs removed.
    """
    df = df.copy()
    df['is_gibberish'] = df[text_column].apply(is_gibberish)
    cleaned_df = df[~df['is_gibberish']].copy()
    cleaned_df.drop(columns=['is_gibberish'], inplace=True)
    return cleaned_df

# ==============================================================================
# 4. Minimum Text Length Filtering
# ==============================================================================

def filter_min_text_length(
    df: pd.DataFrame,
    text_column: str = 'text',
    min_length: int = 20,
    reset_index: bool = True
) -> pd.DataFrame:
    """
    Removes rows from DataFrame where the specified text column has fewer than `min_length` characters.

    Args:
        df (pd.DataFrame): Input DataFrame.
        text_column (str): Column containing text.
        min_length (int): Minimum number of characters required.
        reset_index (bool): Whether to reset DataFrame index.

    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    filtered_df = df[df[text_column].str.len() >= min_length]
    if reset_index:
        filtered_df = filtered_df.reset_index(drop=True)
    return filtered_df

# ==============================================================================
# 5. Afghanistan Paragraph Removal
# ==============================================================================

def remove_paragraphs_with_afghanistan(
    df: pd.DataFrame,
    text_column: str = 'text',
    variants: Optional[List[str]] = None,
    reset_index: bool = True
) -> pd.DataFrame:
    """
    Removes rows from DataFrame where the specified text column contains
    any variant of 'Afghanistan' (case-insensitive), including English and German spellings.

    Args:
        df (pd.DataFrame): DataFrame to filter.
        text_column (str): Name of the text column.
        variants (List[str], optional): Substrings to search for.
        reset_index (bool): Whether to reset DataFrame index after filtering.

    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    if variants is None:
        variants = [
            r"afghanistan",      # English & German
            r"afganistan",       # Common misspelling
            r"afghānistān",      # Diacritics
            r"afganistán",       # Spanish
            r"afganistão",       # Portuguese
        ]
    pattern = re.compile('|'.join(variants), re.IGNORECASE)
    mask = ~df[text_column].str.contains(pattern, na=False)
    filtered_df = df[mask]
    if reset_index:
        filtered_df = filtered_df.reset_index(drop=True)
    return filtered_df




"""
================================================================================
6. Entity Recognition Anonymization  (GLiNER-based)
================================================================================

This module provides functions to recognize and optionally anonymize sensitive entities
(e.g., names, phone numbers, emails, addresses) in text using the GLiNER library.

================================================================================
"""

from typing import List, Dict, Union
import pandas as pd

def entity_recognizer(
    paragraphs: List[str],
    entity_list: List[str] = None,
    anonymize: bool = True,
    model: str = "gliner_multi"
) -> Union[List[str], Dict[str, List[dict]]]:
    """
    Recognizes and optionally anonymizes specified entities in a list of paragraphs.

    Args:
        paragraphs (List[str]): A list of paragraphs to process.
        entity_list (List[str], optional): Entities to recognize. Defaults to ["person", "phone number", "e-mail", "address"].
        anonymize (bool, optional): If True, returns paragraphs with recognized entities anonymized.
        model (str, optional): Pretrained NER model to use.

    Returns:
        List[str]: If anonymize=True, a list of anonymized paragraphs.
        Dict[str, List[dict]]: If anonymize=False, a mapping from paragraph to recognized entities.
    """
    from gliner import GLiNER  # Imported inside to avoid ImportError if not used

    if entity_list is None:
        entity_list = ["person", "phone number", "e-mail", "address"]

    try:
        NER_model = GLiNER.from_pretrained(f"urchade/{model}")
    except Exception as e:
        print(f"Error loading GLiNER model '{model}': {e}")
        return paragraphs  # Return original paragraphs in case of error

    entities_per_text = {}

    for para in paragraphs:
        try:
            entities = NER_model.predict_entities(para, entity_list)
            entities_per_text[para] = entities
        except Exception as e:
            print(f"Error predicting entities for paragraph: {para}\nError: {e}")
            entities_per_text[para] = []

    if not anonymize:
        return entities_per_text

    anonymized_paras = []
    for original, entities in entities_per_text.items():
        anonymized_para = original
        for entity in entities:
            text_to_anonymize = entity["text"]
            label = entity["label"]
            anonymized_para = anonymized_para.replace(text_to_anonymize, f"[{label} removed]")
        anonymized_paras.append(anonymized_para)
    return anonymized_paras

def anonymize_entities_in_dataframe(
    df: pd.DataFrame,
    text_column: str,
    output_column: str = "anonymized_text",
    entity_list: List[str] = None,
    anonymize: bool = True,
    model: str = "gliner_multi",
    filter_expr: str = None,
    sample_n: int = None,
    random_state: int = 42
) -> pd.DataFrame:
    """
    Applies entity recognition/anonymization to a column in a DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.
        text_column (str): The name of the column containing text to anonymize.
        output_column (str): The name of the output column for anonymized text.
        entity_list (List[str], optional): Entities to recognize/anonymize.
        anonymize (bool, optional): If True, outputs anonymized text. If False, outputs entities.
        model (str, optional): Name of GLiNER model to use.
        filter_expr (str, optional): Optional Pandas query string to filter df before processing.
        sample_n (int, optional): If set, sample this many rows before processing.
        random_state (int, optional): Random seed for sampling.

    Returns:
        pd.DataFrame: The DataFrame with an added column of anonymized text or recognized entities.
    """
    df_to_process = df.copy()
    if filter_expr:
        df_to_process = df_to_process.query(filter_expr)
    if sample_n:
        df_to_process = df_to_process.sample(n=sample_n, random_state=random_state)
    paras = df_to_process[text_column].tolist()
    result = entity_recognizer(
        paras,
        entity_list=entity_list,
        anonymize=anonymize,
        model=model
    )
    df_to_process[output_column] = result
    return df_to_process

# ==============================================================================
# 7. Tensor to Float Conversion
# ==============================================================================

def tensor_to_float(value: Any) -> Optional[float]:
    """
    Converts a value representing a tensor (either as a PyTorch Tensor or a string)
    into a rounded float value. If the input is not convertible, returns None.

    Handles:
      - torch.Tensor objects
      - String representations like 'tensor([[0.12345, ...]])'

    Args:
        value (Any): Value to convert.

    Returns:
        Optional[float]: Extracted float rounded to 5 decimals, or None if conversion fails.
    """
    # Handle string tensor
    if isinstance(value, str) and "tensor" in value:
        try:
            match = re.search(r'tensor\(\[\[([0-9.eE+-]+)', value)
            if match:
                return round(float(match.group(1)), 5)
            logger.warning(f"No numeric match found in string: {value}")
            return None
        except Exception as exc:
            logger.error(f"Error processing string tensor: {value}. Exception: {exc}")
            return None

    # Handle torch.Tensor object
    if isinstance(value, torch.Tensor):
        try:
            return round(float(value.flatten()[0].item()), 5)
        except Exception as exc:
            logger.error(f"Error processing torch.Tensor: {value}. Exception: {exc}")
            return None

    logger.warning(f"Value is not a tensor or tensor string: {value}")
    return None

