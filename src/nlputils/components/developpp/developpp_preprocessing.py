import json
import pandas as pd
import numpy as np
import re
import os


########### Remove paragraphs ############

# Searched for headlines and follow-up headlines

# Looking at the documents, the following patterns can be observed:
# The paragraph "Ausgangssituation" is usually followed by "Zielsetzung" // 
# sometimes by "Maßnahmen" (DEG_2193) // "geplante Maßnahmen" (DEG_2598) // 
# "Zielsetzung und geplante Maßnahmen" (DEG_2733) // "Objektive und geplante Maßnahmen" (DEG_2928) // 
# "Erreichte Ergebnisse" (DEG_3848) // "Zielerreichung der Maßnahme" (GIZ_3206, GIZ_3221) // 
# "Ziele und geplante Maßnahmen" (GIZ_3213) // "Erzielte Wirkungen" (GIZ_3741) -> don't include "maßnahmen" 
# because then there are a lot of duplicates with both "Zielsetzung" and "Maßnahmen" found

# The paragraph "Interesse des privaten Partners" is usually followed by "Entwicklungspolitischer Nutzen" // sometimes by "Ergebnisse" (DEG_1946), Nothing (GIZ_2321, GIZ_2429, GIZ_2588, GIZ_2604, GIZ_2638)
# The paragraph "Projektpartner und Projektmotivation" ist usually followed by "Entwicklungspolitische Relevanz"
# The paragraph "Zitate" is usually at the end of the document
# The paragraph "Developmental Background" is usually followed by "aims and measures", "project aim"
# The paragraph "Significance for the partner company" is usually followed by "Important development effects"
# The paragraph "Project partner and project motivation" is usually followed by "Relevance to development policy" or "Relevance for development policy"
# The paragraph "Company Business Model" is usually followed by "develoPPP Ventures Investment", "developpp.de venture investment", "background and relevance for the sector and the target county"


flag_dict = {'ausgangssituation': ['zielsetzung'],
            'ausganssituation': ['zielsetzung'], 
            'ausgangsituation': ['zielsetzung'],
            'interesse des privaten partners': ['entwicklungspolitischer nutzen'], 
            'interesse des privatem partners': ['entwicklungspolitischer nutzen'], 
            'interesse der privaten partner': ['entwicklungspolitischer nutzen'],
            'projektpartner und projektmotivation': ['entwicklungspolitische relevanz'],
            'developmental background': ['aims and measures', 'project aim'], 
            'significance for the partner company': ['important development effects'],
            'project partner and project motivation': ['relevance to development policy', 'relevance for development policy'],
            'company and business model': ['venture investment', 'ventures investment'],
            'Maßnahme: neu beendet': ['Projektübersicht'],
            'Project: new finished': ['Project Overview'],
            'Partnerunternehmen': ['Projektpartner und Projektmotivation'],
            'Projektpartner': ['Projektpartner und Projektmotivation'],
            'Partner company':['Project partner and project motivation'],
            'Project partner': ['Project partner and project motivation'],
            'Partner company': ['Contact'],
            'Project partner': ['Contact']
            }



### Functions 

def remove_paragraphs(directory_path: str, input_file_path: str, output_file_path: str):
    """ 
    Takes one entry of the JSON file (one document) as input and removes all paragraphs 
    based on the previously identified headlines (case-insensitive).
    
    The document entry contains a new section "text_modified" with the modified text and additional check records.
    
    Parameters:
    directory_file_path (str): Base directory where input/output files are located.
    input_file_path (str): File path for the input JSON file.
    output_file_path (str): File path for the output JSON file (modified format).
    
    Returns:
    str: Confirmation message indicating successful modification and saving.
    """

    flag_dict = {'ausgangssituation': ['zielsetzung'],
            'ausganssituation': ['zielsetzung'], 
            'ausgangsituation': ['zielsetzung'],
            'interesse des privaten partners': ['entwicklungspolitischer nutzen'], 
            'interesse des privatem partners': ['entwicklungspolitischer nutzen'], 
            'interesse der privaten partner': ['entwicklungspolitischer nutzen'],
            'projektpartner und projektmotivation': ['entwicklungspolitische relevanz'],
            'project partner and project motivation': ['relevance for development policy'],
            'developmental background': ['aims and measures', 'project aim'], 
            'significance for the partner company': ['important development effects'],
            'project partner and project motivation': ['relevance to development policy', 'relevance for development policy'],
            'company and business model': ['venture investment', 'ventures investment', 'developpp ventures investment'],
            'maßnahme: neu beendet': ['projektübersicht'],
            'project: new finished': ['project Overview'],
            'partnerunternehmen': ['projektpartner und projektmotivation'],
            'projektpartner': ['projektpartner und projektmotivation'],
            'partner company':['project partner and project motivation', 'contact'],
            'project partner': ['project partner and project motivation', 'contact'],      
            }
    
    # Load the input JSON file
    with open(os.path.join(directory_path, input_file_path), "r", encoding="utf-8") as file:
        data = json.load(file)

    # Convert dictionary to dataframe
    df_docs = pd.DataFrame(list(data.items()), columns=['file_name', 'text'])

    # Ensure modified text starts as the original text
    df_docs['text_modified'] = df_docs['text']

    # Add columns for tracking
    df_docs['flag_words_found'] = [[] for _ in range(len(df_docs))]
    df_docs['characters_deleted'] = 0
    df_docs['could_not_delete_para'] = 0
    df_docs['para_deleted'] = 0
    df_docs['percentage_deleted'] = 0.0
    df_docs['deleted_text'] = [[] for _ in range(len(df_docs))]

    # Loop over each document in the dataframe
    for index, row in df_docs.iterrows():
        doc_text = row['text_modified']
        doc_text_lower = doc_text.lower()  # Convert to lowercase for searching
        flag_words_found = []
        deleted_text = []
        characters_deleted = 0
        para_deleted = 0
        could_not_delete_para = 0

        # Check each headline in the document
        for headline, follow_up_headlines in flag_dict.items():
            headline_lower = headline.lower()

            if headline_lower in doc_text_lower:
                flag_words_found.append(headline)
                found_follow_up = False  # Track if a follow-up was found

                for follow_up_headline in follow_up_headlines:
                    follow_up_headline_lower = follow_up_headline.lower()

                    if follow_up_headline_lower in doc_text_lower:
                        # Regex pattern: Find text between headline and follow-up headline
                        pattern = re.escape(headline) + r"(.*?)(?=" + re.escape(follow_up_headline) + r"|$)"
                        match = re.search(pattern, doc_text, flags=re.DOTALL | re.IGNORECASE)

                        if match:
                            deleted_text.append(match.group(1).strip())  # Store deleted text
                            doc_text = re.sub(pattern, "", doc_text, flags=re.DOTALL | re.IGNORECASE)  # Remove text
                            doc_text_lower = doc_text.lower()  # Update lowercase version
                            characters_deleted += len(match.group(1))
                            para_deleted += 1
                            found_follow_up = True

                if not found_follow_up:
                    could_not_delete_para += 1  # If no follow-up was detected

        # Calculate percentage of deleted text
        total_text_length = len(row['text'])
        percentage_deleted = (characters_deleted / total_text_length * 100) if total_text_length > 0 else 0.0

        # Update dataframe with results
        df_docs.at[index, 'text_modified'] = doc_text
        df_docs.at[index, 'flag_words_found'] = flag_words_found
        df_docs.at[index, 'characters_deleted'] = characters_deleted
        df_docs.at[index, 'could_not_delete_para'] = could_not_delete_para
        df_docs.at[index, 'para_deleted'] = para_deleted
        df_docs.at[index, 'percentage_deleted'] = percentage_deleted
        df_docs.at[index, 'deleted_text'] = deleted_text
    
    # Print info on percentage deleted 
    print(f"Average percentage of document deleted: {df_docs['percentage_deleted'].mean()}")
    print(f"Max percentage of document deleted: {df_docs['percentage_deleted'].max()}")
    print(f"Min percentage of document deleted: {df_docs['percentage_deleted'].min()}")


    # Save full dataframe as JSON (modification tracking)
    modification_info_path = os.path.join(directory_path, "03_all_docs_processed/modification_info.json")
    df_docs.to_json(modification_info_path, orient="index", indent=4, force_ascii=False)

    # Save in original format (file_name as keys, modified text as values)
    modified_dict = df_docs.set_index("file_name")["text_modified"].to_dict()
    with open(os.path.join(directory_path, output_file_path), "w", encoding="utf-8") as json_file:
        json.dump(modified_dict, json_file, indent=4, ensure_ascii=False)

    return f"Modified documents saved to {output_file_path} and modification info saved to modification_info.json"


