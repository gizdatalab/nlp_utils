import glob
import pandas as pd
import os
import logging
from ....nlputils.components.axaserver import axaprocessor

def load_tables(tables_path):
    """
    return all the tables for a particular file in form of list of dataframes

    Params
    -------------
    - tables_path: path to the folder containing tables for a particular file for a file
                 processed through axaserver they are located in sub-dir "folder_location/tables/"
    
    """
    # read all files
    tables = glob.glob(tables_path+ "*")
    # check if tables exist or not
    if len(tables) !=0:
        # read tables which exist as csv files
        table_list = [[int((os.path.splitext(os.path.basename(x))[0]).split("_")[0]),
                    pd.read_csv(x, index_col=[0])] for x in tables] 
        # sort tables files by indexing order
        table_list = sorted(table_list, key=lambda x: x[0])
        # iterate through tables list
        for i,tab in enumerate(table_list):
            # checking for column names
            tab_columns =  [x.lower() for x in  list(tab[1].columns)]
            unnamed = [True if 'unnamed' in x else False for x in tab_columns]
            # table info exist as tuple where first value is page number
            page= tab[0]

            # using the heuristic that if there are lot of unnamed columns
            # and if number of columns are same as in previous columns on previous page
            # then probably the table is just a extended table of previous one
            # so use the columns of tbale in previous table
            if sum(unnamed) >= len(tab_columns)*0.6:
                if len(tab_columns) == len(table_list[i-1][1].columns):
                    if page == table_list[i-1][0] + 1:
                        tab[1] = tab[1].reset_index(drop=True).T.reset_index().T
                        tab[1] = tab[1].reset_index(drop=True)
                        tab[1].columns = list(table_list[i-1][1].columns)

                # if not then columns are dummy empty columsn which need to be discarded
                else:
                    nan_value = float("NaN") 
                    tab[1].replace(" ", nan_value, inplace=True) 
                    tab[1].replace("", nan_value, inplace=True) 
                    tab[1].dropna(how='all', axis=1, inplace=True)
                    tab[1] = tab[1].reset_index(drop=True).T.reset_index().T
                    tab[1] = tab[1].reset_index(drop=True)
        
        return table_list

def paragraph_sanitize(paragraphs, lower_threshold, upper_threshold):
    """
    takes paragraphs list and sanitizes it for token lower/upper count threshold

    """
    logging.basicConfig(level=logging.DEBUG,
                            format='%(name)s - %(levelname)s - %(message)s')
    # new placeholder
    new_paragraphs = []
    # iterate through the paragraphs list
    for para in paragraphs:
        # if element type is paragraph then sanitize it
        if para['metadata']['type'] =='paragraph':
            # get simple token list for paragraph
            para_tokens = str(para['content']).split()
            # check for upper threshold
            if len(para_tokens) < upper_threshold:
                # check for lower threshold
                if len(para_tokens) > lower_threshold:
                    # all good just append the para in new list
                    new_paragraphs.append(para)
                # if the token count is less than lower threshold
                # then just append it to previous paragraph
                else:
                    if len(new_paragraphs)>0:
                        # only merge if previous element is also 'paragraph'
                        if new_paragraphs[-1]['metadata']['type'] =='paragraph':
                            # append the content and headings
                            new_paragraphs[-1]['content'] = new_paragraphs[-1]['content'] + " \n" + para['content']
                            new_paragraphs[-1]['metadata']['headings'].append(para['metadata']['headings'])
                        # if previous element is not paragraph type then just append to new paragraphs list
                        else:
                            new_paragraphs.append(para)
                    else:
                        new_paragraphs.append(para)
            # if upper threshold value is not respected then split it iteratively
            else:
                while len(para_tokens) > upper_threshold:
                    new_paragraphs.append({'content':" ".join(para_tokens[:upper_threshold]), 
                                           'metadata':para['metadata']})
                    para_tokens = para_tokens[upper_threshold:]
        # if element type is not 'paragraph' then just append without santization
        else:
            new_paragraphs.append(para)
        
    return new_paragraphs

def table_sanitize(paragraphs,token_limit = 400):
    """ 
    takes list of paragraphs and sanitizes the tables for token count
    
    """
    placeholder = []
    for para in paragraphs:
        if para['metadata']['type'] !='table':
            placeholder.append(para)
        else:
            df = pd.DataFrame(data = para['content'], columns=para['metadata']['columns'])

            def sanitize_table(df=df,token_limit = token_limit):
                # get token count for each row
                df['token_count'] = None
                for i in range(len(df)):
                    df.loc[i,'token_count'] =  len(" ".join(str(x) for x in list(df.loc[[i]].values.flatten())).split())
                # get token cummulative token count till each row
                df['agg_token_count'] = df.token_count.cumsum()
                #check if the token count till last row is less than threshold
                if df.iloc[-1]['agg_token_count'] <= token_limit:
                    df.drop(columns=['token_count','agg_token_count'],inplace=True)
                    placeholder.append(para)
                    return
                else:
                    # get index val where to split the dataframe
                    index_val = list(filter(lambda i: i > token_limit, df.agg_token_count.to_list()))[0]
                    index_val = df.agg_token_count.to_list().index(index_val)
                    # if index==0 no split possible
                    if index_val == 0:
                        df.drop(columns=['token_count','agg_token_count'],inplace=True)
                        placeholder.append(para)
                    # split dataframe, then recursively call the sanitization
                    else:
                        placeholder.append({'content':para['content'][:3],'metadata':para['metadata']})
                        df.drop(columns=['token_count','agg_token_count'],inplace=True)
                        sanitize_table(df.iloc[index_val:,:].reset_index(drop=True))
                    return
            # making the call to recursive table sanitization
            sanitize_table()
    return placeholder

def simplejson_splitter(json_filepath, headings_level, filename, page_start = 0,lower_threshold = 30,
                        upper_threshold = 300, formats = ['paragraph','list','table']):
    """
    will take simple-json output filepath from axaparsr and perform chunking

    Params
    ---------------
    - json_filepath: filepath of simple-json file
    - headings_level: number of heading that need to be kept in metadata information
    - filename: filename which need to be seeded in metadata
    - lower_threshold: number of token (naive string.split is used to get tokens) to check for lower threshold
    - upper_threshold: number of token (naive string.split is used to get tokens) to check for
    - formats: list of axaparsr elements to consider as part of paragraphs usual list of elements
               returned in simple-json = ['paragraph','list','table', 'heading','tableOfContent']

    Returns
    -------------------
    - paragraphs: list of para, each para is dictionary with two pair of key,values (keys in = ['content','metadata])
    - table_of_content: list of para, where element type=='tableOfContent'

    """
    # get structured output from simple-json file
    simple_json_structured = axaprocessor.simple_json_parsr(json_filepath)
    # get only page_list, other compoenent is list of unique elements types
    simple_json_structured =simple_json_structured['page_list']

    # placeholder to collect all element types in list of formats
    paragraphs = []
    # placeholder to collect number of heading info to be seeded in metadata for each para
    headings = []
    # collect the table of content in seprate placeholder
    table_of_contents = []
    # iterate through the pages
    for page in simple_json_structured:
        if page['page']>=page_start:
        # iterate through each elements within page
            for item in page['content']:
                # headings are collected in separate mechanism to be seeded to metadata
                # if headings is also included in list formats to be considered then
                # headings also go in paragraph content
                if item['type'] == 'heading':
                    # checking for how many previous headings are to be considered to be seeded in metadata
                    if len(headings) <headings_level:
                        item['page'] = page['page']
                        headings.append(item)
                    else:
                        # remove the first element from headings to accomodate new headings
                        headings.pop(0)
                        item['page'] = page['page']
                        headings.append(item)
                # table of contents are also collected separately
                if item['type'] == 'tableOfContent':
                    table_of_contents.append({'content':item['content'],
                                            'page':page['page']})
                # collect elements data if its one of the permitted formats
                if item['type'] in formats:
                    # collect metadata
                    metadata = {}
                    # add heading info in metadata
                    if headings:
                        placeholder = []
                        for i,heading in enumerate(reversed(headings)):
                            placeholder.append({f'headings_{i}':{'content':heading['content'], 'page':heading['page'],'level':heading['level']}})
                    else:
                        placeholder = []
                    if placeholder:
                        metadata['headings'] =  placeholder   
                    else:
                        metadata['headings'] = []   
                    # add page numner info           
                    metadata['page'] = page['page']
                    # add filename
                    metadata['document_name'] = filename
                    # if item type is table collect columns info
                    if item['type'] == 'table':
                        metadata['columns'] = item['columns']
                    
                    # add paragraph with metadata to paragraphs list
                    metadata['type'] = item['type']
                    paragraphs.append({'content':item['content'],'metadata':metadata})

    logging.info(f"Paragraphs count in {filename}:{len(paragraphs)}")
    
    # Running paragraph sanitization in terms of token count, iteratively 
    paragraphs_count_check= False
    while paragraphs_count_check == False:
        para_counts = len(paragraphs)
        paragraphs = paragraph_sanitize(paragraphs=paragraphs,lower_threshold=lower_threshold,
                                    upper_threshold=upper_threshold)
        paragraphs_count_check = len(paragraphs) == para_counts

    logging.info(f"Paragraphs count in {filename} after sanitization:{len(paragraphs)}")
    

    # running tables sanitization
    paragraphs = table_sanitize(paragraphs=paragraphs,token_limit=upper_threshold)
    logging.info(f"Paragraphs count in {filename} after table sanitization:{len(paragraphs)}")
    
    return {'paragraphs':paragraphs, 'table_of_contents':table_of_contents}