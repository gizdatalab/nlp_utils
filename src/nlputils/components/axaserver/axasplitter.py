import glob
import pandas as pd
import os


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