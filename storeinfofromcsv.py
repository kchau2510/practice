import cx_Oracle
import functools
import operator
from bisect import bisect
import itertools
from functools import reduce
import xlrd
import pandas as pd
import openpyxl
import xlwt



list_zip_codes = []
dictionary_states_zip = {}
list_target_zip_codes = []
path_to_file = "D:/Order Optimizer/US.csv"
path_to_store_details = "D:/Order Optimizer/StoreData_US.xlsx"


def process_all_records():
    """Read Delivery code & state from excel and checked against stored zip codes & states. If the zip code exist in
    list,that becomes the source zip code , else all the available zip codes are fetched for the state and nearest
     zip code is considered as source zip code. These zip codes are then exported to Targeted Excel"""
    data = pd.read_csv(path_to_file,encoding='latin-1')
    data = data.dropna()
    query_store_data()
    for index, row in data.iterrows():
        zip_code = str(row['DELIVERY_ZIP_CODE'])[0:5]
        state = row['STATE']
        if state in ('TX', 'OK', 'NM', 'AR', 'LA'):
         list_target_zip_codes.append('75067')
         continue
        else:
         if state in ('PA', 'NY', 'NJ', 'CT', 'RI'):
          list_target_zip_codes.append('07094')
          continue
         else:
          if state in ('GA', 'FL', 'AL', 'SC', 'NC', 'TN'):
           list_target_zip_codes.append('30567')
           continue
          else:
           if state in ('CA', 'AZ', 'UT', 'ID', 'OR'):
            list_target_zip_codes.append('89131')
            continue

        if zip_code in list_zip_codes:
            list_target_zip_codes.append(zip_code)
        else:
             if dictionary_states_zip.get(row['STATE']) is None:
                list_target_zip_codes.append(zip_code)
             else:
              target_zip_code = dictionary_states_zip.get(row['STATE']).split(",")
              destzipcode = min(target_zip_code, key=lambda x: abs(int(x) - int(zip_code)))
              list_target_zip_codes.append(destzipcode)

    #list_chunks = list(chunks(list_target_zip_codes, 5))
    series_target_zip_codes = pd.Series(list_target_zip_codes)
    series_target_zip_codes.index = data.index
    data['Source_ZIP'] = series_target_zip_codes
    data.to_csv(path_to_file, chunksize=1000)

    # for chunk in data:
    #     chunk.to_csv(os.path.join(path_to_file),
    #                  columns=[['Source_ZIP']],mode='a')


# Create a function called "chunks" with two arguments, l and n:
def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]

def query_store_data():
    """This method fetches store details(available Zip codes & States) from CSV , prepares list of postal codes
    and dictionary containing state as key and zip code as value"""
    data = pd.read_excel(path_to_store_details, encoding='utf-F', use_iterators=True, read_only=True)
    list_zip_codes = data['POSTAL_CD'].astype(str).values.tolist()
    states = data['STATE'].astype(str).values.tolist()
    for index, row in data.iterrows():
        postal_code = str(row['POSTAL_CD'])[0:5]
        state = row['STATE']
        if dictionary_states_zip.__contains__(state):
         dictionary_states_zip[state] = dictionary_states_zip[state]+','+postal_code
        else:
         dictionary_states_zip[state] = postal_code



process_all_records()















