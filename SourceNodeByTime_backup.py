#!/usr/bin/python
# -*- coding: utf-8 -*-
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
from goto import with_goto

list_zip_codes = []
dictionary_states_zip = {}
list_target_zip_codes = []
dictionary_zip_count = {}
path_to_file = 'D:/Order Optimizer/Order Data/Order Optimizer/US.csv'
path_to_store_details = 'D:/Order Optimizer/StoreData_US.xlsx'
capacitythreshhold = 10
naveda = [5, 8, 9]
texas = [3, 6, 7]
georgia = [2, 3, 4]
new_jersey = [0, 1]

@with_goto
def process_all_records():
    """Read Delivery code & state from excel and checked against stored zip codes & states. If the zip code exist in
    list,that becomes the source zip code , else all the available zip codes are fetched for the state and nearest
     zip code is considered as source zip code. These zip codes are then exported to Targeted Excel"""

    data = pd.read_csv(path_to_file, encoding='latin-1')
    data = data.dropna()
    query_store_data()
    print('list of states ')
    print(dictionary_states_zip)
    count = 1
    previous_date = 0
    temp_previous_date = 0

    for (index, row) in data.iterrows():
        zip_code = (row['DELIVERY_ZIP_CODE'])[0:5]
        state = row['STATE']
        submittedDate = row['SUBMITTED_DATE']
        date = (row['SUBMITTED_DATE'])[0:9]
        time = (row['SUBMITTED_DATE'])[10:18]
        if index == 0:
            previous_date = date
            temp_previous_date = date
        else:
            previous_date = temp_previous_date
            temp_previous_date = date


        # Reset Dictionary when order submission date changes

        if str(temp_previous_date) != str(previous_date):
            dictionary_zip_count.clear()

        # if zip_code in zip code dictionary:

        dictionary_zip_count1 = dictionary_zip_count
        if dictionary_states_zip.__contains__(str(zip_code)):
            if dictionary_zip_count[str(zip_code)] <= capacitythreshhold:
                destzipcode = zip_code
        else:
            if dictionary_states_zip.get(row['STATE']) is None:
                if dictionary_zip_count[nearest_zip_code] <= capacitythreshhold:
                 destzipcode = zip_code
                else:
                    goto .destZipZero
            else:
                target_zip_code = dictionary_states_zip.get(row['STATE']).split(',')
                label .getzipcode
                if not target_zip_code:
                 goto .destZipZero
                else:
                 nearest_zip_code = min(target_zip_code, key=lambda x: abs(int(x) - int(zip_code)))
                if dictionary_zip_count.__contains__(str(nearest_zip_code)):
                    if dictionary_zip_count[nearest_zip_code] <= capacitythreshhold:
                        destzipcode = nearest_zip_code
                    else:
                        target_zip_code.remove(nearest_zip_code)
                        goto .getzipcode
                else:
                    destzipcode = nearest_zip_code
                    if capacitythreshhold > 10:
                     print('capacity' + capacitythreshhold)
                     print(destzipcode)

        label .destZipZero
        if not destzipcode :
         print('Empty Dest')
         initials = row['DELIVERY_ZIP_CODE'][0]
         if naveda.__contains__(initials):
            destzipcode = '89131'
         else:
            if georgia.__contains__(initials):
               destzipcode = '30567'
            else:
                if texas.__contains__(initials):
                    destzipcode = '75067'
                else:
                    destzipcode = '07094'

        list_target_zip_codes.append(destzipcode)

        # Increase the count by 1 if the delivery has already been made by particular node

        if dictionary_zip_count.__contains__(str(destzipcode)):
            count = int(count) + 1
            dictionary_zip_count[str(destzipcode)] = int(count) + 1
        else:
            count = count
            dictionary_zip_count[str(destzipcode)] = int(count)
        count = count

    series_target_zip_codes = pd.Series(list_target_zip_codes)
    series_target_zip_codes.index = data.index
    data['Source_ZIP'] = series_target_zip_codes
    data.to_csv(path_to_file, chunksize=1000)


def query_store_data():
    """This method fetches store details(available Zip codes & States) from CSV , prepares list of postal codes
    and dictionary containing state as key and zip code as value"""

    data = pd.read_excel(path_to_store_details, encoding='utf-F',
                         use_iterators=True, read_only=True)
    list_zip_codes = data['POSTAL_CD'].astype(str).values.tolist()
    states = data['STATE'].astype(str).values.tolist()
    for (index, row) in data.iterrows():
        postal_code = str(row['POSTAL_CD'])[0:5]
        state = row['STATE']
        if dictionary_states_zip.__contains__(state):
            dictionary_states_zip[state] = dictionary_states_zip[state] \
                + ',' + postal_code
        else:
            dictionary_states_zip[state] = postal_code


process_all_records()
