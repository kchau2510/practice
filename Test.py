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
path_to_file = 'D:/Order Optimizer/Order Data/Order Optimizer/Feb2019.csv'
path_to_store_details = 'D:/Order Optimizer/StoreData_US.xlsx'
capacitythreshhold = 9
naveda = [5, 8, 9]
texas = [3, 6, 7]
georgia = [2, 3, 4]
new_jersey = [0, 1]
warehouse = ['89131', '75067', '30567', '07094']

@with_goto
def process_all_records():
    """Read Delivery code & state from excel and checked against stored zip codes & states. If the zip code exist in
    list,that becomes the source zip code , else all the available zip codes are fetched for the state and nearest
     zip code is considered as source zip code. These zip codes are then exported to Targeted Excel"""
    dictionary_zip_count = {}
    data = pd.read_csv(path_to_file, encoding='latin-1')
    data = data.dropna()
    query_store_data()
    count = 1
    previous_date = 0
    temp_previous_date = 0

    for (index, row) in data.iterrows():
        zip_code = (row['DELIVERY_ZIP_CODE'])[0:5]
        state = row['STATE']
        date = (row['SUBMITTED_DATE'])[0:9]
        time = (row['SUBMITTED_DATE'])[10:18]

        # Reset Dictionary when order submission date changes
        if index == 0:
            previous_date = date
            temp_previous_date = date
        else:
            previous_date = temp_previous_date
            temp_previous_date = date
        if str(temp_previous_date) != str(previous_date):
            dictionary_zip_count.clear()



        # Logic to get the nearest store/warehouse node depending on capacity
        if not dictionary_states_zip.__contains__(state):
            print('State does not exist in store information '+state)
            list_target_zip_codes.append(str(get_warehouse_details(zip_code)))
            # dictionary = increase_count(zip_code,dictionary_zip_count)
            # dictionary_zip_count = dictionary
        else:
            list_zip_codes = dictionary_states_zip.get(state).split(',')
            nearest_zip_code = min(list_zip_codes, key=lambda x: abs(int(x) - int(zip_code)))
            if dictionary_zip_count.__contains__(nearest_zip_code) and nearest_zip_code not in warehouse:
                if dictionary_zip_count.get(nearest_zip_code) <= capacitythreshhold:
                    list_target_zip_codes.append(str(nearest_zip_code))
                    dictionary = increase_count(nearest_zip_code, dictionary_zip_count)
                    dictionary_zip_count = dictionary
                else:
                    list_target_zip_codes.append(str(get_warehouse_details(zip_code)))
                    # dictionary = increase_count(zip_code,dictionary_zip_count)
                    # dictionary_zip_count = dictionary
            else:
                list_target_zip_codes.append(str(nearest_zip_code))
                dictionary = increase_count(nearest_zip_code, dictionary_zip_count)
                dictionary_zip_count = dictionary

    print(list_target_zip_codes)
    print(dictionary_zip_count)
    series_target_zip_codes = pd.Series(list_target_zip_codes)
    series_target_zip_codes.index = data.index
    data['Source_ZIP'] = series_target_zip_codes
    data.to_csv(path_to_file, chunksize=1000)


@lru_cache(maxsize=1)
def get_sku_store_from_solr():
    data = pd.read_csv(path_to_file, encoding='latin-1')
    data = data.dropna()
    query_store_data()
    sku_ids = data['SKU_ID']
    sku_list = sku_ids.to_string(header=False, index=False).split('\n')
    sku_list = list(dict.fromkeys(sku_list))
    dictionary_sku_store = {}
    number_of_batches = len(sku_list) / 100
    for batch in range(1, round(number_of_batches)):
    #for batch in range(1, 3):
        if batch == 1:
            vals = '|'.join(map(str, sku_list[:100]))
            sku_size = 100
        else:
            start = sku_size+1
            end = sku_size+1+99
            sku_size = end
            vals = '|'.join(map(str, sku_list[start:end]))

        solr_end_point = 'https://et01-www.bbbyapp.com/api/apollo/collections/sku/query-profiles/bbb-sku/select?web3feo=abc&sku=' + vals.strip()
        user_agent = {'User-agent': 'Googlebot/2.1 (+http://www.googlebot.com/bot.html)'}
        response = requests.get(solr_end_point)
        for data_item in response.json()['response']['docs']:
            if 'STORES' in data_item:
             sku_stores = data_item['STORES']
             sku_id = data_item['SKU_ID']
             dictionary_sku_store[sku_id] = sku_stores
            else:
             dictionary_sku_store[sku_id] = ['Inventory Not Found']

    #MyCache.update(dictionary_sku_store, dictionary_sku_store)
    print(dictionary_sku_store)
    return dictionary_sku_store


def get_warehouse_details(zip_code):
    destzipcode = ''
    initials = zip_code
    if initials in naveda:
        destzipcode = '89131'
    else:
        if initials in georgia:
            destzipcode = '30567'
        else:
            if initials in texas:
                destzipcode = '75067'
            else:
                if initials in new_jersey:
                  destzipcode = '07094'
                else:
                  destzipcode = min(warehouse, key=lambda x: abs(int(x) - int(zip_code)))
    return destzipcode


def increase_count(zip_code,dictionary_zip_count):
    # Increase the count by 1 if the delivery has already been made by particular node
    if dictionary_zip_count.__contains__(zip_code):
        dictionary_zip_count[zip_code] = dictionary_zip_count.get(zip_code) + 1
    else:
        dictionary_zip_count[zip_code] = 1
    return dictionary_zip_count




def query_store_data():
    """This method fetches store details(available Zip codes & States) from CSV , prepares list of postal codes
    and dictionary containing state as key and zip code as value"""

    data = pd.read_excel(path_to_store_details, encoding='utf-F',
                          use_iterators=True, read_only=True, dtype=str)
    list_zip_codes = data['POSTAL_CD'].astype(str).values.tolist()
    states = data['STATE'].astype(str).values.tolist()
    for (index, row) in data.iterrows():
        postal_code = str(row['POSTAL_CD'])[0:5]
        state = row['STATE']
        store = row['STORE_ID']
        if dictionary_states_zip.__contains__(state):
            dictionary_states_zip[state] = str(dictionary_states_zip[state]) \
                + ',' + postal_code
        else:
            dictionary_states_zip[state] = postal_code

        if dictionary_store_zip.__contains__(store):
            dictionary_store_zip[store] = str(dictionary_store_zip[store]) \
                                           + ',' + postal_code
        else:
            dictionary_store_zip[store] = postal_code





process_all_records()


