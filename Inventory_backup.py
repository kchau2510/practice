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
import requests
from  functools import lru_cache
import datetime
import random


list_zip_codes = []
dictionary_states_zip = {}
dictionary_store_zip = {}
list_target_zip_codes = []
path_to_file = 'D:/Order Optimizer/Order Data/Order Optimizer/Feb2019.csv'
path_to_store_details = 'D:/Order Optimizer/StoreData_US.xlsx'
capacitythreshhold = 100
inventory = 1
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
    dictionary_sku_store = get_sku_store_from_solr()

    for (index, row) in data.iterrows():
        zip_code = (row['DELIVERY_ZIP_CODE'])[0:5]
        state = row['STATE']
        date = (row['SUBMITTED_DATE'])[0:9]
        time = (row['SUBMITTED_DATE'])[10:18]
        sku_id = row['SKU_ID']

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
            list_target_zip_codes.append(str(get_warehouse_details(zip_code[:1])))
        else:
          list_zip_codes = dictionary_states_zip.get(state).split(',')
          if sku_id not in dictionary_sku_store:
           list_target_zip_codes.append(str(get_warehouse_details(zip_code[:1])))
          else:
           if dictionary_sku_store[sku_id] == 'Inventory Not Found':
            list_target_zip_codes.append(str(get_warehouse_details(zip_code[:1])))
           else:
            list_of_stores = dictionary_sku_store[sku_id]
            nearest_zip_code = min(list_zip_codes, key=lambda x: abs(int(x) - int(zip_code)))
            count1 = 0
            if str(nearest_zip_code)+str(sku_id) not in dictionary_zip_count or dictionary_zip_count.get(str(nearest_zip_code)+str(sku_id)) < capacitythreshhold:
                for store in list_of_stores:
                  if str(store) in dictionary_store_zip.keys():
                    zip_of_store = dictionary_store_zip[str(store)]
                    if store == 'Inventory Not Found':
                        list_target_zip_codes.append(str(get_warehouse_details(zip_code[:1])))
                    else:
                        if zip_of_store == nearest_zip_code:
                         list_target_zip_codes.append(str(nearest_zip_code))
                         count1 = count1+1
                         dictionary = increase_count(str(nearest_zip_code), dictionary_zip_count)
                         dictionary_zip_count = dictionary
                         dictionary_inventory = increase_count_inventory(str(nearest_zip_code)+str(sku_id), dictionary_zip_count)

                         break
                        else:
                         #list_target_zip_codes.append(str(get_warehouse_details(zip_code)))
                         continue

                  #list_target_zip_codes.append(str(get_warehouse_details(zip_code[:1])))
            else:
                  list_zip_codes = list(dict.fromkeys(list_zip_codes))
                  list_zip_codes.remove(str(nearest_zip_code))
                  nearest_zip_code = min(list_zip_codes, key=lambda x: abs(int(x) - int(zip_code)))
                  for store in list_of_stores:
                      if str(store) in dictionary_store_zip.keys():
                          zip_of_store = dictionary_store_zip[str(store)]
                          if store == 'Inventory Not Found':
                              list_target_zip_codes.append(str(get_warehouse_details(zip_code[:1])))
                              count1 = count1 + 1
                          else:
                              if zip_of_store == nearest_zip_code:
                                  list_target_zip_codes.append(str(nearest_zip_code))
                                  count1 = count1 + 1
                                  dictionary = increase_count(str(nearest_zip_code)+str(sku_id), dictionary_zip_count)
                                  dictionary_zip_count = dictionary
                                  dictionary_zip_count_inventory = increase_count_inventory(str(nearest_zip_code) + str(sku_id),dictionary_zip_count_inventory)
                                  break
                              else:
                                continue
            if count1 == 0:
                list_target_zip_codes.append(str(get_warehouse_details(zip_code[:1])))



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
    for batch in range(0, round(number_of_batches)):
    #for batch in range(1, 3):
        if batch == 0:
            vals = '|'.join(map(str, sku_list[:100]))
            sku_size = 100
        else:
            start = sku_size+1
            end = sku_size+1+99
            sku_size = end
            vals = '|'.join(map(str, sku_list[start:end]))

        solr_end_point = 'https://www.bedbathandbeyond.com/api/apollo/collections/sku/query-profiles/bbb-sku/select?web3feo=abc&sku=' + vals.strip()
        user_agent = {'User-agent': 'Googlebot/2.1 (+http://www.googlebot.com/bot.html)'}
        response = requests.get(solr_end_point)
        for data_item in response.json()['response']['docs']:
            if 'STORES' in data_item:
             sku_stores = data_item['STORES']
             sku_id = data_item['SKU_ID']
             dictionary_sku_store[sku_id] = sku_stores
            else:
             sku_id = data_item['SKU_ID']
             dictionary_sku_store[sku_id] = ['Inventory Not Found']

    #MyCache.update(dictionary_sku_store, dictionary_sku_store)
    return dictionary_sku_store


def get_warehouse_details(zip_code):
    destzipcode = ''
    initials = zip_code
    if int(initials) in naveda:
        destzipcode = '89131'
    else:
        if int(initials) in georgia:
            destzipcode = '30567'
        else:
            if int(initials) in texas:
                destzipcode = '75067'
            else:
                if int(initials) in new_jersey:
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

def increase_count_inventory(zip_code,dictionary_zip_count):
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

        dictionary_store_zip[store] = postal_code



def test():
    list_zip_codes = dictionary_states_zip.get('CA').split(',')
    print(list_zip_codes)
    list_zip_codes.remove("95020")
    print(list_zip_codes)


process_all_records()
#dictionary_sku_store = get_sku_store_from_solr()
#print(dictionary_sku_store)



class MyCache:
    """"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.cache = {}
        self.max_cache_size = 10

    # ----------------------------------------------------------------------
    def __contains__(self, key):
        """
        Returns True or False depending on whether or not the key is in the
        cache
        """
        return key in self.cache

    # ----------------------------------------------------------------------
    def update(self, key, value):
        """
        Update the cache dictionary and optionally remove the oldest item
        """
        if key not in self.cache and len(self.cache) >= self.max_cache_size:
            self.remove_oldest()

        self.cache[key] = {'date_accessed': datetime.datetime.now(),
                           'value': value}

    # ----------------------------------------------------------------------
    def remove_oldest(self):
        """
        Remove the entry that has the oldest accessed date
        """
        oldest_entry = None
        for key in self.cache:
            if oldest_entry is None:
                oldest_entry = key
            elif self.cache[key]['date_accessed'] < self.cache[oldest_entry][
                'date_accessed']:
                oldest_entry = key
        self.cache.pop(oldest_entry)

    # ----------------------------------------------------------------------
    @property
    def size(self):
        """
        Return the size of the cache
        """
        return len(self.cache)

cache = MyCache()
def test():

  print(cache.size)
  if 'dictionary_sku_store' in cache:
      print(key is present)
      print(dictionary_sku_store)
  else:
      print("Not present")
      #get_sku_store_from_solr()
      cache.update('dictionary_sku_store', 'Test data')
      print(cache.size)

#test()


