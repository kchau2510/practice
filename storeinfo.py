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
path_to_file = "D:/Order Optimizer/US.xls"
connection_string = '''bbb_core/bbb_core@(DESCRIPTION=
                                            (ADDRESS_LIST=
                                                (ADDRESS=
                                                    (PROTOCOL=TCP)
                                                    (HOST=DelvmpllBBAB13.sapient.com)
                                                    (PORT=1521)
                                                )
                                            )
                                            (CONNECT_DATA=
                                                (SID=BBBDEV)
                                            )
                                        )'''

"""This is currently unused method."""
def target(zipcode , state):
    zipcode = zipcode[0:4]
    connection = cx_Oracle.connect(connection_string)
    cursor = connection.cursor()
    querystring = "select * from bbb_store where postal_Cd = :zipCode"
    cursor.prepare(querystring)
    cursor.execute(None, {'zipCode': zipcode})
    result = cursor.fetchall()
    cursor.close()

    if len(result) == 0:
        querybystate = "select postal_cd from bbb_store where state = :state"
        cursor = connection.cursor()
        cursor.prepare(querybystate)
        cursor.execute(None, {'state': state})
        stateresult = cursor.fetchall()
        cursor.close()
        connection.close()
        # convert List of truple to list of string
        listofstring = res = [''.join(i) for i in stateresult]
        destzipcode = min(listofstring, key=lambda x: abs(int(x) - int(zipcode)))
        return destzipcode
    connection.close()


def process_all_records():
    """Read Delivery code & state from excel and checked against stored zip codes & states. If the zip code exist in
    list,that becomes the source zip code , else all the available zip codes are fetched for the state and nearest
     zip code is considered as source zip code. These zip codes are then exported to Targeted Excel"""
    data = pd.read_excel(path_to_file, encoding='utf-F')
    zip_codes = data['DELIVERY_ZIP_CODE'].astype(str).values.tolist()
    states = data['STATE'].astype(str).values.tolist()
    query_store_data()
    for index, row in data.iterrows():
        zip_code = row['DELIVERY_ZIP_CODE'][0:5]
        if zip_code in list_zip_codes:
            destzipcode = zip_code
            list_target_zip_codes.append(zip_code)
        else:
             if dictionary_states_zip.get(row['STATE']) is None:
                 list_target_zip_codes.append(zip_code)
             else:
              target_zip_code = dictionary_states_zip.get(row['STATE']).split(",")
              destzipcode = min(target_zip_code, key=lambda x: abs(int(x) - int(zip_code)))
              list_target_zip_codes.append(destzipcode)

    series_target_zip_codes = pd.Series(list_target_zip_codes)
    series_target_zip_codes.index = data.index
    data['Source_ZIP'] = series_target_zip_codes
    export_excel = data.to_excel(path_to_file, index=None, header=True)


def query_store_data():
    """This method fetches store details(available Zip codes & States) from database , prepares list of postal codes
    and dictionary containing state as key and zip code as value"""
    connection = cx_Oracle.connect(connection_string)
    cursor = connection.cursor()
    querystring = "select state , postal_cd from bbb_store group by postal_cd , state"
    cursor.execute(querystring)
    field_map = fields(cursor)
    for row in cursor:
        postal_code = row[field_map['POSTAL_CD']][0:5]
        list_zip_codes.append(postal_code)
        state = row[field_map['STATE']]
        if dictionary_states_zip.__contains__(state):
         dictionary_states_zip[state] = dictionary_states_zip[state]+','+postal_code
        else:
         dictionary_states_zip[state] = postal_code
    cursor.close()
    connection.close()


def fields(cursor):
    """ Given a DB API 2.0 cursor object that has been executed, returns
    a dictionary that maps each field name to a column index; 0 and up. """
    results = {}
    column = 0
    for d in cursor.description:
        results[d[0]] = column
        column = column + 1
    return results


process_all_records()















