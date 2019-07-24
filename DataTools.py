import pandas as pd

def createLocationData():
    userItemDF = pd.read_csv('./data/Prod_Data.csv')  
    userItemDF = userItemDF.convert_objects(convert_numeric = True)
    
    mappingDF = pd.read_csv('./data/userlatlongmapping.csv')
    userLatDict = dict(zip(mappingDF['user'], mappingDF['lat']))
    userLongDict = dict(zip(mappingDF['user'], mappingDF['long']))
    
    userItemLatLongDF = pd.DataFrame(data=None, columns=userItemDF.columns)
    userItemLatLongDF['lat'] = None
    userItemLatLongDF['long'] = None
        
    for index,row in userItemDF.iterrows():
        new_row = row
        new_row['lat'] = userLatDict.get(row['user'])
        new_row['long'] = userLongDict.get(row['user'])
        userItemLatLongDF.loc[-1] = new_row
        userItemLatLongDF.index = userItemLatLongDF.index + 1
    userItemLatLongDF.to_csv('data/user-item-latlong.csv',index = False)


userItemLocationDF = pd.read_csv('./data/user-item-latlong.csv')
userItemLocationDF = userItemLocationDF.convert_objects(convert_numeric = True)
userItemLocationDF["location"] = userItemLocationDF['lat'].astype(str) + '-' + userItemLocationDF['long'].astype(str)
userItemLocationDF.to_csv('data/user-item-location.csv',index = False)
