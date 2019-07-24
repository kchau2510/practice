import pandas as pd


def readUserItemLatLongData():
    df = pd.read_csv('./data/user-item-latlong.csv')  
    df = df.convert_objects(convert_numeric = True)
    return df 

def readUserItemLocationData():
    df = pd.read_csv('./data/user-item-location.csv')  
    df = df.convert_objects(convert_numeric = True)
    return df 

def readLocationLocationMapping():
    df = pd.read_csv('data/LocationLocationMapping.csv', encoding='latin-1')
    return df

def readLocationCorrelation():
    df = pd.read_csv("data/location-location-data.csv", index_col="location")
    return df

def readProductDetails():
    df = pd.read_csv('data/productDetails.csv', index_col="productId", encoding='latin-1')
    return df

def getUserItemLatLongData():
    return useritemLatLongDF

def getUserItemLocationData():
    return useritemLocationDF

def getLocationLocationmapping():
    return locationMappingDF

def getLocationCorrelation():
    return locationCorrelationDF

def getProductDetails():
    return productDetailsDF

def saveUserItemLocationData():
    newUserItemLocationDF = useritemLatLongDF.copy()
    newUserItemLocationDF["location"] = newUserItemLocationDF['lat'].astype(str) + '-' + newUserItemLocationDF['long'].astype(str)
    newUserItemLocationDF.to_csv('data/user-item-location.csv',index = False)

def saveLocationLocationMapping(locationLocationMapping):
    locationLocationMapping.to_csv('data/LocationLocationMapping.csv',index = False)    #creates the output.csv

useritemLatLongDF = readUserItemLatLongData()
useritemLocationDF = readUserItemLocationData()
locationMappingDF = readLocationLocationMapping()
locationCorrelationDF = readLocationCorrelation()
productDetailsDF = readProductDetails()