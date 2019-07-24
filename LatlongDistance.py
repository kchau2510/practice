import pandas as pd
import numpy as np
import requests

def dist_from_coordinates(lat1, lon1, lat2, lon2):
  R = 6371  # Earth radius in km
  #conversion to radians
  d_lat = np.radians(lat2-lat1)
  d_lon = np.radians(lon2-lon1)
  r_lat1 = np.radians(lat1)
  r_lat2 = np.radians(lat2)
  #haversine formula
  a = np.sin(d_lat/2.) **2 + np.cos(r_lat1) * np.cos(r_lat2) * np.sin(d_lon/2.)**2
  haversine = 2 * R * np.arcsin(np.sqrt(a))
  return haversine

def records_within_radius(df, lat1, long1, radius):
    df2 = pd.DataFrame(data=None, columns=df.columns)
    df2['distance'] = None
    for index,row in df.iterrows():
      lat2 = row['lat'] #first row of location.lat column here
      long2 = row['long'] #first row of location.long column here
      distance = dist_from_coordinates(lat1, long1, lat2, long2)  #get the distance
      #print(index,distance)
      if radius > distance:
          new_row = row
          new_row['distance'] = distance
          df2.loc[-1] = new_row
          df2.index = df2.index + 1
    df2 = df2.sort(['distance'], ascending=[1])
    return df2

def users_within_radius_df(df, lat1, long1, radius):
    df2 = pd.DataFrame(data=None, columns=['user','distance'])
    for index,row in df.iterrows():
      lat2 = row['lat'] #first row of location.lat column here
      long2 = row['long'] #first row of location.long column here
      distance = dist_from_coordinates(lat1, long1, lat2, long2)  #get the distance
      #print(index,distance)
      if radius > distance:
          new_row = [row['user'], distance]
          df2.loc[-1] = new_row
          df2.index = df2.index + 1
    df2 = df2.sort(['distance'], ascending=[1])
    return df2

def users_within_radius_csv(df, lat1, long1, radius):
    csv = '';
    for index,row in df.iterrows():
      lat2 = row['lat'] #first row of location.lat column here
      long2 = row['long'] #first row of location.long column here
      distance = dist_from_coordinates(lat1, long1, lat2, long2)  #get the distance
      #print(index,distance)
      if radius > distance:
          csv = csv +','+ row['user']
    return csv

def get_latlong_from_ip(ip):
    #https://extreme-ip-lookup.com/json/63.70.164.200?callback=getIP
    response = requests.get("https://extreme-ip-lookup.com/json/" + ip + "?callback=getIP")
    print(response.content)
    #data = response.json()
    #print(data)
    #print(data.lat)
    #return [data.lat, data.lon] 
    return [28.626164,77.035561]

def records_within_radius_ip(df, ip, radius):
    latlong = get_latlong_from_ip(ip)
    return records_within_radius(df, latlong[0], latlong[1], radius)

input_file = "user-latlong-data.csv"
df = pd.read_csv(input_file)  
df = df.convert_objects(convert_numeric = True)

def get_records(radius,ip):
    #df2 = records_within_radius(df, 28.626164, 77.035561, 15)
    df2 = records_within_radius_ip(df, ip, radius)
    #print(df2)
    #output_file = "C:/Data/workspace/python/Howathon/user-latlong-data-distance1.csv"
    #df2.to_csv(output_file,index = False)    #creates the output.csv
    return df2

def get_user_user_mapping_byradius(radius):
    unique_users_df = df.drop_duplicates('user')
    columns = ['user','nearby_users']
    user_nearbyuser_df = pd.DataFrame(data=None, columns=columns)
    for index,row in unique_users_df.iterrows():
      lat = row['lat'] #first row of location.lat column here
      long = row['long'] #first row of location.long column here
      nearby_users_csv = users_within_radius_csv(unique_users_df, lat, long, radius)
      new_row = [row['user'], nearby_users_csv]
      user_nearbyuser_df.loc[-1] = new_row
      user_nearbyuser_df.index = user_nearbyuser_df.index + 1
    return user_nearbyuser_df

array = df['user'].unique()
#print(array)
#unique_users_df = df.loc[df['user'].isin(array)]
#print(unique_users_df)
#unique_users_df = df.drop_duplicates('user')
#print(unique_users_df)


#https://www.datascience.com/learn-data-science/fundamentals/introduction-to-correlation-python-data-science
    