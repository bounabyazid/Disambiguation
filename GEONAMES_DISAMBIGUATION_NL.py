#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  3 08:58:10 2018

@author: Yazid Bounab
"""
import re
import ast

import math
import time

import numpy as np

import pandas as pd

import urllib
import requests

import urllib.request
import urllib.parse
from xml.etree import ElementTree as ET
from unidecode import unidecode

#http://geonames.nga.mil/gns/html/namefiles.html?fbclid=IwAR0GbMoxnn2cu_wMqifVH94SmujWScFWplGVKFIizsnM647xTcO7dftJxwc
#https://www.tutorialspoint.com/How-to-convert-string-representation-of-list-to-list-in-Python
#https://gist.github.com/nickjevershed/6480846

#_______________________________________________________________

def distance_on_unit_sphere(cords1,cords2):
    lat1, long1 = cords1
    lat2, long2 = cords2
    radius = 6371
    # spherical coordinates in radians.
    degrees_to_radians = math.pi/180.0
        
    # phi = 90 - latitude
    phi1 = (90.0 - lat1)*degrees_to_radians
    phi2 = (90.0 - lat2)*degrees_to_radians
        
    # theta = longitude
    theta1 = long1*degrees_to_radians
    theta2 = long2*degrees_to_radians
        
  
    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) + math.cos(phi1)*math.cos(phi2))
    arc = math.acos( cos )

    km = arc * radius
    return km
#_______________________________________________________________

def get_potential_locations(location,user_name,fuzzy_score):
    requestURL = 'http://api.geonames.org/search?' + 'q=' + location \
                 + '&fuzzy=' + fuzzy_score + '&username=' + user_name
    #print (requestURL)

    XML = requests.get(requestURL, stream=True)
    tree = ET.parse(XML.raw)
    root = tree.getroot()
    
    Total_Rsults = 0
    for totalResultsCount in root.iter('totalResultsCount'):
        Total_Rsults = int(totalResultsCount.text)

    Potent_Locations = []
    
    if Total_Rsults > 0:
       items = root.findall('geoname')
       for item in items:
           if item:
              Potent_Locations.append([location,unidecode(item.find('name').text),item.find('countryName').text,
                                      [float(item.find('lat').text),float(item.find('lng').text)]])
    
    return Potent_Locations
#_______________________________________________________________

def split_uppercase(value):
    S = re.sub(r'([A-Z][a-z]+)', r' \1', value)
    return re.sub(r'([A-Z]+)', r' \1', S)

#_______________________________________________________________
    
def LOAD_DATA():
    
    start_time = time.time()
    
    df = pd.read_csv('locations_tw_nj.tsv',delimiter='\t',encoding='utf-8')

    df['entities.hashtags'] = df['entities.hashtags'].str.replace('\\','')
    df['entities.hashtags'] = df['entities.hashtags'].str.replace('\"','')
    df['entities.hashtags'] = df['entities.hashtags'].str.replace(',indices:[[0-9]+,[0-9]+]','', regex = True)
    df['entities.hashtags'] = df['entities.hashtags'].str.replace('text:','')
    df['entities.hashtags'] = df['entities.hashtags'].str.replace('[0-9]','')
    df['entities.hashtags'] = df['entities.hashtags'].str.replace('[{}]','')
    df['entities.hashtags'] = df['entities.hashtags'].str.replace('[\[\]]','')

    df['entities.hashtags'] = df['entities.hashtags'].str.replace(',',' ')
    
    df['entities.hashtags'] = df['entities.hashtags'].astype(str)
    df['entities.hashtags'] = df['entities.hashtags'].apply(split_uppercase)
    
    df['entities.hashtags'] = df['entities.hashtags'].str.replace(' +',' ', regex = True)
    df['entities.hashtags'] = df['entities.hashtags'].apply(lambda x: x.strip())
    df['entities.hashtags'] = df['entities.hashtags'].apply(lambda x: x.lower())
    
    IDs = df['X'].tolist()
    Tweet = df['text'].tolist()
    Created_at = df['created_at'].tolist()
    User_location = df['user.location'].tolist()
    Hashtags = df['entities.hashtags'].tolist()
    Geo_type = df['geo.type'].tolist()
    Location_Cords = df['coordinates.coordinates'].tolist()
    
    df2 = pd.DataFrame({'ID': IDs,'Tweet':Tweet,'Created_at' : Created_at, 'User_location': User_location, 'Hashtags': Hashtags, 'User_Cords': Location_Cords,'Geo_type': Geo_type, 'Potent_Locations': 'NA','Full_Location_Name': 'NA','Lat_Long' : '[]'})
    
    end_time = time.time()

    print('...........DATAFRME HAS BEEN LOADED AFTER = %s SECONDS',end_time - start_time,'...........')

    return df,df2
#_______________________________________________________________

def HASHTAG_LOCATION_MATCHING(df):
   
    Hashtags = df.Hashtags.tolist()
    
    i=0
    for hashtag in Hashtags:
        print(i)
        if hashtag:
           
           Locations = []
           SPHERIC_DISTs = []
           cords1 = ast.literal_eval(df.at[i,'User_Cords'])
          
           if len(hashtag.split()) == 1:
              Locations = get_potential_locations(hashtag,'username','0.9')
              for loc in Locations:
                  cords2 = loc[3]#ast.literal_eval(loc[3])
                  SPHERIC_DISTs.append(distance_on_unit_sphere(cords1, cords2))
           elif len(hashtag.split()) > 1:
                for token in hashtag.split():
                    Locations.extend(get_potential_locations(token,'username','0.9'))
                for loc in Locations:
                    cords2 = loc[3]#ast.literal_eval(loc[3])
                    SPHERIC_DISTs.append(distance_on_unit_sphere(cords1, cords2))
                       
           if len(SPHERIC_DISTs) > 0:
              index_min = np.argmin(SPHERIC_DISTs)
              Loc = Locations[index_min]
              df.at[i,'Potent_Locations'] = [SPHERIC_DISTs[index_min],Loc[0],Loc[1],loc[3]]
        i+=1
    df.to_csv('GEONAMES NL DISAMBIGUATED LOCATIONS.tsv',sep='\t')
#_______________________________________________________________

df,df2 = LOAD_DATA()

HASHTAG_LOCATION_MATCHING(df2)
