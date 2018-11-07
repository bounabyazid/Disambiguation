#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  3 08:58:10 2018

@author: polo
"""
import re

import math
import time
import emoji

import numpy as np
import editdistance

import pandas as pd

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

    root = ET.parse(urllib.request.urlopen(requestURL)).getroot()
    Potent_Locations = []
    
    items = root.findall('geoname')
    for item in items:
        if item:
           Potent_Locations.append([item.find('name').text,item.find('countryName').text,
                                [item.find('lat').text,item.find('lng').text]])
    
    return Potent_Locations
#_______________________________________________________________

def give_emoji_free_text(text):
    allchars = [str for str in text]
    emoji_list = [c for c in allchars if c in emoji.UNICODE_EMOJI]
    clean_text = ' '.join([str for str in text.split() if not any(i in str for i in emoji_list)])
    return clean_text

#_______________________________________________________________

def Remove_punctuations(text):
    SymbList =  [':', '.', '¿', '?', ',', '…', ';', '/', '-', '¡', '!', '|', 'ブ', 'レ', 'イ', 'ジ', 'ン', 'グ', 'ス', 'タ', 'ー', '+', '’', '–', '=', '“', '”', '‘', '„', '*', '_', '´']
    return ''.join(c for c in text if c not in SymbList)
#_______________________________________________________________

def split_uppercase(value):
    S = re.sub(r'([A-Z][a-z]+)', r' \1', value)
    return re.sub(r'([A-Z]+)', r' \1', S)

#_______________________________________________________________
    
def LOAD_DATA():
    
    start_time = time.time()
    
    df = pd.read_csv('UK_Tweets/old_tweets.csv',delimiter=',',encoding='utf-8')

    Tweet = df['text'].tolist()
    latitude = df['latitude'].tolist()
    longitude = df['longitude'].tolist()
    timestamp = df['timestamp'].tolist()
    
    df3 = pd.DataFrame({'Tweet': Tweet,'latitude':latitude,'longitude' : longitude, 'timestamp': timestamp, 'Hashtags': '', 'Potent_Locations': 'NA','Full_Location_Name': 'NA','Lat_Long' : '[]'})

    df3['Hashtags'] = df3['Tweet'].str.findall(r"#(\w+)").apply(' '.join)
    df3['Hashtags'] = df3['Hashtags'].apply(split_uppercase)
    df3['Hashtags'] = df3['Hashtags'].str.replace('[0-9]','')
    df3['Hashtags'] = df3['Hashtags'].str.replace(' +',' ', regex = True)
    df3['Hashtags'] = df3['Hashtags'].apply(lambda x: x.strip())
    df3['Hashtags'] = df3['Hashtags'].apply(lambda x: x.lower())
    
    df2 = pd.read_csv('Gazetteer_uk.tsv',delimiter='\t',encoding='utf-8',names = ["Locations", "Lat", "Long"])
    df2['Locations'] = df2['Locations'].str.replace('[0-9]','')
    df2['Locations'] = df2['Locations'].str.replace(' +',' ', regex = True)
    df2['Locations'] = df2['Locations'].str.replace('-',' ')
    df2['Locations'] = df2['Locations'].apply(lambda x: x.lower())
    end_time = time.time()

    print('...........DATAFRME HAS BEEN LOADED AFTER = %s SECONDS',end_time - start_time,'...........')

    return df,df2,df3
#_______________________________________________________________
    
def Edit_Distance_Threshold(hashtag,Locations,Threshold,cords,df2):
    EDs = []
    poten_locs = []
    j = 0
    for loc in Locations:
        ED = editdistance.eval(hashtag,loc)
        if ED <= Threshold:
           EDs.append([ED,j])
        j+=1
    SPHERIC_DISTs = []
    for ED in EDs:
        j = ED[1]
        cords2 = [df2.at[j,'Lat'],df2.at[j,'Long']]
        SPHERIC_DISTs.append(distance_on_unit_sphere(cords, cords2))
    if len(EDs)>0:
       index_min = np.argmin(SPHERIC_DISTs)
       ED = EDs[index_min]
       j = ED[1]
       poten_locs.append([ED[0],SPHERIC_DISTs[index_min],hashtag,Locations[j],df2.at[j,'Lat'],df2.at[j,'Long']])
    return poten_locs
#_______________________________________________________________

def HASHTAG_LOCATION_MATCHING(df,df2):
   
    Locations = df2.Locations.tolist()
    Hashtags = df.Hashtags.tolist()
    
    i=0    
    for hashtag in Hashtags:
        M_locs = []
        cords1 = [df3.at[i,'latitude'],df3.at[i,'longitude']]
        if len(hashtag.split()) == 1:
           ED = Edit_Distance_Threshold(hashtag,Locations,1,cords1,df2)
           if ED:
              M_locs.append(ED)
        elif len(hashtag.split()) > 1:
             for token in hashtag.split():
                 ED = Edit_Distance_Threshold(token,Locations,1,cords1,df2)
                 if ED:
                    M_locs.append(ED)
        if M_locs:
           df.at[i,'Potent_Locations'] = M_locs
        i+=1
    df.to_csv('DISAMBIGUATED LOCATIONS UK.tsv',sep='\t')
#_______________________________________________________________

df,df2,df3 = LOAD_DATA()

HASHTAG_LOCATION_MATCHING(df3,df2)
