#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  3 08:58:10 2018

@author: polo
"""
import re
import ast

import math
import time

import numpy as np
import editdistance

from fuzzywuzzy import fuzz

import pandas as pd

from sklearn.metrics import accuracy_score

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
        
    # Compute spherical distance from spherical coordinates.
        
    # For two locations in spherical coordinates 
    # (1, theta, phi) and (1, theta, phi)
    # cosine( arc length ) = 
    #    sin phi sin phi' cos(theta-theta') + cos phi cos phi'
    # distance = rho * arc length
    
    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) + math.cos(phi1)*math.cos(phi2))
    arc = math.acos( cos )

    # Remember to multiply arc by the radius of the earth 
    # in your favorite set of units to get length.
    km = arc * radius
    return km

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
    
    df3 = pd.DataFrame({'ID': IDs,'Tweet':Tweet,'Created_at' : Created_at, 'User_location': User_location, 'Hashtags': Hashtags, 'User_Cords': Location_Cords,'Geo_type': Geo_type, 'Potent_Locations': 'NA','Full_Location_Name': 'NA','Lat_Long' : '[]'})

    df2 = pd.read_csv('Gazetteer_nl.tsv',delimiter='\t',encoding='utf-8',names = ["Locations", "Lat", "Long"])
    df2['Locations'] = df2['Locations'].str.replace('[0-9]','')
    df2['Locations'] = df2['Locations'].str.replace(' +',' ', regex = True)
    df2['Locations'] = df2['Locations'].str.replace('-',' ')
    df2['Locations'] = df2['Locations'].apply(lambda x: x.lower())
    
    end_time = time.time()

    print('...........DATAFRME HAS BEEN LOADED AFTER = %s SECONDS',end_time - start_time,'...........')

    return df,df2,df3
#_______________________________________________________________

def Min_Distance(poten_locs,Geo_Cords):
    return 0
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
        cords1 = ast.literal_eval(df3.at[i,'User_Cords'])
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
    df.to_csv('DISAMBIGUATED LOCATIONS.tsv',sep='\t')
#_______________________________________________________________

df,df2,df3 = LOAD_DATA()

HASHTAG_LOCATION_MATCHING(df3,df2)
