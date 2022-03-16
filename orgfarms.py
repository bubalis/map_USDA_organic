#!/usr/bin/env python
# coding: utf-8

# In[300]:


import os

import requests
import zipfile
import subprocess

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

from shapely.geometry import Point
from datetime import datetime
from bs4 import BeautifulSoup
from fiona import listlayers
from random import uniform

#%%

zcta_url = 'https://www2.census.gov/geo/tiger/TIGER2010DP1/ZCTA_2010Census_DP1.zip'


def get_most_recent_data():
    '''Download most recent spreadsheet of the USDA organic integrity database.
    '''
    
    
    res = requests.get('https://organic.ams.usda.gov/Integrity/Reports/DataHistory.aspx')
    soup = BeautifulSoup(res.text)
    
    soup = BeautifulSoup(res.text)
    links = soup.findAll('a')
    month = datetime.now().strftime('%B')
    links2 = []
    for link in links:
        if link.text == month and link.get('href'):
            links2.append(link)
            
    stem = 'https://organic.ams.usda.gov/Integrity'
    url = stem+links2[0].get('href')[2:]
    
    r = requests.get(url) 
    open('temp.xlsx', 'wb').write(r.content)
    
    return pd.read_excel('temp.xlsx', engine = 'xlrd' , skiprows = [1,2])


def download_census_map():
    url = 'https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_tract_500k.zip'
    fn = url.split('/')[-1]
    #subprocess.run(['curl', 
    #'https://www2.census.gov/geo/tiger/TGRGDB21/tlgdb_2021_a_us_block.gdb.zip',
    #'--output',
    #'tlgdb_2021_a_us_block.gdb.zip'            ])
    
    r = requests.get(url)
    open(fn, 'wb').write(r.content)
    
    
    
    with zipfile.ZipFile(fn, 'r') as zip_ref:
        zip_ref.extractall(fn.split('.')[0])
    os.remove(fn)
    
    
    #file = os.path.join('tlgdb_2021_a_us_block', 'tlgdb_2021_a_us_block.gbd')
   # gdf = gpd.read_file(file, layer = listlayers(file)[0] )
    
    #gdf['tract'] = gdf['GEOID'].apply(lambda x: x[:11])
    #gdf = gdf.dissolve(by = 'tract').reset_index()[['tract', 'geometry']]
    
    #gdf.to_file('census_map')
    #return gdf



def get_random_point_in_polygon(poly):
    '''
    Return a point at a random point within the polygon.
    

    Parameters
    ----------
    poly : a shapely polygon
        polygon to put a point into

    Returns
    -------
    p : a shapely point, at a random location within the polygon.

    '''
    
    
    minx, miny, maxx, maxy = poly.bounds
    while True:
        p = Point(uniform(minx, maxx), uniform(miny, maxy))
        if poly.contains(p):
            return p











def intToZip(num):
    '''Format a number as a zipcode string.'''
    
    
    zipcode = str(num)
    while len(zipcode)<5:
        zipcode = '0'+zipcode
    return zipcode




def add_leadingZeros(num, len_ = 11):
    '''Add leading Zeros to a number and return it as a string with the length:
        len_
    

    Parameters
    ----------
    num : a number
    
    len_ : an integer, length of the string to be returned.
        DESCRIPTION. The default is 11.

    Returns
    -------
    string : The number as a string, with leading 0s added.
    '''
    
    
    
    string = str(num)
    if len(string) >= len_:
        pass
    
    else:
        dif = len_ - len(string)
        string = '0'* dif + string
    
    return string


#%%



def zipAssigner(row):
    '''Get the zipcode for the row, and clip it to 5 characters.'''
    
    z =  row['Physical Address: ZIP/Postal Code']
    if z:
        return z[:5]
    else:
        return row['Mailing Address: ZIP/Postal Code'][:5]



usa_str = 'United States of America'

if __name__ == '__main__':
    
    
    if not os.path.exists('cb_2020_us_tract_500k'):
       print("Downloading the Census Map")
       download_census_map()
    else:
        #gdf = gpd.read_file('census_map')
        pass
    
    gdf = gpd.read_file('cb_2020_us_tract_500k')
    df = get_most_recent_data()
    
    df = df.fillna('')
    
    
    basemap = gpd.read_file(os.path.join("states_21basic","states.shp"))
    
    #zip_gdb = os.path.join('USA_Census_Tract_Boundaries', 'v10' , "tracts.gdb")
    #gdf = gpd.read_file(zip_gdb, layer = listlayers(zip_gdb)[-1])
    gdf.to_crs(basemap.crs, inplace = True)
    
    
    zip_df = pd.read_csv("ZIP_TRACT_032019.csv", dtype={'zip': str})
    
    zip_df['zip'] =  zip_df['zip'].apply(intToZip)
    zip_df['tract'] =  zip_df['tract'].apply(add_leadingZeros)



    
    #filter to just USA farms
    usa = df[(df['Physical Address: Country'
       ].str.contains(usa_str))| 
       (df['Mailing Address: Country'].str.contains(usa_str))]
    
    
    #merge together and format zipcode tract names/data
    usa['zip'] = usa.apply(zipAssigner, axis=1)
    
    usa = usa.merge(zip_df[['tract', 'zip']], on='zip', how='left')
    
    usa = usa.drop_duplicates(subset = usa.columns[:-1]) 
    usa['tract'] = usa['tract'].apply(add_leadingZeros)
    
    
    
    usa = gpd.GeoDataFrame(usa.merge(gdf[['GEOID', 'geometry']], 
                                     left_on = 'tract', right_on = 'GEOID'))
    
    
    
    #filter to only farms.
    farms = usa[(usa['LIVESTOCK  Certification Status']!='') | 
                (usa['CROPS  Certification Status']!='')]
    
    
    #Assign all farms a class: Crops, Livestock or Mixed
    farms['Livestock'] = farms['LIVESTOCK  Certification Status']=='Certified'
    farms['Crops'] = farms['CROPS  Certification Status']=='Certified'
    farms['Class'] = farms['Crops'] + farms['Livestock']*2
    farms['Class'] = farms['Class'].replace(
        {1: 'Crops', 2: 'Livestock', 3: 'Mixed'})
    farms = farms[farms['Class']!=0] #excludes farms that have been suspended
    
    
    #create column for count of farms in each ziptract
    tract_counts = farms.groupby('tract')[['Class']].count()
    tract_counts.columns = ['tract_count']
    
    farms = farms.merge(tract_counts, left_on = 'tract', right_index = True)
    
    
    
    #assign a point for each farm
    #If farm is the only certified farm in the tract, use the centroid.
    #otherwise, use a random point. 
    #Note that doing it this way is MUCH faster than
    # generating a random point for each farm
    
        
    center_points = farms.loc[farms['tract_count'] == 1, 'geometry'].centroid
    
    
    random_points = farms.loc[farms['tract_count'] > 1, 'geometry'].apply(
                 get_random_point_in_polygon)
    points = pd.concat([center_points, random_points])
    
    
    farms.geometry = points
    
    
    
    
    
    
    #%%
    
    #paramaters for the figure.
    cmap = plt.cm.viridis
    fig = plt.figure(figsize=(20,10))
    
    gs = fig.add_gridspec(30, 20)
    
    
    non_contig = '|'.join(['Alaska', 'Hawaii','Puerto Rico', ])
    
    #subset the lower-48
    farms_48 = farms[
  (~farms['Physical Address: State/Province'].str.contains(non_contig))  & 
  (~farms['Mailing Address: State/Province'].str.contains(non_contig))]
    
    lower_48 = basemap[~basemap["STATE_NAME"].str.contains(non_contig)]
    
    ax = fig.add_subplot(gs[0:, 0:])
    lower_48.plot(ax=ax, color='white', edgecolor='black')
    farms_48.plot(ax=ax, markersize=4, 
                  column='Class', cmap=cmap, legend=True, legend_kwds={
            'title': 'Cerfication Scope', 'fontsize': 'xx-large', 'markerscale': 3, 
            'title_fontsize':'xx-large', 'loc': 'lower right'})
    
   #now = datetime.now()
   #month = now.strftime('%B')
   #yr =  now.year
    
    #plt.title(f'Certified Organic Farms in the US as of {month} {yr}',
    #          fontsize=40)
    
    plt.tight_layout()
    ax.axis('off')
    
    
    
    
    # Plot non-contiguous Areas of the US
    locs = [gs[21:, 0:6,], gs[25:, 4:8], gs[13:,17:]]
    for state, loc in zip(non_contig.split('|')[:-1], locs[:-1]):
        print(state)
        ax = fig.add_subplot(loc)
        geom = basemap[basemap["STATE_NAME"] == state]
        geom.plot(ax = ax, color = 'white', edgecolor = 'black')
        selector = (farms['Physical Address: State/Province'] == state) | \
                    (farms['Mailing Address: State/Province']==state)
        
        subset = farms[selector]
        
        subset = subset[subset['geometry'].apply(
            lambda x: geom['geometry'].iloc[-1].contains(x))]
        
        subset.plot(ax = ax, markersize = 4, column = 'Class', cmap = cmap)
        ax.axis('off')
        ax.set_xticks([])
        ax.set_yticks([])
    
    #save the data and the plot.
    
    plt.savefig("map_of_organic_farms.png")
    plt.show()
    farms.to_csv("all_farms.csv")
#%%



