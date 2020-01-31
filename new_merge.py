#####################################################################
#####                                                           #####
#####   Merging der STRM3-Daten und anderer Daten mit den       #####
#####   EV-Profilen                                             #####
#####                                                           #####
#####   Author:      David Rößler (2020)                        #####
#####   Last edited:    01/27/2020                              #####
#####                                                           #####
#####################################################################
import pandas as pd
import georasters as gr
import geopandas as geopd
import os
import numpy as np
import requests
import datetime as dt

### Daten https://arxiv.org/abs/1905.02081
os.chdir('/home/daveknave/PycharmProjects/geomerge/data')
#%%
datadf = pd.read_csv('merged_data_ev_only.csv')
datadf =  datadf.rename(columns=dict([(c, c.replace('[', '_').replace(']', '_')) for c in datadf.columns]))
# https://librenepal.com/article/reading-srtm-data-with-python/
SAMPLES = 1201 # Change this to 3601 for SRTM1

with open('N42W084.SRTMGL3.hgt/N42W084.hgt', 'rb') as hgt_data:
    # Each data is 16bit signed integer(i2) - big endian(>)
    elevations = np.fromfile(hgt_data, np.dtype('>i2'), SAMPLES * SAMPLES).reshape((SAMPLES, SAMPLES))
    hgt_data.close()
#%%
x,y = datadf['Latitude_deg_'], datadf['Longitude_deg_']
lat_row = np.round((x - np.floor(x)) * (SAMPLES - 1), 0).astype(int)
lon_row = np.round((y - np.floor(y)) * (SAMPLES - 1), 0).astype(int)

datadf['Elevation'] = elevations[(SAMPLES - 1 - lat_row).tolist(), lon_row.tolist()].astype(int)

datadf.to_csv('data_ev_only_elevation.csv', index=False)
uniquedf = datadf.drop_duplicates(subset=['Latitude_deg_', 'Longitude_deg_'])
uniquedf.loc[:,['Latitude_deg_', 'Longitude_deg_', 'Elevation']].to_csv('unique_points.csv', index=False)
# %%
base_url = 'https://www.ncei.noaa.gov/access/services/data/v1'
parameters = {
    'dataset' : 'daily-summaries',
    'startDate' : (dt.timedelta(days=datadf['DayNum'].min()-1) + dt.datetime.strptime('2017-11-01', '%Y-%m-%d')).strftime('%Y-%m-%d'),
    'endDate' : (dt.timedelta(days=30-1) + dt.datetime.strptime('2017-11-01', '%Y-%m-%d')).strftime('%Y-%m-%d'),
    # 'endDate' : (dt.timedelta(days=datadf['DayNum'].max()-1) + dt.datetime.strptime('2017-11-01', '%Y-%m-%d')).strftime('%Y-%m-%d'),
    'boundingBox' : ','.join([
        str(datadf['Latitude_deg_'].max().round(3)),
        str(datadf['Longitude_deg_'].min().round(3)),
        str(datadf['Latitude_deg_'].min().round(3)),
        str(datadf['Longitude_deg_'].max().round(3)),
    ]),
    'dataTypes' : 'WIND_DIR,WIND_SPEED',
    'format' : 'csv',
    'units' : 'metric',
    'stations' : 'USW00094847,USW00094847,USW00094847'


}
result = requests.get(base_url, parameters)
#%%
from io import StringIO
res = pd.read_csv(StringIO(result.text))
print(parameters)

#%%
import haversine
stationsdf = pd.read_csv('7890488/city_info.csv', parse_dates=[4, 5], infer_datetime_format=True, index_col=0)
stationsdf['Stn.edDate'] = pd.to_datetime(stationsdf['Stn.edDate'], format='%Y-%m-%d')
stationsdf['Stn.stDate'] = pd.to_datetime(stationsdf['Stn.stDate'], format='%Y-%m-%d')

datadf = pd.read_csv('data_ev_only_elevation.csv')
#%%
def find_closest_station(x):
    global stationsdf
    closest_station_id = ''
    closest_station_name = ''
    min_dist = float('inf')
    for key, station in stationsdf.iterrows():
        latest_date = dt.timedelta(days=x['DayNum'].max()-1) + dt.datetime.strptime('2017-11-01', '%Y-%m-%d')
        earliest_date = dt.timedelta(days=x['DayNum'].min()-1) + dt.datetime.strptime('2017-11-01', '%Y-%m-%d')
        if station['Stn.edDate'] < earliest_date  or station['Stn.stDate'] > latest_date: continue
        dist = haversine.haversine((float(x['lat'].iloc[0]), float(x['lon'].iloc[0])), (float(station['Lat']), float(station['Lon'])))
        print(dist)
        if dist < min_dist:
            min_dist = dist
            closest_station_id = station['ID']
            closest_station_name = station['Name']
            print(closest_station_id)

    x['StID'] = closest_station_id
    x['StName'] = closest_station_name

    return x
#%%
datadf[['lat', 'lon']] = datadf[['Latitude_deg_','Longitude_deg_']].astype(float).round(2)
#%%
datadf = datadf.groupby(['lat','lon'], as_index=False).apply(lambda x: find_closest_station(x))
#%%
datadf.to_csv('data_ev_only_elevation_incl_ws.csv', index=False)
#%%
# https://kilthub.cmu.edu/articles/Compiled_daily_temperature_and_precipitation_data_for_the_U_S_cities/7890488
datadf = pd.read_csv('data_ev_only_elevation_incl_ws.csv')
for ws in datadf['StID'].unique().tolist():
    weatherdf = pd.read_csv('7890488/' + ws + '.csv')
