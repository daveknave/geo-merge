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
from sklearn.metrics.pairwise import haversine_distances
# import haversine
#%%
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

        if dist < min_dist:
            min_dist = dist
            closest_station_id = station['ID']
            closest_station_name = station['Name']

    x['StID'] = closest_station_id
    x['StName'] = closest_station_name

    return x
#%%
datadf[['lat', 'lon']] = datadf[['Latitude_deg_','Longitude_deg_']].astype(float).round(2)
#%%
datadf = datadf.groupby(['lat','lon'], as_index=False).apply(lambda x: find_closest_station(x))
#%%
datadf['Date'] = pd.to_timedelta(datadf['DayNum'].astype(int)+1, unit='d') + dt.datetime.strptime('2017-11-01', '%Y-%m-%d')
datadf.to_csv('data_ev_only_elevation_incl_ws.csv', index=False)
#%%
# https://kilthub.cmu.edu/articles/Compiled_daily_temperature_and_precipitation_data_for_the_U_S_cities/7890488
datadf = pd.read_csv('data_ev_only_elevation_incl_ws.csv')
for ws in datadf['StID'].unique().tolist():
    weatherdf = pd.read_csv('7890488/' + str(ws) + '.csv', index_col=0, parse_dates=[0])
    datadf = datadf.merge(weatherdf, on='Date')

datadf.to_csv('data_ev_complete_weather.csv', index=False)
# %%
datadf = pd.read_csv('data_ev_complete_weather.csv').drop_duplicates()
def kantenmodell(d):
    ### Elevation Change
    d['elev_delta'] = d['Elevation'].shift(-1) - d['Elevation']
    ### State of Charge Change
    d['soc_delta'] = d['HV Battery SOC_%_'].shift(-1) - d['HV Battery SOC_%_']
    ### Distance
    concated =  pd.concat([
        d[['Latitude_deg_','Longitude_deg_']].shift(-1).astype(float).add_suffix('_to').reset_index(drop=True),
        d[['Latitude_deg_','Longitude_deg_']].astype(float).add_suffix('_from').reset_index(drop=True)], axis=1
    )
    dist_matrix = haversine_distances(concated[['Latitude_deg__from', 'Longitude_deg__from']], concated[['Latitude_deg__to', 'Longitude_deg__to']]) * 6371000/1000
    d['distance'] = [dist_matrix[i,i] for i in range(dist_matrix.shape[0]) if i < dist_matrix.shape[1] - 1] + [np.nan]
    print(dist_matrix)
    return d

complete = datadf.head(10000).groupby(['Trip'], as_index=False).apply(lambda y: kantenmodell(y))
['DayNum', 'VehId', 'Trip', 'Timestamp(ms)', 'Latitude_deg_',
       'Longitude_deg_', 'Vehicle Speed_km/h_', 'MAF_g/sec_',
       'Engine RPM_RPM_', 'Absolute Load_%_', 'OAT_DegC_', 'Fuel Rate_L/hr_',
       'Air Conditioning Power_kW_', 'Air Conditioning Power_Watts_',
       'Heater Power_Watts_', 'HV Battery Current_A_', 'HV Battery SOC_%_',
       'HV Battery Voltage_V_', 'Short Term Fuel Trim Bank 1_%_',
       'Short Term Fuel Trim Bank 2_%_', 'Long Term Fuel Trim Bank 1_%_',
       'Long Term Fuel Trim Bank 2_%_', 'Elevation', 'lat', 'lon', 'StID',
       'StName', 'Date', 'tmax', 'tmin', 'prcp', 'elev_delta', 'soc_delta',
       'distance']

renames = {
        'Latitude_deg_' : 'lat',
       'Longitude_deg_' : 'lon',
        'Vehicle Speed_km/h_': 'speed',
        'Air Conditioning Power_kW_' : 'ac_power_kw', 
        'Air Conditioning Power_Watts_' : 'ac_power_w',
        'Heater Power_Watts_' : 'heater_w',
        'HV Battery Current_A_' : 'hv_current_a',
        'HV Battery SOC_%_' : 'hv_soc_percent',
        'HV Battery Voltage_V_' : 'hv_voltage_v',
        'Elevation' : 'elevation_m'
}
complete = complete.drop(columns=[
    'MAF_g/sec_',
    'Engine RPM_RPM_', 'Absolute Load_%_', 'OAT_DegC_', 'Fuel Rate_L/hr_',
    'Short Term Fuel Trim Bank 1_%_',
    'Short Term Fuel Trim Bank 2_%_', 'Long Term Fuel Trim Bank 1_%_',
    'Long Term Fuel Trim Bank 2_%_', 'lat', 'lon',
]).rename(columns=renames)