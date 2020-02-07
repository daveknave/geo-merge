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
import os
import numpy as np
import requests
import datetime as dt
### Daten https://arxiv.org/abs/1905.02081
os.chdir('/home/daveknave/PycharmProjects/geomerge/data')
from sklearn.metrics.pairwise import haversine_distances
from sklearn.linear_model import LinearRegression
import haversine
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
    return d

complete = datadf.groupby(['Trip'], as_index=False).apply(lambda y: kantenmodell(y))
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

complete.to_csv('data_ev_complete_weather_edges.csv', index=False)
#%%

#%%
complete = pd.read_csv('data_ev_complete_weather_edges.csv').drop_duplicates()
complete['motion_idx'] = 0
last_loc_change_idx = 0
last_trip = 0
new_block = False
for ridx, ro in complete.iterrows():
    if ro['Trip'] != last_trip: ### Neuer Trip startet automatisch einen neuen Laufindex
        last_trip = ro['Trip']
        last_loc_change_idx = ridx


    if new_block:
        last_loc_change_idx = ridx
        new_block = False


    complete.loc[ridx,'motion_idx'] = last_loc_change_idx

    if ro['distance'] > 0:
        new_block = True
complete.to_csv('data_ev_complete_weather_edges_moidx.csv', index=False)
#%%
complete = pd.read_csv('data_ev_complete_weather_edges_moidx.csv')
def aggregate_motion(d):
    x = {
        'speed' : d['speed'].mean(),
        'distance' : d['distance'].max(),
        'soc_delta': d['soc_delta'].sum(),
        'elev_delta': d['elev_delta'].sum(),
        'hv_current_a': d['hv_current_a'].mean(),
        'hv_voltage_v': d['hv_voltage_v'].mean(),
        'speed': d['speed'].iloc[0]
    }
    x.update(d.drop(columns=['distance', 'soc_delta', 'elev_delta', 'hv_current_a', 'hv_voltage_v']).iloc[-1].to_dict())

    lr_model = LinearRegression(fit_intercept=False)
    lr_model.fit(d[['speed']], d.reset_index(drop=True).reset_index()[['index']])

    x['accel'] = lr_model.coef_[0][0]


    return pd.Series(data=x)
final_aggregate_rows = complete.groupby('motion_idx', as_index=False).apply(lambda d: aggregate_motion(d))
final_aggregate_rows.to_csv('final_aggregate_rows.csv', index=False)