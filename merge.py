import pandas as pd
import georasters as gr
import geopandas as geopd
import os
import numpy as np
### https://arxiv.org/abs/1905.02081
os.chdir('/home/daveknave/PycharmProjects/geomerge/data')
datadir = 'VED'
tiffdir = 'geotiff'

#%%
weeks = os.listdir(datadir)
datadf = pd.DataFrame()

for w in weeks:
    loaddf = pd.read_csv(datadir + '/' + w)
    datadf = datadf.append(loaddf)
#%%
datadf.to_csv('merged_data.csv', index=False)
#%%
car_descr = pd.read_excel('VED-master/Data/VED_Static_Data_PHEV&EV.xlsx')
#%%
datadf = datadf[datadf['VehId'].isin(car_descr['VehId'])]
datadf.to_csv('merged_data_ev_only.csv', index=False)

#%%
a1 = gr.from_file(tiffdir + '/' + 'gebco_08_rev_elev_A1_grey_geo.tif')
b1 = gr.from_file(tiffdir + '/' + 'gebco_08_rev_elev_B1_grey_geo.tif')
c1 = gr.from_file(tiffdir + '/' + 'gebco_08_rev_elev_C1_grey_geo.tif')
d1 = gr.from_file(tiffdir + '/' + 'gebco_08_rev_elev_D1_grey_geo.tif')
a2 = gr.from_file(tiffdir + '/' + 'gebco_08_rev_elev_A2_grey_geo.tif')
b2 = gr.from_file(tiffdir + '/' + 'gebco_08_rev_elev_B2_grey_geo.tif')
c2 = gr.from_file(tiffdir + '/' + 'gebco_08_rev_elev_C2_grey_geo.tif')
d2 = gr.from_file(tiffdir + '/' + 'gebco_08_rev_elev_D2_grey_geo.tif')
raster1 = gr.union([a1, b1,c1,d1])
raster2 = gr.union([a2,b2, c2, d2])
#%%
raster1.nodata_value = -999
raster2.nodata_value = -999
raster1.to_tiff(tiffdir + '/' + 'A1_B1_C1_D1')
raster2.to_tiff(tiffdir + '/' + 'A2_B2_C2_D2')
#%%
raster1 = gr.from_file(tiffdir + '/' + 'A1_B1_C1_D1.tif')
raster2 = gr.from_file(tiffdir + '/' + 'A2_B2_C2_D2.tif')
#%%
raster = gr.union([raster1, raster2])
raster.to_tiff(tiffdir + '/' + 'all.tif')
#%%
datadf = pd.read_csv('merged_data_ev_only.csv')
datadf =  datadf.rename(columns=dict([(c, c.replace('[', '_').replace(']', '_')) for c in datadf.columns]))
# raster = gr.from_file(tiffdir + '/' + 'A1_B1_C1_D1_A2.tif.tif')
#%%
x,y = datadf['Latitude_deg_'], datadf['Longitude_deg_']
datadf['Elevation'] = raster.map_pixel(x.tolist(), y.tolist())
#%%
datadf.to_csv('data_ev_only_elevation.csv', index=False)
#%%
datadf = pd.read_csv('data_ev_only_elevation.csv')
#%%
uniquedf = datadf.drop_duplicates(subset=['Latitude_deg_', 'Longitude_deg_'])
uniquedf.loc[:,['Latitude_deg_', 'Longitude_deg_', 'Elevation']].to_csv('unique_points.csv', index=False)

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
