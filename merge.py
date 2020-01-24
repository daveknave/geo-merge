import pandas as pd
import georasters as gr
import geopandas as geopd
import os
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
datadf = pd.read_csv('merged_data_ev_only.csv')
datadf =  datadf.rename(columns=dict([(c, c.replace('[', '_').replace(']', '_')) for c in datadf.columns]))
#%%
a1 = gr.from_file(tiffdir + '/' + 'gebco_08_rev_elev_A1_grey_geo.tif')
b1 = gr.from_file(tiffdir + '/' + 'gebco_08_rev_elev_B1_grey_geo.tif')
c1 = gr.from_file(tiffdir + '/' + 'gebco_08_rev_elev_C1_grey_geo.tif')
d1 = gr.from_file(tiffdir + '/' + 'gebco_08_rev_elev_D1_grey_geo.tif')
a2 = gr.from_file(tiffdir + '/' + 'gebco_08_rev_elev_A2_grey_geo.tif')
#%%
raster = gr.union([b1,c1,d1,a1,a2])
#%%
import utm

x,y = (datadf['Latitude_deg_'], datadf['Longitude_deg_'])
datadf['Elevation'] = raster.map_pixel(x, y)
#%%
datadf.to_csv('data_ev_only_elevation.csv', index=False)
#%%
datadf = pd.read_csv('data_ev_only_elevation.csv')
#%%
uniquedf = datadf.drop_duplicates(subset=['Latitude_deg_', 'Longitude_deg_'])
uniquedf.loc[:,['Latitude_deg_', 'Longitude_deg_', 'Elevation']].to_csv('unique_points.csv', index=False)