import pandas as pd
import georasters as gr
import os
#%%
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
west = gr.from_file(tiffdir + '/' + 'gebco_08_rev_elev_A1_grey_geo.tif')
east = gr.from_file(tiffdir + '/' + 'gebco_08_rev_elev_B1_grey_geo.tif')
#%%
datadf =  datadf.rename(columns=dict([(c, c.replace('[', '_').replace(']', '_')) for c in datadf.columns]))
#%%
import utm
def get_elevation(d):

    print(d['Latitude_deg_'], d['Longitude_deg_'])
    r =  utm.from_latlon(d['Latitude_deg_'], d['Longitude_deg_'])
    x, y = r[0:2]
    print(r)
    return east.map_pixel(x,y)

datadf['Elevation'] = datadf.apply(lambda x: get_elevation(x), axis=1)
#%%
gr.Multi
