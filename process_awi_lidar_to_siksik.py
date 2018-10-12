'''
I tried using dask.distributed and it is indeed faster than this pandas chunk method,
but I was not able to find a way to dump the dask results to local file.

It seems that dask wanted to dump each worker's result to a different .part file
before merging into the output .txt file but couldn't manage to create those files
before error-ing out

May also be a Windows-related issue...
'''
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path

ROOT_DIR = Path('.')
lidar_file = ROOT_DIR / 'TVC_ALS_201609.txt'
siksik_shp = ROOT_DIR / 'SikSik_shp' / 'SikSik.shp'

# get SikSik site geometry, bounding box
siksik = gpd.read_file(str(siksik_shp))
siksik_geom = siksik.geometry[0]
minx, miny, maxx, maxy = siksik.total_bounds

# output subset txt file
subset_txt = ROOT_DIR / 'TVC_ALS_201609_SikSik_subset.txt'
if subset_txt.exists():
    subset_txt.unlink()
header = 'X[m],Y[m],Z[m],Amplitude[DN],EchoWidth[ns],EchoType[DN],' + \
         'TerrainProbability[DecimalFraction],RelativeHeight[m],' + \
         'Class[DN],PointSourceId[DN]\n'
with subset_txt.open('w') as subset:
    subset.write(header)

# number of dataframe rows per chunk
chunksize = 512000

# may be useful if memory issues are encountered
## explicit dtypes that are still pretty generous
#dtype = {
#   'X[m]': 'float32',
#   'Y[m]': 'float32',
#   'Z[m]': 'float32',
#   'Amplitude[DN]': 'int32',
#   'EchoWidth[ns]': 'float32',
#   'EchoType[DN]': 'int32',
#   'TerrainProbability[DecimalFraction]': 'float32',
#   'RelativeHeight[m]': 'float32',
#   'Class[DN]': 'uint8', # can be one of [1,2,3,4,5]
#   'PointSourceId[DN]': 'uint32' 
#}

for chunk in pd.read_csv(str(lidar_file), delim_whitespace=True, chunksize=chunksize):
    # first pass, check against siksik bounding box
    df = chunk.loc[
        (chunk['X[m]'] >= minx) &
        (chunk['X[m]'] <= maxx) &
        (chunk['Y[m]'] >= miny) &
        (chunk['Y[m]'] <= maxy)
    ]
    if df.size > 0:
        # bounding box match console indicator
        print('*', end='', flush=True) 
        # second pass, check points for being within siksik polygon
        geoms = [Point(xy) for xy in zip(df['X[m]'], df['Y[m]'])]
        gdf = gpd.GeoDataFrame(
            data=df,
            geometry=geoms,
            crs={'init':'epsg:32608'} # UTM 8N WGS84
        )
        gdf = gdf.loc[gdf.geometry.within(siksik_geom)]
        if gdf.size > 0:
            # geometry match console indicator
            print('+', end='', flush=True)
            # append to SikSik_subset text file
            gdf.drop('geometry', axis=1).to_csv(
                str(subset_txt), index=False, header=False, 
                mode='a', float_format='%.3f'
            )
    # still-alive console indicator
    print('.', end='', flush=True)
print('\nDone!')
