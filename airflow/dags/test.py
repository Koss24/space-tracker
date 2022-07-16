from skyfield.api import load, wgs84, EarthSatellite
import numpy as np
import pandas as pd
import time
import os 
from pyspark import SparkContext

data = pd.read_json('/home/L/Programming/dataEng/spaceApp/airflow/dags/tempSatelliteData.json')
print(data.head())

newData = data.filter(['tle_line0', 'tle_line1', 'tle_line2'])
newData.to_csv('test.txt', sep='\n', index=False)

stations_file = 'test.txt'
satellites = load.tle_file(stations_file)
ts = load.timescale()
t = ts.now()

print(satellites)
data = pd.DataFrame(data=satellites)
data = data.to_numpy()


def normal_for():
    ty = time.time()
    for satellite in satellites:
        geocentric = satellite.at(t)
        lat,lon = wgs84.latlon_of(geocentric)
        print('Latitude:', lat)
        print('Longitude:', lon)
    print(np.round_(time.time()-ty,3),'sec')


sc = SparkContext()

rdd = sc.parallelize(satellites)

print(rdd.take(5))

