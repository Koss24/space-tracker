from skyfield.api import load, wgs84, EarthSatellite

stations_file = '/home/L/Programming/dataEng/spaceApp/spacTrackAPI/TLEData.txt'
satellites = load.tle_file(stations_file)
ts = load.timescale()
t = ts.now()
count = 0
print('Loaded', len(satellites), 'satellites')

for satellite in satellites:
    geocentric = satellite.at(t)
    lat,lon = wgs84.latlon_of(geocentric)
    print('Latitude:', lat)
    print('Longitude:', lon)
    count += 1
print(count)

#geocentric = satellites

'''
ts = load.timescale()
line1 = '1 25544U 98067A   22187.68301169  .00005411  00000-0  10327-3 0  9991'
line2 = '2 25544  51.6431 240.0331 0004472 340.5411 165.5537 15.49847551348208'

satellite = EarthSatellite(line1,line2, "ISS",ts)
print(satellite)
t = ts.now()
geocentric = satellite.at(t)
lat,lon = wgs84.latlon_of(geocentric)
print('Latitude:', lat)
print('Longitude:', lon)

curl -c cookies.txt -b cookies.txt https://www.space-track.org/ajaxauth/login -d 'identity=lkosedy24@gmail.com&password=895qPWiGqv6D3cK63bzhEjP'

ty = time.time()
print(np.round_(time.time()-ty,3),'sec')
'''