import math


class Haversine:
    '''
    use the haversine class to calculate the distance between
    two lon/lat coordnate pairs.
    output distance available in kilometers, meters, miles, and feet.
    example usage: Haversine([lon1,lat1],[lon2,lat2]).feet

    See: https://nathanrooy.github.io/posts/2016-09-07/haversine-with-python/

    ''

    def __init__(self, lon1, lat1, lon2, lat2):

        lon1, lat1 = lon1, lat1
        lon2, lat2 = lon2, lat2

        R = 6371000                               # radius of Earth in meters
        phi_1 = math.radians(lat1)
        phi_2 = math.radians(lat2)

        delta_phi=math.radians(lat2-lat1)
        delta_lambda=math.radians(lon2-lon1)

        a = math.sin(delta_phi/2.0)**2+ math.cos(phi_1)*math.cos(phi_2) * math.sin(delta_lambda/2.0)**2

        c=2*math.atan2(math.sqrt(a),math.sqrt(1-a))

        self.meters=R*c                         # output distance in meters
        self.km=self.meters/1000.0              # output distance in kilometers
        self.miles=self.meters*0.000621371      # output distance in miles
        self.feet=self.miles*5280               # output distance in feet



# haversine = Haversine((-84.412977,39.152501),(-84.412946,39.152505))
#
# print(haversine.km)
