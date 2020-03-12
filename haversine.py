# -*- coding: utf-8 -*-
from math import sin,radians,cos,asin,sqrt

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2]) #radians 角度转弧度
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) # 反正弦
    r = 6371
    return c * r

# if __name__=='__main__':
#    print(haversine(108.415,22.886111,108.417,22.886111))