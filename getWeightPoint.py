def getWeightPoint(lis):
    if len(lis)==1:
        return lis[0]
    area = 0.0
    x,y = 0.0,0.0

    a = len(lis)
    for i in range(a):
        lat = lis[i][0]     #weidu
        lng = lis[i][1]      #jingdu

        if i == 0:
            lat1 = lis[-1][0]
            lng1 = lis[-1][1]
        else:
            lat1 = lis[i-1][0]
            lng1 = lis[i-1][1]

        fg = (lat*lng1 - lng*lat1)/2.0

        area += fg
        x += fg*(lat+lat1)/3.0
        y += fg*(lng+lng1)/3.0
    x = x/area
    y = y/area
    return [x,y]