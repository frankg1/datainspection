def getMeanPoint(shift_points):
        #print 'orignal shifted points'
        #print shift_points
        points=shift_points.tolist()
        sumx=0.0
        sumy=0.0
        for point in points:
            sumx+=point[0]
            sumy+=point[1]
        x=sumx/len(points)
        y=sumy/len(points)
        return [x,y]