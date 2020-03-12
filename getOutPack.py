from scipy.spatial import ConvexHull

def convex_hull(points):
    #receive a numpy array   return a list
    hull = ConvexHull(points)
    hullIndexs=hull.vertices.tolist()
    result=points[hullIndexs]
    return result.tolist()