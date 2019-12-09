from arango import ArangoClient
from scipy import stats
from math import *

class CalculateKL():
    print()
    def __init__(self, video_file, type, filter):
        print(video_file)
        frames_number = 145
        fps = 29
        get_data(frames_number, fps, type, filter)

def get_data(frames_number, fps, type, filter):

    client = ArangoClient(hosts='http://localhost:8529')
    db = client.db('NebulaDB', username='nebula', password='nebula')
    shots_coll = db.collection('Shots')
    shots = shots_coll.all(limit=1)
    actors_coll=db.collection('Actors')
    vector1 = []
    vector2 = []
    actors_count = 0
    actors_query = 'FOR doc IN Actors FILTER doc.`_id` IN @requested RETURN doc'
    actors_bind = {'requested': filter}
    actors = ""
    if type == "all":
        actors = actors_coll.all(limit=90)
    elif type == "by_type":
        actors = actors_coll.find(filters={'description': filter})
    elif type == "by_actors":
        actors = db.aql.execute(actors_query, bind_vars=actors_bind)
    for shot in shots:
        for actor in actors:
            positions = db.aql.execute(
                 'FOR v  IN OUTBOUND K_SHORTEST_PATHS @from TO @to GRAPH @graph '
                 'SORT v.edges[0].order RETURN {pos: v.edges[0]}',
                 bind_vars={'from': actor, 'to': shot['_id'], 'graph': "NebulaGraphKL"}
             )
            frames = [0.0]* (frames_number)
            max_pos = 0
            actors_count = actors_count + 1
            for position in positions:
                offset = float(position['pos']['timeOffset'][:-1])
                curr_frame = offset * fps
                i = 'normalizedBoundingBox'
                x = (position['pos'][i].get('left'))
                w = (position['pos'][i].get('top'))
                y = (position['pos'][i].get('bottom'))
                h = (position['pos'][i].get('right'))
                if x is None:
                    x = 0.0
                if w is None:
                    w = 0.0
                if y is None:
                    y = 0.0
                if h is None:
                    h = 0.0
                center = {0.0,0.0}
                center = center_geolocation(((x, w), (h, y)))
                frames[int(curr_frame)] = {center}
                max_pos = int(curr_frame)
            count = 0
            temp = 0.0
            for frame in frames:
                if (frame == 0.0):
                    frames[count] = temp
                else:
                    temp = frames[count]
                count = count +1
                if (count == max_pos):
                    break
            vector1.append({'Description': actor['description'], 'Actor': actor['_id'], 'pos': frames})
            vector2.append({'Description': actor['description'], 'Actor': actor['_id'], 'pos': frames})
    for vec1 in vector1:
        print(vec1)
        for vec2 in vector2:
            if vec1['Actor'] != vec2['Actor']:
                #print("\"",vec1['Actor'],"\",\"" ,vec2['Actor'], "\"")
                print("===============",vec2)
                #entropy = stats.entropy(pk=vec1['pos'], qk=vec2['pos'])
                #print(entropy)

    return (vector1)

def center_geolocation(locations):
    """
    Provide a relatively accurate center lat, lon returned as a list pair, given
    a list of list pairs.
    ex: in: geolocations = ((lat1,lon1), (lat2,lon2),)
        out: (center_lat, center_lon)
"""
    x = 0
    y = 0
    z = 0

    for lat, lon in locations:
        lat = float(lat)
        lon = float(lon)
        x += cos(lat) * cos(lon)
        y += cos(lat) * sin(lon)
        z += sin(lat)

    x = float(x / len(locations))
    y = float(y / len(locations))
    z = float(z / len(locations))

    return (atan2(y, x), atan2(z, sqrt(x * x + y * y)))


def calculateDistance(x1, y1, x2, y2):
    dist = sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)
    return dist


CalculateKL("test","all", "all")
