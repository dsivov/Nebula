import cv2
from math import *
from arango import ArangoClient

class Annotator():
#type: all, by_type, by_actors
#type = person, car...
#actors = ["Actors/id", "Actors/id"]
    def __init__(self, video_file,type, filter):
        print(video_file)
        video_annotate(video_file,type,filter)

def video_annotate(video_file, type, filter):
    cap = cv2.VideoCapture(video_file)
    mp4fourcc = cv2.VideoWriter_fourcc(*'MP4V')

    fps = int(cap.get(cv2.CAP_PROP_FPS))

    W, H = (int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)), int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)))
    f_begin, f_end = 0, int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    print('the video ' + video_file + ' has ' + str(f_end) + ' frames')
    print('Frames Per Second (fps): ' + str(fps))
    print('total amount of frames: ' + str(int(cap.get(cv2.CAP_PROP_FRAME_COUNT))))
    print('frame size: ' + str(W) + 'x' + str(H))
    i0 = range(f_begin, f_end, fps)
    i1 = range(f_begin + fps, f_end + fps, fps)

    frame_list = []
    writer = cv2.VideoWriter("out.mp4", mp4fourcc, fps, (W, H))
    positions = get_annotation_from_db(fps, int(f_end),type, filter)

    for i in range(f_begin, f_end):
        ret, frame = cap.read()
        for pos in positions:
            if pos['pos'][i] != 0.0:
                x = (pos['pos'][i].get('left'))
                w = (pos['pos'][i].get('top'))
                y = (pos['pos'][i].get('bottom'))
                h = (pos['pos'][i].get('right'))
                if x is None or w is None or y is None or h is None:
                    x, w, y, h = (0,0,0,0)
                Xc, Yc = center_geolocation(((x, w),(h, y)))
                print(x, w, y, h)
                print(Xc,Yc)
                actor = pos['Description']
                cv2.rectangle(frame, (int(x*W), int(w*H)), (int(h*W), int(y*H)), (255, 0, 0),2)
                cv2.putText(frame, actor, (int(x*W), int(w*H)), cv2.FONT_HERSHEY_COMPLEX, 1, (255, 255, 0))
                cv2.circle(frame,(int(Yc*W),int(Xc*H)),3, (255, 0, 0),2)
        frame_list.append(frame)
    for f in frame_list:
        writer.write(f)
    writer.release()
    cap.release()

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

def get_annotation_from_db(fps, frames_number, type, filter):
    client = ArangoClient(hosts='http://localhost:8529')
    db = client.db('NebulaDB', username='nebula', password='nebula')
    shots_coll = db.collection('Shots')
    shots = shots_coll.all(limit=1)
    actors_coll=db.collection('Actors')
    ret = []
    actors_count = 0
    actors_query = 'FOR doc IN Actors FILTER doc.`_id` IN @requested RETURN doc'
    actors_bind = {'requested': filter}

    if type == "all":
        actors = actors_coll.all(limit=90)
    elif type == "by_type":
        actors = actors_coll.find(filters={'description': filter})
    elif type == "by_actors":
        actors = db.aql.execute(actors_query, bind_vars=actors_bind)
    for shot in shots:
        for actor in actors:
            print(actor)
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
                frames[int(curr_frame)] = position['pos']['normalizedBoundingBox']
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
            ret.append({'Description': actor['description'], 'Actor': actor['_id'], 'pos': frames})
    return (ret)


def main():
   #Annotator("17.mp4","by_actors", ["Actors/2365153","Actors/2365284"])
   Annotator("17.mp4", "by_type", "car")

if __name__ == '__main__':
    main()