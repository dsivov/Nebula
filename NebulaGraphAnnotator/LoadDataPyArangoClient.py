from arango import ArangoClient
from pandas.io.json import json_normalize
import pandas as pd
import json
import uuid

def get_data():
    with open("vtag3.json", "r") as myfile:
        return(json.loads(myfile.read()))

def get_movies(all_data):
    movies = json_normalize(data=all_data['annotationResults'],errors = 'ignore')
    return(movies)

def get_actors_and_frames(all_data):
    actors=json_normalize(data=all_data['annotationResults'],record_path=['objectAnnotations'],errors = 'ignore')
    return(actors)

def get_shots(all_data):
    shots=json_normalize(data=all_data['annotationResults'],record_path=['shotAnnotations'],errors = 'ignore')
    return(shots)

def init_nebula_db():
    # Initialize the ArangoDB client.
    client = ArangoClient(hosts='http://localhost:8529')

    # Connect to "_system" database as root user.
    # This returns an API wrapper for "_system" database.
    sys_db = client.db('_system', username='root', password='root')

    # Create a new database named "test" if it does not exist.
    #sys_db.delete_database('NebulaDB')
    if not sys_db.has_database('NebulaDB'):
        sys_db.create_database('NebulaDB',users=[{'username': 'nebula', 'password': 'nebula', 'active': True}])
    # Connect to "test" database as root user.
    # This returns an API wrapper for "test" database.
    db = client.db('NebulaDB', username='nebula', password='nebula')
    #Temp

    # Create a new collection named "students" if it does not exist.
    # This returns an API wrapper for "students" collection.
    if db.has_graph('NebulaGraphKL'):
        nebula_graph_kl = db.graph('NebulaGraphKL')
    else:
        nebula_graph_kl = db.create_graph('NebulaGraphKL')
    if db.has_collection('Actors'):
        actors = db.collection('Actors')
    else:
        actors = db.create_collection('Actors')
    if db.has_collection('Shots'):
        shots = db.collection('Shots')
    else:
        shots = db.create_collection('Shots')
    if db.has_collection('Positions'):
        positions = db.collection('Positions')
    else:
        positions = db.create_collection('Positions')
    if db.has_collection('Movies'):
        movies = db.collection('Movies')
    else:
        movies = db.create_collection('Movies')
    if db.has_collection('TimeFrames'):
        time_frames = db.collection('TimeFrames')
    else:
        time_frames = db.create_collection('TimeFrames')

    if db.has_graph('Nebula'):
        nebula = db.graph('Nebula')
    else:
        nebula = db.create_graph('Nebula')

    if not nebula_graph_kl.has_edge_definition('ActorsToPositions'):
        actors2positions = nebula_graph_kl.create_edge_definition(
            edge_collection='ActorsToPositions',
            from_vertex_collections=['Actors'],
            to_vertex_collections=['Positions']
        )

    if not nebula_graph_kl.has_edge_definition('FramesToShots'):
        frames2shots = nebula_graph_kl.create_edge_definition(
            edge_collection='FramesToShots',
            from_vertex_collections=['TimeFrames'],
            to_vertex_collections=['Shots']
    )

    if not nebula_graph_kl.has_edge_definition('ShotsToFrames'):
        frames2shots = nebula_graph_kl.create_edge_definition(
            edge_collection='ShotsToFrames',
            from_vertex_collections=['Shots'],
            to_vertex_collections=['TimeFrames']
    )

    if not nebula_graph_kl.has_edge_definition('MovieToActors'):
        movie2actors = nebula_graph_kl.create_edge_definition(
            edge_collection='MovieToActors',
            from_vertex_collections=['Movies'],
            to_vertex_collections=['Actors']
        )

    if not nebula_graph_kl.has_edge_definition('PositionToFrames'):
        positions2frames = nebula_graph_kl.create_edge_definition(
            edge_collection='PositionToFrames',
            from_vertex_collections=['Positions'],
            to_vertex_collections=['TimeFrames']
    )

    if not nebula_graph_kl.has_edge_definition('ActorsToFrames'):
        actors2frames = nebula_graph_kl.create_edge_definition(
            edge_collection='ActorsToFrames',
            from_vertex_collections=['Actors'],
            to_vertex_collections=['TimeFrames']
        )

    if not nebula_graph_kl.has_edge_definition('FramesToActors'):
        frames2actors = nebula_graph_kl.create_edge_definition(
            edge_collection='FramesToActors',
            from_vertex_collections=['TimeFrames'],
            to_vertex_collections=['Actors']
        )
    return(db)

def delete_db():
    client = ArangoClient(hosts='http://localhost:8529')
    # Connect to "_system" database as root user.
    # This returns an API wrapper for "_system" database.
    sys_db = client.db('_system', username='root', password='root')
    if not sys_db.has_database('NebulaDB'):
        print("NEBULADB not exist")
    else:
        sys_db.delete_database('NebulaDB')

def load_data(db):
    all_data=get_data()
    actors = get_actors_and_frames(all_data)
    shots=get_shots(all_data)
    movies = get_movies(all_data)
    movie_id = uuid.uuid4().hex
    for movie in movies.iterrows():
        movie_coll = db.collection('Movies')
        movie_coll.insert({'url_path': movie[1]['inputUri'],'movie_id': movie_id})

    counter=0
    for shot in shots.iterrows():
        shots_coll=db.collection('Shots')
        shots_coll.insert({'startTimeOffset': shot[1]['startTimeOffset'], 'endTimeOffset': shot[1]['endTimeOffset'], 'sequence': counter, 'movie_id': movie_id})
        counter += 1
    for actor in actors.iterrows():
        actor_id = uuid.uuid4()
        actors_coll=db.collection('Actors')
        positions_coll=db.collection('Positions')
        timeframes_coll=db.collection('TimeFrames')
        actors_coll.insert({'entityid': actor[1]['entity.entityId'], 'description': actor[1]['entity.description'], 'confidence': actor[1]['confidence']
                               , 'startTimeOffset': actor[1]['segment.startTimeOffset'] ,
                            'endTimeOffset':actor[1]['segment.endTimeOffset'], 'id': str(actor_id.hex) + "_" + actor[1]['entity.entityId'], 'movie_id': movie_id})
        order = 0
        for frame in actor[1]['frames']:
            frame['actor_id'] = str(actor_id.hex) + "_" + actor[1]['entity.entityId']
            frame['movie_id'] = movie_id
            frame['order'] = order
            positions_coll.insert(frame)
            timeframes_coll.insert({'_id': "TimeFrames/" + frame['timeOffset'],
                                    'movie_id': movie_id}, overwrite=True)
            order += 1

def create_graph_KL(db):
    positions_coll = db.collection('Positions')
    timeframes_coll = db.collection('TimeFrames')
    actors_coll = db.collection('Actors')
    movies_coll = db.collection('Movies')
    shots_coll = db.collection('Shots')
    movie2actors = db.graph('NebulaGraphKL').edge_collection('MovieToActors')
    frames2shots = db.graph('NebulaGraphKL').edge_collection('FramesToShots')
    shots2frames = db.graph('NebulaGraphKL').edge_collection('ShotsToFrames')
    actors2frames = db.graph('NebulaGraphKL').edge_collection('ActorsToFrames')
    frames2actors = db.graph('NebulaGraphKL').edge_collection('FramesToActors')
    for movie in movies_coll.all():
        for timeframe in timeframes_coll.find(filters={'movie_id': movie['movie_id']}):
            for shot in shots_coll.find(filters={'movie_id': movie['movie_id']}):
                #print("Timeframe: ",timeframe )
                if ((float(shot['startTimeOffset'][:-1]) <= float(timeframe['_key'][:-1]))
                        and (float(shot['endTimeOffset'][:-1]) >= float(timeframe['_key'][:-1]))):
                    frames2shots.link(timeframe, shot)
                    shots2frames.link(shot, timeframe)
                    print("Shot to Frame:", shot, timeframe)
        for actor in actors_coll.find(filters={'movie_id': movie['movie_id']}):
            movie2actors.link(movie, actor, data={'confidence': actor['confidence']})
            positions = positions_coll.find(filters={'actor_id': actor['id'], 'movie_id': movie['movie_id']})
            for timeframe in timeframes_coll.find(filters={'movie_id': movie['movie_id']}):
                for position in positions:
                    actors2frames.link(actor, timeframe, data=position)
                    frames2actors.link(timeframe, actor, data=position)
                    print("Actor to Frame:", actor, timeframe)

def calculate_kl(db):
    from scipy import stats
    if db.has_graph('EntropyKL'):
        db.delete_graph('EntropyKL')
        entropy_graph = db.create_graph('EntropyKL')
    else:
        entropy_graph = db.create_graph('EntropyKL')

    if not entropy_graph.has_edge_definition('ActorToActor'):
        actors2actors = entropy_graph.create_edge_definition(
            edge_collection='ActorToActor',
            from_vertex_collections=['Actors'],
            to_vertex_collections=['Actors']
    )

    actors_coll = db.collection('Actors')
    #actors = actors_coll.all()
    for actor_from in actors_coll.all():
        for actor_to in actors_coll.all():
            if (actor_from['_id'] != actor_to['_id'] and actor_from['confidence'] > 0.9 and actor_to['confidence'] > 0.9):
                positions = db.aql.execute(
                    'FOR v  IN OUTBOUND K_SHORTEST_PATHS @from TO @to GRAPH @graph RETURN {from: v.edges[0], to: v.edges[1]}',
                    bind_vars={'from': actor_from, 'to': actor_to, 'graph': "NebulaGraphKL"}
                )
                left_vector1 = pd.Series()
                left_vector2 = pd.Series()
                right_vector1 = pd.Series()
                right_vector2 = pd.Series()
                top_vector1 = pd.Series()
                top_vector2 = pd.Series()
                bottom_vector1 = pd.Series()
                bottom_vector2 = pd.Series()
                vector1 = pd.Series()
                vector2 = pd.Series()
                for pos in positions:
                    left_vector1 = left_vector1.append(pd.Series(pos['from']['normalizedBoundingBox'].get('left')))
                    left_vector2 = left_vector2.append(pd.Series(pos['to']['normalizedBoundingBox'].get('left')))
                    vector1 = vector1.append(left_vector1)
                    vector2 = vector2.append(left_vector2)
                    right_vector1 = right_vector1.append(pd.Series(pos['from']['normalizedBoundingBox'].get('right')))
                    right_vector2 = right_vector2.append(pd.Series(pos['to']['normalizedBoundingBox'].get('right')))
                    vector1 = vector1.append(right_vector1)
                    vector2 = vector2.append(right_vector2)
                    top_vector1 = top_vector1.append(pd.Series(pos['from']['normalizedBoundingBox'].get('top')))
                    top_vector2 = top_vector2.append(pd.Series(pos['to']['normalizedBoundingBox'].get('top')))
                    vector1 = vector1.append(top_vector1)
                    vector2 = vector2.append(top_vector2)
                    bottom_vector1 = bottom_vector1.append(pd.Series(pos['from']['normalizedBoundingBox'].get('bottom')))
                    bottom_vector2 = bottom_vector2.append(pd.Series(pos['to']['normalizedBoundingBox'].get('bottom')))
                    vector1 = vector1.append(bottom_vector1)
                    vector2 = vector2.append(bottom_vector2)
                if (vector1.size == vector2.size and vector2.size > 0 and vector1.size > 0 ):
                    entropy = stats.entropy(pk=vector1.values, qk=vector2.values)
                print("From Actor: " + actor_from['description'] + " To Actor: " + actor_to['description'],
                      actor_from['_id'], actor_to['_id'], entropy)
                if (entropy != float("inf")):
                    actors2actors.link(actor_from, actor_to, data={'entropy': entropy})



def main():
    print()
    #delete_db()
    db = init_nebula_db()
    #load_data(db)
    #create_graph_KL(db)
    calculate_kl(db)


if __name__ == '__main__':
    main()