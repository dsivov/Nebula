#!/usr/bin/python
import sys
from pyArango.connection import *
from pyArango.graph import *
from pyArango.collection import *
import pyArango.validation as VAL
from pyArango.theExceptions import ValidationError
import types


class Slices(object):

    class NebulaObject(Collection):
        _fields = {
            "name": Field(),
            "Label": Field(),
            "type": Field(),
            "frameid": Field(),
            "sliceif": Field(),
            "positionX": Field(),
            "positionY": Field(),
            "features": Field(),
            "path": Field()
        }

    class NebulaGraph(Edges):
        _fields = {
            "weight": Field(),
            "frameid": Field(),
            "sliceif": Field(),
            "depth": Field()
        }


    class NebulaGraph(Graph):
        _edgeDefinitions = (EdgeDefinition('NebulaGraph',
                                           fromCollections=["NebulaObject"],
                                           toCollections=["NebulaObject"]),)
        _orphanedCollections = []


    def __init__(self):
        self.conn = Connection(arangoURL='http://18.219.43.150:8529', username="nebula", password="nebula")

        self.db = self.conn["NebulaDB"]
        #if self.db.hasGraph('Slice'):
        #    raise Exception("Video Slice")

        self.db.dropAllCollections()


        self.nebulaobject = self.db.createCollection("NebulaObject")
        self.relation = self.db.createCollection("NebulaGraph")
        ng = self.db.createGraph("NebulaGraph")
        ng.createVertex('NebulaObject', {"type": 'Slice',"name": 'Mv1', "sliceid": 1, "path": "/storage/mv1.mp4"}).save
        ng.createVertex('NebulaObject', {"type": 'Frame',"name": 'Fr1', "frameid": 1, "sliceid": 1}).save
        ng.createVertex('NebulaObject', {"type": 'Frame',"name": 'Fr2', "frameid": 2, "sliceid": 1}).save
        ng.createVertex('NebulaObject', {"type": 'Object',"name": 'Object1', "Label": 'Person',"frameid":1, "positionX": 10, "positionY": 40,"features": "[1,5,6,7,23,62]" }).save
        ng.createVertex('NebulaObject', {"type": 'Object',"name": 'Object2', "Label": 'Car' ,"frameid":1, "positionX": 20, "positionY": 40, "features": "[1,5,16,7,48,62]"}).save
        ng.createVertex('NebulaObject', {"type": 'Object',"name": 'Object3', "Label": 'Traffic Light', "frameid":1,"positionX": 20, "positionY": 40 ,"features": "[1,15,16,7,23,62]"}).save
        ng.createVertex('NebulaObject', {"type": 'Object',"name": 'Object4', "Label": 'Person', "frameid":1,"positionX": 30, "positionY": 40, "features": "[1,5,6,7,23,62]"}).save
        ng.createVertex('NebulaObject', {"type": 'Object',"name": 'Object5', "Label": 'Person', "frameid":2,"positionX": 10, "positionY": 40, "features": "[1,5,6,7,23,62]"}).save
        ng.createVertex('NebulaObject', {"type": 'Object',"name": 'Object6', "Label": 'Car', "frameid":2, "positionX": 30, "positionY": 40, "features": "[1,5,6,7,23,62]"}).save
        ng.createVertex('NebulaObject', {"type": 'Object',"name": 'Object7', "Label": 'Traffic Light', "frameid":2,"positionX": 20, "positionY": 40, "features": "[1,5,6,7,23,62]"}).save
        slices = {'type': "Slice"}
        sliceq = self.nebulaobject.fetchByExample(slices, batchSize=20, count=True)

        for s in sliceq:
            frames = {'sliceid': s['sliceid'], 'type': "Frame"}
            query2 = self.nebulaobject.fetchByExample(frames, batchSize=20, count=True)
            print(s['name'])
            for e1 in query2:
                objects = {'frameid': e1['frameid'], 'type': "Object"}
                query1 = self.nebulaobject.fetchByExample(objects, batchSize=20, count=True)
                print(e1['name'], e1['frameid'])
                for e2 in query1:
                    print(s['name'], e1['name'], e2['name'])
                    ng.link('NebulaGraph', s, e1,
                           {"frameid": e1['frameid'], "weight": 1, "depth": 1})
                    ng.link('NebulaGraph', e1, e2,
                           {"frameid": e2['frameid'], "weight": 1, "depth": 1})


            #example2 = {'frameid': 1}
            #query2 = self.nebulaobject.fetchByExample(example2, batchSize=20, count=True)


            #    if (e1['features'] != e2['features']):


               # if (e1['features'] != e2['features']):
                    #print(e1['name'], e2['name'])
                    #g.link('NebulaGraph', e1, e2, {"frameid": e2['frameid'], "weight": (e2['positionX'] - e1['positionX']), "depth": 1})

        #g.link('relation', a, b, {"type": 'married', "_key": 'aliceAndBob'})
        #g.link('relation', a, c, {"type": 'friend', "_key": 'aliceAndCharly'})
        #g.link('relation', c, d, {"type": 'married', "_key": 'charlyAndDiana'})
        #g.link('relation', b, d, {"type": 'friend', "_key": 'bobAndDiana'})


Slices()