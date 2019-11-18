#!/usr/bin/python
import sys
from pyArango.connection import *
from pyArango.graph import *
from pyArango.collection import *
import pyArango.validation as VAL
from pyArango.theExceptions import ValidationError
import types


class Slices(object):

    class Slice(Collection):
        _fields = {
            "name": Field(),
            "sliceid": Field(),
            "path": Field()
        }

    class Frame(Collection):
        _fields = {
            "name": Field(),
            "sliceid": Field(),
            "frameid": Field()
        }

    class NebulaObject(Collection):
        _fields = {
            "name": Field(),
            "Label": Field(),
            "type": Field(),
            "frameid": Field(),
            "positionX": Field(),
            "positionY": Field(),
            "features": Field()
        }

    class ObjectGraph(Edges):
        _fields = {
            "weight": Field(),
            "frameid": Field(),
            "depth": Field()
        }

    class FrameGraph(Edges):
        _fields = {
            "weight": Field(),
            "frameid": Field(),
            "depth": Field()
        }

    class SliceGraph(Edges):
        _fields = {
            "weight": Field(),
            "frameid": Field(),
            "depth": Field()
        }

    class ObjectGraph(Graph):
        _edgeDefinitions = (EdgeDefinition('ObjectGraph',
                                           fromCollections=["NebulaObject"],
                                           toCollections=["NebulaObject"]),)
        _orphanedCollections = []

    class FrameGraph(Graph):
        _edgeDefinitions = (EdgeDefinition('FrameGraph',
                                           fromCollections=["Frame"],
                                           toCollections=["NebulaObject"]),)

    class SliceGraph(Graph):
        _edgeDefinitions = (EdgeDefinition('SliceGraph',
                                            fromCollections=["Slice"],
                                            toCollections=["Frame"]),)
        _orphanedCollections = []

    def __init__(self):
        self.conn = Connection(arangoURL='http://18.219.43.150:8529', username="nebula", password="nebula")

        self.db = self.conn["NebulaDB"]
        #if self.db.hasGraph('Slice'):
        #    raise Exception("Video Slice")

        self.db.dropAllCollections()

        self.slice = self.db.createCollection("Slice")
        self.frame = self.db.createCollection("Frame")
        self.nebulaobject = self.db.createCollection("NebulaObject")
        self.relation = self.db.createCollection("SliceGraph")
        self.relation = self.db.createCollection("FrameGraph")
        self.relation = self.db.createCollection("ObjectGraph")

        sg = self.db.createGraph("SliceGraph")
        fg = self.db.createGraph("FrameGraph")
        og = self.db.createGraph("ObjectGraph")
        sg.createVertex('Slice', {"name": 'Mv1', "sliceid": 1, "path": "/storage/mv1.mp4"}).save
        fg.createVertex('Frame', {"name": 'Fr1', "frameid": 1, "sliceid": 1}).save
        fg.createVertex('Frame', {"name": 'Fr2', "frameid": 2, "sliceid": 1}).save
        og.createVertex('NebulaObject', {"name": 'Object1', "Label": 'Person',"frameid":1, "type": 'Street', "positionX": 10, "positionY": 40,"features": "[1,5,6,7,23,62]" }).save
        og.createVertex('NebulaObject', {"name": 'Object2', "Label": 'Car' ,"frameid":1, "type": 'Street', "positionX": 20, "positionY": 40, "features": "[1,5,16,7,48,62]"}).save
        og.createVertex('NebulaObject', {"name": 'Object3', "Label": 'Traffic Light', "frameid":1,"type": 'Street', "positionX": 20, "positionY": 40 ,"features": "[1,15,16,7,23,62]"}).save
        og.createVertex('NebulaObject', {"name": 'Object4', "Label": 'Person', "frameid":1,"type": 'Street', "positionX": 30, "positionY": 40, "features": "[1,5,6,7,23,62]"}).save
        og.createVertex('NebulaObject', {"name": 'Object5', "Label": 'Person', "frameid":2,"type": 'Street', "positionX": 10, "positionY": 40, "features": "[1,5,6,7,23,62]"}).save
        og.createVertex('NebulaObject', {"name": 'Object6', "Label": 'Car', "frameid":2, "type": 'Street', "positionX": 30, "positionY": 40, "features": "[1,5,6,7,23,62]"}).save
        og.createVertex('NebulaObject', {"name": 'Object7', "Label": 'Traffic Light', "frameid":2,"type": 'Street', "positionX": 20, "positionY": 40, "features": "[1,5,6,7,23,62]"}).save

        query3 = self.slice.fetchAll()
        for s in query3:
            print(s)

        for s in query3:
            frames = {'sliceid': s['sliceid']}
            query2 = self.frame.fetchByExample(frames, batchSize=20, count=True)

            for e1 in query2:
                objects = {'frameid': e1['frameid']}
                query1 = self.nebulaobject.fetchByExample(objects, batchSize=20, count=True)
                #print(e1['name'])
                for e2 in query1:
                    print(s['name'], e1['name'], e2['name'])
                    og.link('ObjectGraph', s, e1,
                           {"frameid": e1['frameid'], "weight": 1, "depth": 1})
                    og.link('ObjectGraph', e1, e2,
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