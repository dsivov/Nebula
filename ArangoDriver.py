#!/usr/bin/python
import sys
from pyArango.connection import *
from pyArango.graph import *
from pyArango.collection import *
import pyArango.validation as VAL
from pyArango.theExceptions import ValidationError
import types


class Slice(object):
    class Frame(Collection):
        _fields = {
            "name": Field()
        }

    class NebulaObject(Collection):
        _fields = {
            "name": Field(),
            "Label": Field(),
            "type": Field(),
            "frameid": Field(),
            "position": Field(),
            "features": Field()
        }

    class NebulaGraph(Edges):
        _fields = {
            "weight": Field(),
            "frameid": Field(),
            "depth": Field()
        }

    class MovieGraph(Graph):
        _edgeDefinitions = (EdgeDefinition('NebulaGraph',
                                           fromCollections=["NebulaObject"],
                                           toCollections=["NebulaObject"]),)
        _orphanedCollections = []

    def __init__(self):
        self.conn = Connection(username="Nebula", password="nebula")

        self.db = self.conn["Nebula"]
        if self.db.hasGraph('Slice'):
            raise Exception("Video Slice")

        self.db.dropAllCollections()
        self.female = self.db.createCollection("Frame")
        self.male = self.db.createCollection("NebulaObject")
        self.relation = self.db.createCollection("NebulaGraph")

        g = self.db.createGraph("MovieGraph")

        a = g.createVertex('NebulaObject', {"name": 'Object1', "Label": 'Person',"frameid":1, "type": 'Street', "position": '10x30', "features": "[1,5,6,7,23,62]" });
        b = g.createVertex('NebulaObject', {"name": 'Object2', "Label": 'Car' ,"frameid":1, "type": 'Street', "position": '10x30', "features": "[1,5,16,7,48,62]"});
        c = g.createVertex('NebulaObject', {"name": 'Object3', "Label": 'Traffic Light', "frameid":1,"type": 'Street', "position": '10x30', "features": "[1,15,16,7,23,62]"});
        d = g.createVertex('NebulaObject', {"name": 'Object4', "Label": 'Person', "frameid":1,"type": 'Street', "position": '10x30', "features": "[1,5,6,7,23,62]"});
        a1 = g.createVertex('NebulaObject', {"name": 'Object5', "Label": 'Person', "frameid":2,"type": 'Street', "position": '10x30', "features": "[1,5,6,7,23,62]"});
        b1 = g.createVertex('NebulaObject', {"name": 'Object6', "Label": 'Car', "frameid":2, "type": 'Street', "position": '10x30', "features": "[1,5,6,7,23,62]"});
        c1 = g.createVertex('NebulaObject', {"name": 'Object7', "Label": 'Traffic Light', "frameid":2,"type": 'Street', "position": '10x30', "features": "[1,5,6,7,23,62]"});
        a.save()
        b.save()
        c.save()
        d.save()

        a1.save()
        b1.save()
        c1.save()

        #g.link('relation', a, b, {"type": 'married', "_key": 'aliceAndBob'})
        #g.link('relation', a, c, {"type": 'friend', "_key": 'aliceAndCharly'})
        #g.link('relation', c, d, {"type": 'married', "_key": 'charlyAndDiana'})
        #g.link('relation', b, d, {"type": 'friend', "_key": 'bobAndDiana'})


Slice()