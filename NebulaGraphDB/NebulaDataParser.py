#!/usr/bin/python
import sys
from pyArango.connection import *
from pyArango.graph import *
from pyArango.collection import *
import pyArango.validation as VAL
from pyArango.theExceptions import ValidationError
import types
import json
#from google.protobuf.json_format import MessageToJson


class Slices(object):
    class nebulaMovie(Collection):
        _fields = {
            "movie_id": Field(),
            "type": Field(),
            "name": Field(),
            "url": Field()
        }

    class nebulaSegment(Collection):
        _fields = {
            "movie_id": Field(),
            "type": Field(),
            "startTimeOffset": Field(),
            "endTimeOffset": Field()
        }

    class objectAnnotation(Collection):
        _fields = {
            "movie_id": Field(),
            "type": Field(),
            "description": Field(),
            "entityId": Field(),
            "startTimeOffset": Field(),
            "endTimeOffset": Field()
        }

    class nebulaFrame(Collection):
        _fields = {
            "movie_id": Field(),
            "type": Field(),
            "timeOffset": Field()
        }

    class objectToFrameEdge(Edges):
        _fields = {
            "movie_id": Field(),
            "bottom": Field(),
            "top": Field(),
            "right": Field(),
            "left": Field(),
            "type": Field()
        }

    class frameToSegmentEdge(Edges):
        _fields = {
            "movie_id": Field(),
            "type": Field()
        }

    class segmentToMovieEdge(Edges):
        _fields = {
            "movie_id": Field(),
            "type": Field(),
            "seq": Field()
        }

    class NebulaObjectToFrameGraph(Graph):
        _edgeDefinitions = (EdgeDefinition('objectToFrameEdge',
                                           fromCollections=["nebulaFrame"],
                                           toCollections=["objectAnnotation"]),
                            EdgeDefinition('frameToSegmentEdge',
                                            fromCollections=["nebulaSegment"],
                                            toCollections=["nebulaFrame"]),
                            EdgeDefinition('segmentToMovieEdge',
                                            fromCollections=["nebulaMovie"],
                                            toCollections=["nebulaSegment"])
                            ,)
        _orphanedCollections = []

    def __init__(self):
        self.conn = Connection(arangoURL='http://127.0.0.1:8529', username="nebula", password="nebula")
        #self.conn = Connection(arangoURL='http://18.219.43.150:8529', username="nebula", password="nebula")
        self.db = self.conn["NebulaDB"]
    # if self.db.hasGraph('Slice'):
    #    raise Exception("Video Slice")
    #     self.db.dropAllCollections()
    #     self.nebulaobject = self.db.createCollection("objectAnnotation")
    #     self.objectToFrame = self.db.createCollection("objectToFrameEdge")
    #     self.frameToSegment = self.db.createCollection("frameToSegmentEdge")
    #     self.nebulaFrame = self.db.createCollection("nebulaFrame")
    #     self.nebulaMovie = self.db.createCollection("nebulaMovie")
    #     self.nebulaSegment = self.db.createCollection("nebulaSegment")
    #     self.segmentToMovie =  self.db.createCollection("segmentToMovieEdge")
    #
        nebulaGraph = self.db.createGraph('NebulaObjectToFrameGraph', createCollections = False)
        with open("vtag.json", "r") as myfile:
            all_data = json.loads(myfile.read())

            for movies in all_data['annotationResults']:
                movie = self.nebulaMovie.createDocument()
                movie["url"] = movies['inputUri']
                movie["type"] = "MOVIE"
                movie.save()
                count = 0
                for shotLabelAnnotations in movies['shotLabelAnnotations']:
                    for Segmnets in shotLabelAnnotations['segments']:
                        #print (Segmnets)
                        segmentQuery = {'startTimeOffset': float(Segmnets['segment']['startTimeOffset'][:-1]), 'endTimeOffset': float(Segmnets['segment']['endTimeOffset'][:-1])}
                        segmentNode = self.nebulaSegment.fetchFirstExample(segmentQuery)
                        if not segmentNode:
                            segmentNode = self.nebulaSegment.createDocument()
                            segmentNode["type"] = "SEGMENT"
                            segmentNode['startTimeOffset'] = float(Segmnets['segment']['startTimeOffset'][:-1])
                            segmentNode['endTimeOffset'] = float(Segmnets['segment']['endTimeOffset'][:-1])
                            segmentNode.save()
                            print ("Create!!!")
                        else:
                            segmentNode = segmentNode[0]
                            print ("Exists!!!!")
                        print (segmentNode)
                        segmentToMovie = self.segmentToMovie.createEdge()
                        segmentToMovie["_to"] = segmentNode['_id']
                        segmentToMovie["_from"] = movie['_id']
                        segmentToMovie["seq"] = count + 1
                        segmentToMovie.save()

                for objAnnot in movies['objectAnnotations']:
                    objAnnotNode = self.nebulaobject.createDocument()
                    objAnnotNode["type"] = "OBJECT:" + objAnnot['entity']['description']
                    objAnnotNode["confidence"] = objAnnot['confidence']
                    objAnnotNode["description"] = objAnnot['entity']['description']
                    objAnnotNode["entityId"] = objAnnot['entity']['entityId']
                    objAnnotNode["startTimeOffset"] = objAnnot['segment']['startTimeOffset']
                    objAnnotNode["endTimeOffset"] = objAnnot['segment']['endTimeOffset']
                    objAnnotNode.save()
                    for frames in objAnnot['frames']:
                        existQuery = {'timeOffset': frames['timeOffset']}
                        frameNode = self.nebulaFrame.fetchFirstExample(existQuery)
                        if not frameNode:
                            frameNode = self.nebulaFrame.createDocument()
                            frameNode["type"] = "FRAME"
                            frameNode["timeOffset"] = frames['timeOffset']
                            frameNode.save()
                        else:
                            frameNode = frameNode[0]
                        objToFrame = self.objectToFrame.createEdge()
                        objToFrame["_from"] = objAnnotNode['_id']
                        objToFrame["_to"] = frameNode['_id']
                        objToFrame["bottom"] = frames['normalizedBoundingBox'].get('bottom', 0)
                        objToFrame["top"] = frames['normalizedBoundingBox'].get('top', 0)
                        objToFrame["right"] = frames['normalizedBoundingBox'].get('right', 0)
                        objToFrame["left"] = frames['normalizedBoundingBox'].get('left', 0)
                        #nebulaGraph.link('objectToFrameEdge',objAnnotNode, frameNode, {"type": "Object To Frame"})
                        objToFrame.save()
                        frameToSegment = self.frameToSegment.createEdge()
                        time = float(frameNode['timeOffset'][:-1])
                        print (frameNode,time)
                        segmentQuery = "FOR doc IN nebulaSegment FILTER @time >= doc.`startTimeOffset` and @time <= doc.`endTimeOffset` RETURN doc"
                        bindVars = {'time': time}
                        segmentNode = self.db.AQLQuery(segmentQuery, rawResults=False, batchSize=1, bindVars=bindVars)
                        print (segmentNode, time)
                        frameToSegment["_to"] = frameNode['_id']
                        frameToSegment["_from"] = segmentNode[0]['_id']
                        print (frameToSegment)
                        print (movie)
                        frameToSegment.save()


Slices()