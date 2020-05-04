from pyspark import SparkContext
from pyspark.sql import SQLContext
import time
from graphframes import *
import os
import sys

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell'
start = time.time()
sc = SparkContext('local[*]', "lpaCommunity")
sc.setLogLevel("ERROR")
spark = SQLContext(sc)

input_path = "C:\\Users\\sanka\\Desktop\\Study\\Sem3\\DM\\Assignment\\Assignment4\\power_input.txt"
data = sc.textFile(input_path)
#output = sys.argv[2]

# reading file and generating the vertices and edges list

def vertFunction(pair):
    vertex = set()
    temp = pair.split(' ')
    vertex.add((temp[0], 'v'))
    vertex.add((temp[1], 'v'))
    return vertex

def edgeFunction(value):
    edge = []
    temp1 = value.split(' ')
    edge.append((temp1[0], temp1[1]))
    edge.append((temp1[1], temp1[0]))
    return edge

vertices = data.map(lambda x:vertFunction(x)).flatMap(lambda x:x).collect()
verticesSet = list(set(vertices))
edges = data.map(lambda x:edgeFunction(x)).flatMap(lambda x:x).collect()

graphVertices = spark.createDataFrame(verticesSet, ["id","name"])
graphEdges = spark.createDataFrame(edges, ["src", "dst"])

# create GraphFrame
graph = GraphFrame(graphVertices, graphEdges)

# run LPA with 5 iterations
communities = graph.labelPropagation(maxIter=5)

# group based on the label and form node tuples
nodeList = communities.rdd.map(lambda x: (x[2], x[0])).groupByKey().map(lambda x: x[1]).collect()

# iterating over the values grouped by the label
communityList=[]
for i in nodeList:
    j= sorted(i)
    communityList.append((j,len(j)))

# sort the tuples based on the tuple size and then lexicographically
communityList.sort(key=lambda x: x[0])
communityList.sort(key=lambda x: x[1])

# writing output to the file
f = open('output.txt', 'w')
for i in communityList:
    s= str(i[0]).replace("[","").replace("]","")
    f.write(str(s))
    f.write("\n")
f.close()

# printing time taken
print("Duration", time.time()-start)
