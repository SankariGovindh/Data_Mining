import time
from pyspark import SparkContext
import copy
import sys

start = time.time()
sc = SparkContext('local[*]', 'girvanCommunity')
sc.setLogLevel("ERROR")
file_path = sys.argv[1]
data = sc.textFile(file_path)
comOutput = sys.argv[3]
betweenOp = sys.argv[2]

# global variables
visited = []
count = []
rootList = []
maxModularity = -1
communitySize = 1
maxBetween = []


def vertFunction(pair):
    vertex = []
    temp = pair.split(' ')
    vertex.append(temp[0])
    vertex.append(temp[1])
    return vertex


def edgeFunction(value):
    edge = []
    temp1 = value.split(' ')
    edge.append((temp1[0], temp1[1]))
    edge.append((temp1[1], temp1[0]))
    return edge


def betweennessCalc(root, adjList):
    # root is the actual vertex for which bfs to be done
    visited = []
    bfsQueue = []

    # arrays to calculate the shortest path to the vertex and the edge credit
    parent = {}
    label = {}
    creditDict = {}
    outputList = []
    level = {}

    for vertex in adjList.keys():
        level[vertex] = -1

    # perform bfs based on the adjacency list
    visited.append(root)
    bfsQueue.append(root)
    label[root] = 1
    level[root] = 0

    while bfsQueue:
        currentNode = bfsQueue.pop(0)
        visited.append(currentNode)

        for neighbour in adjList[currentNode]:

            if level[neighbour] == -1:
                bfsQueue.append(neighbour)
                parent[neighbour] = []
                level[neighbour] = level[currentNode] + 1
                label[neighbour] = 0

            if level[neighbour] == level[currentNode] + 1:
                parent[neighbour].append(currentNode)
                label[neighbour] += label[currentNode]

    # end of while

    # calculate the credit for every edge
    visitedCopy = visited.copy()
    visitedCopy.reverse()
    reverseTraversal = visitedCopy
    # to start from the leaf node and calculate the credit corresponding to the leaf nodes
    for node in reverseTraversal:

        if node != root:

            # compute the credit for the node
            nodeParent = parent[node]

            for p in nodeParent:
                if node not in creditDict:
                    creditDict[node] = 1
                    childCredit = creditDict[node]
                else:
                    childCredit = creditDict[node]
                credit = (label[p] / label[node]) * childCredit

                # store it as a list of tuples
                key = (node, p)
                edgeKey = tuple(sorted(key))
                outputList.append((edgeKey, credit))

                # updating the credit for the parent node in the credit dictionary
                if p not in creditDict:
                    creditDict[p] = 1 + credit
                else:
                    creditDict[p] += credit
            # end of credit calc
    # end of for reverseTraversal

    return outputList


# communityList - [[v1,v2,v3], [v4,v2,v3], etc...]
def modularityCalc(communityList, m):
    totalModularity = 0.0
    for community in communityList:
        comModularity = 0.0
        for i in community:
            ki = len(adjDict[i])
            for j in community:
                if j in adjDict[i]:
                    Aij = 1
                else:
                    Aij = 0
                kj = len(adjDict[j])
                comModularity += (Aij - ((ki * kj) / (2 * m)))
                # end of for
        totalModularity += comModularity
    # end of communities
    totalModularity = (totalModularity / (2 * m))
    return totalModularity


def bfs(vertex):
    connected = []
    queue = []
    connected.append(vertex)
    queue.append(vertex)

    while queue:
        node = queue.pop(0)
        for neighbour in updatedGraph[node]:
            if neighbour not in connected:
                connected.append(neighbour)
                queue.append(neighbour)

    return connected


# once edges are removed the graph gets divided store the list of communities
def communityFinder(inGraph):
    newGraph = inGraph.copy()
    communities = []
    graphLen = len(newGraph)

    while (graphLen != 0):

        vertex = list(newGraph.keys())[0]
        connectedComp = bfs(vertex)
        for node in connectedComp:
            del newGraph[node]
        communities.append(connectedComp)
        graphLen = len(newGraph)

    return communities


########################################################################################################################
# contains the pairs of edges - (a,b)
edges = data.map(lambda x: edgeFunction(x)).flatMap(lambda x: x).collect()
rddList = sc.parallelize(edges)

m = len(edges) / 2

# group all the adjacent nodes based on the first vertex - (v, [list of adjacent])
adjacencyRDD = rddList.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y)
adjDict = adjacencyRDD.collectAsMap()
updatedGraph = copy.deepcopy(adjDict)

vertices = data.map(lambda x: vertFunction(x)).flatMap(lambda x: x).collect()
verticesSet = list(set(vertices))
verticesRDD = sc.parallelize(verticesSet)

# calling the betweennessCalc function for this RDD - getting called correct - when updating the betweenness value - mistake
betweenRDD = verticesRDD.flatMap(lambda x: betweennessCalc(x, adjDict)).reduceByKey(lambda a, b: a + b) \
    .map(lambda x: (x[0], (x[1] / 2)))

outputBetweenList = betweenRDD.sortByKey().map(lambda x: (x[1], x[0])).sortByKey(ascending=False).collect()

# calculate modularity, remove edges and calculate betweenness - repeat in a loop
betweenDict = betweenRDD.sortBy(lambda x: -x[1]).collectAsMap()
edgeToRemove = list(betweenDict.keys())[0]

# condition to check if the community length is != total number of vertices to get individual component
communityComp = []
iterCount = verticesRDD.count()

while 1:

    edge = edgeToRemove
    v1 = edge[0]
    v2 = edge[1]
    updatedGraph[v1].remove(v2)
    updatedGraph[v2].remove(v1)

    communityComp = communityFinder(updatedGraph)
    modularity = modularityCalc(communityComp, m)

    if (len(communityComp) == iterCount):
        break

    if modularity > maxModularity:
        maxModularity = modularity
        maxCommunity = communityComp

    newBetweenDict = verticesRDD.flatMap(lambda x: betweennessCalc(x, updatedGraph)).reduceByKey(lambda a, b: a + b) \
        .map(lambda x: (x[0], (x[1] / 2))).sortBy(lambda x: -x[1]).collectAsMap()
    edgeToRemove = list(newBetweenDict.keys())[0]

print("maxMod", maxModularity)
print("community", len(maxCommunity))

########################################################################################################################


# writing output of betweenness to file
f = open(betweenOp, 'w')
for value in outputBetweenList:
    f.write(str(value[1]) + ", " + str(value[0]))
    f.write('\n')
f.close()

finalAns = []
for i in maxCommunity:
    j = sorted(i)
    finalAns.append((j, len(j)))

# sort the tuples based on the tuple size and then lexicographically
finalAns.sort(key=lambda x: x[0])
finalAns.sort(key=lambda x: x[1])

# writing communities to file
f = open(comOutput, 'w')
for i in finalAns:
    s = str(i[0]).replace("[", "").replace("]", "")
    f.write(str(s))
    f.write("\n")
f.close()

print("Duration:", time.time() - start)
