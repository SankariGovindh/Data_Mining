import time
import numpy as np
from sklearn.cluster import KMeans
import sys
import math
from sklearn.metrics.cluster import normalized_mutual_info_score

np.seterr(divide='ignore', invalid='ignore')
startTime = time.time()
inputPath = "C:\\Users\\sanka\\Desktop\\Study\\Sem3\\DM\\Assignment\\Assignment6\\hw6_clustering.csv"
#noOfCluster = sys.argv[1]
#output = sys.argv[2]
noOfCluster = 10

#global variables
retainedSet = []
discardStats = {}
compressStats = {}
clusterLabel = {}
K = noOfCluster
chunkCount = 0
remainingData = []
interResult = {}
noOfDSPoints = 0
noOfCSPoints = 0
noOfCSCluster = 0
noOfRSPoints = 0
discardSet = []
csLabel = 0
rs = 0

############################################ initialisation of DS, CS and RS ###########################################
# random data load from the input file - 20%
data = np.loadtxt(inputPath, delimiter=",")
nSize, nCols= data.shape
originalIndex = data[:,1].astype(int).tolist()
totalIndex = [-1] * nSize
np.random.shuffle(data)
dataSize = len(data)
sampleSize = int(dataSize / 5)
chunkCount += 1
initData = data[:sampleSize]
prevEnd = sampleSize

# running k-means cluster with large k = 5*noOfCluster - euclidean distance for similarity measurement
# [0 1 2 3 4]
dataPoint = initData[:,0].tolist()
# [[-1 0 2] [3.4 5 6]]
dimensionData = initData[:,2:]
nRows, dimension = dimensionData.shape
clusterIndex = KMeans(n_clusters= 5 * K).fit_predict(dimensionData)

# map between the datapoints and the value in the file - may be removed later - {0:[-1.6, 2], 1:[3,-4.8]} - {pt:[list of dimensions]}
ptDict = {}
for pt in data:
    pt = pt.tolist()
    ptIndex = pt[0]
    if ptIndex not in ptDict:
        ptDict[ptIndex] = pt[2:]

# k-means cluster - generating RS
i = 0
for index in clusterIndex:
    if index in clusterLabel:
        clusterLabel[index].append(dataPoint[i])
    else:
        clusterLabel[index] = [dataPoint[i]]
    i += 1

for label,points in clusterLabel.items():
    if len(clusterLabel[label]) == 1:
        retainedSet.append(points[0])
    else:
        discardSet.append(points)

############################################## initialising DS #########################################################

flatDislist = []
for sublist in discardSet:
    for item in sublist:
        flatDislist.append(item)

discardLabel = {}
dsDimension = []
for point in flatDislist:
    dsDimension.append(ptDict[point])

# running k-means cluster with k = noOfCluster - generating DS and DSstats
discardClusterIndex = KMeans(n_clusters= K).fit_predict(dsDimension)

# iteration to collect the list of points that are present in a cluster
i = 0
for index in discardClusterIndex:
    if index in discardLabel:
        discardLabel[index].append(flatDislist[i])
        totalIndex[int(flatDislist[i])] = index
    else:
        discardLabel[index] = [flatDislist[i]]
        totalIndex[int(flatDislist[i])] = index
    i += 1

# for every datapoint, for every cluster in discardset compute N, SUM and SUMSQ
# [[0,1,2], [3,4], [5,6]]
for key,value in discardLabel.items():

    # value of the form [0,1,2] - for each point get the corresponding datapoints and that becomes set of datapoints
    pointList = value
    tempDimList = []
    for point in pointList:
        tempDimList.append(ptDict[point]) # [[-1,2,-4], [0.3,2,4]]

    X = np.array(tempDimList)
    N = len(tempDimList)
    SUM = np.sum(X, axis=0)
    SUMSQ = np.sum(np.square(X), axis=0)
    centroid = SUM / N
    variance = (SUMSQ / N) - (np.square(SUM / N))
    standardDev = np.sqrt(variance)

    # dictionary for each cluster index with the values N, SUM and SUMSQ {0: (N, SUM, SUMSQ), 1:(N, SUM, SUMSQ), etc...}
    discardStats[key] = (N, SUM, SUMSQ, centroid, variance, standardDev)

noOfDSPoints = 0
for key,value in discardStats.items():
    noOfDSPoints += value[0]

######################################### initialising CS from RS ######################################################

# running k-means with larger K to get CS
csCluster = {}
rsDimension = []
compLabel = {}
for point in retainedSet:
    rsDimension.append(ptDict[point])

minVal = min((5*K) , len(retainedSet))
csClusterIndex = KMeans(n_clusters=minVal).fit_predict(rsDimension)

# iteration to collect the list of points that are present in a cluster
i = 0
for index in csClusterIndex:
    if index in compLabel:
        compLabel[index].append(retainedSet[i])
    else:
        compLabel[index] = [retainedSet[i]]
    i += 1

# calculating N, SUM and SUMSQ for every dimension of the datapoint
# for every datapoint, for every value in discardset compute N, SUM and SUMSQ
# [[0,1,2], [3,4], [5,6]]
for key, value in compLabel.items():

    if len(compLabel[key]) > 1:

        # value of the form [0,1,2] - for each point get the corresponding datapoints and that becomes set of datapoints
        pointList = value
        tempDimList = []
        for point in pointList:
            tempDimList.append(ptDict[point])  # [[-1,2,-4], [0.3,2,4]]
            retainedSet.remove(point)

        for point in pointList:
            if csLabel in csCluster:
                csCluster[csLabel].append(int(point))
            else:
                csCluster[csLabel] = [int(point)]

        # in case if added to the CS set remove the points from retained set
        X = np.array(tempDimList)
        N = len(tempDimList)
        SUM = np.sum(X, axis=0)
        SUMSQ = np.sum(np.square(X), axis=0)
        centroid = SUM / N
        variance = (SUMSQ / N) - (np.square(SUM / N))
        standardDev = np.sqrt(variance)

        # dictionary for each cluster index with the values N, SUM and SUMSQ {0: (N, SUM, SUMSQ), 1:(N, SUM, SUMSQ), etc...}
        compressStats[csLabel] = (N, SUM, SUMSQ, centroid, variance, standardDev)
        csLabel += 1

noOfCSPoints = 0
for key,value in compressStats.items():
    noOfCSPoints += value[0]

noOfCSCluster = len(compressStats)
noOfRSPoints = len(retainedSet)

iteration = 0
interResult[iteration] = (noOfDSPoints, noOfCSCluster, noOfCSPoints, noOfRSPoints)
iteration += 1

############################################# updating DS, CS and RS ###################################################

distance = 2*(math.sqrt(dimension))

def updateDS(workingData):

    rest_data = []

    for line in workingData:

        dimList = line[2:]
        minMahDist = sys.maxsize
        clusterInfo = -1

        for cluster, value in discardStats.items():

            centroid = value[3]
            stdDev = value[5]
            sqVal = np.square((dimList-centroid)/stdDev)
            mahDist = np.sqrt(np.sum(sqVal))

            # store the closest cluster to this point among all the cluster points
            if mahDist < minMahDist:
                minMahDist = mahDist
                clusterInfo = cluster

        # update the DS statistics of the closest cluster with the new points
        if minMahDist < distance and clusterInfo > -1:
            dsToUpdate = discardStats[clusterInfo]
            newN = dsToUpdate[0] + 1
            newSum = dsToUpdate[1] + dimList
            newSumSq = dsToUpdate[2] + np.square(dimList)
            newCentroid = newSum / newN
            newVariance = (newSumSq / newN) - (np.square(newSum / newN))
            newStdDev = np.sqrt(newVariance)
            updatedVal = {clusterInfo: (newN, newSum, newSumSq, newCentroid, newVariance, newStdDev)}
            discardStats.update(updatedVal)
            tempPoint = int(line[0])
            totalIndex[tempPoint] = clusterInfo

        else:
            rest_data.append(line)

    return rest_data


def updateCS(remainingPoint):

    rest_data = []
    for line in remainingPoint:

        dimList = line[2:]
        minMahDist = sys.maxsize
        clusterInfo = -1

        for cluster, value in compressStats.items():

            centroid = value[3]
            stdDev = value[5]
            sqVal = np.square((dimList - centroid) / stdDev)
            mahDist = np.sqrt(np.sum(sqVal))

            # store the closest cluster to this point among all the cluster points
            if mahDist < minMahDist:
                minMahDist = mahDist
                clusterInfo = cluster

        # update the DS statistics of the closest cluster with the new points
        if minMahDist < distance and clusterInfo > -1:
            csToUpdate = compressStats[clusterInfo]
            newN = csToUpdate[0] + 1
            newSum = csToUpdate[1] + dimList
            newSumSq = csToUpdate[2] + np.square(dimList)
            newCentroid = newSum / newN
            newVariance = (newSumSq / newN) - (np.square(newSum / newN))
            newStdDev = np.sqrt(newVariance)
            updatedVal = {clusterInfo: (newN, newSum, newSumSq, newCentroid, newVariance, newStdDev)}
            compressStats.update(updatedVal)
            tempPoint = int(line[0])
            csCluster[clusterInfo].append(tempPoint)
        else:
            rest_data.append(line)

    return rest_data
############################################ merge of DS, CS and RS ####################################################
def mergeCsToCs(index1, index2):

    newCSData = compressStats[index2]
    csToUpdate = compressStats[index1]
    newN = csToUpdate[0] + newCSData[0]
    newSum = csToUpdate[1] + newCSData[1]
    newSumSq = csToUpdate[2] + newCSData[2]
    newCentroid = newSum / newN
    newVariance = (newSumSq / newN) - (np.square(newSum / newN))
    newStdDev = np.sqrt(newVariance)
    updatedVal = {index1: (newN, newSum, newSumSq, newCentroid, newVariance, newStdDev)}
    compressStats.update(updatedVal)

def mergeCsToDs(cs,ds):

    newCSData = compressStats[cs]
    dsToUpdate = discardStats[ds]
    newN = dsToUpdate[0] + newCSData[0]
    newSum = dsToUpdate[1] + newCSData[1]
    newSumSq = dsToUpdate[2] + newCSData[2]
    newCentroid = newSum / newN
    newVariance = (newSumSq / newN) - (np.square(newSum / newN))
    newStdDev = np.sqrt(newVariance)
    updatedVal = {ds: (newN, newSum, newSumSq, newCentroid, newVariance, newStdDev)}
    discardStats.update(updatedVal)

############################################ clustering remaining data #################################################

while chunkCount <= 4:

    # load the next 20% of the data from the file
    remainingData = []
    start = prevEnd
    end = start + sampleSize
    if chunkCount != 4:
        currentData = data[start:end]
    else:
        currentData = data[start:]

    prevEnd = end

    # call the DS stats update function to calc MD value
    remainingData = updateDS(currentData)

    # call the CS stats update function to calc MD value --> current condition never compressed sets formed
    if len(compressStats) > 0:
        if remainingData:
            remainingData = updateCS(remainingData)

    if remainingData:
        for line in remainingData:
            newPoint = line[0]
            retainedSet.append(newPoint)

    # again run kmeans on the remaining RS points to get the CS clusters
    rsDimension = []
    compLabel = {}
    for point in retainedSet:
        rsDimension.append(ptDict[point])

    minVal = min((5 * K), len(retainedSet))
    csClusterIndex = KMeans(n_clusters= minVal).fit_predict(rsDimension)

    # iteration to collect the list of points that are present in a cluster
    i = 0
    for index in csClusterIndex:
        if index in compLabel:
            compLabel[index].append(retainedSet[i])
        else:
            compLabel[index] = [retainedSet[i]]
        i += 1


    for key, value in compLabel.items():

        if len(compLabel[key]) > 1:

            # value of the form [0,1,2] - for each point get the corresponding datapoints and that becomes set of datapoints
            pointList = value
            tempDimList = []
            for point in pointList:
                tempDimList.append(ptDict[point])  # [[-1,2,-4], [0.3,2,4]]
                retainedSet.remove(point)

            for point in pointList:
                if csLabel in csCluster and point not in csCluster[csLabel]:
                    csCluster[csLabel].append(int(point))
                else:
                    csCluster[csLabel] = [int(point)]

            # in case if added to the CS set remove the points from retained set
            X = np.array(tempDimList)
            N = len(tempDimList)
            SUM = np.sum(X, axis=0)
            SUMSQ = np.sum(np.square(X), axis=0)
            centroid = SUM / N
            variance = (SUMSQ / N) - (np.square(SUM / N))
            standardDev = np.sqrt(variance)

            # dictionary for each cluster index with the values N, SUM and SUMSQ {0: (N, SUM, SUMSQ), 1:(N, SUM, SUMSQ), etc...}
            compressStats[csLabel] = (N, SUM, SUMSQ, centroid, variance, standardDev)
            csLabel += 1

    toDelete = []
    # stores the list of keys
    tempComList = list(compressStats.keys())
    initLen = len(tempComList)

    # merge two CS clusters
    for firstIndex in tempComList:

        currLen = len(tempComList)
        minMD = sys.maxsize
        secondVal = tuple()
        firstVal = tuple()
        minIndex = -1

        if firstIndex not in toDelete:

            firstVal = compressStats[firstIndex]
            md = 0
            centroid1 = firstVal[3]
            stdDev1 = firstVal[5]

            for secondIndex in tempComList:

                if firstIndex != secondIndex and secondIndex not in toDelete:
                    secondVal = compressStats[secondIndex]
                    centroid2 = secondVal[3]
                    stdDev2 = secondVal[5]

                    # calculate the MD between the clusters using the centroid and the standard deviation
                    dr = np.square(stdDev1) * np.square(stdDev2)
                    nr = np.square(centroid1 - centroid2)
                    md = np.sqrt(np.sum(nr / dr))

                    if md < minMD:
                        minMD = md
                        minIndex = secondIndex

            if minMD < distance and minIndex > -1:
                mergeCsToCs(firstIndex,minIndex)
                toDelete.append(minIndex)
                tempComList.remove(minIndex)
                del compressStats[minIndex]

                l1 = csCluster[firstIndex]
                l2 = csCluster[minIndex]
                csCluster[firstIndex] = l1 + l2
                del csCluster[minIndex]

    noOfDSPoints = 0
    for key, value in discardStats.items():
        noOfDSPoints += value[0]

    noOfCSPoints = 0
    for key, value in compressStats.items():
        noOfCSPoints += value[0]

    noOfCSCluster = len(compressStats)
    noOfRSPoints = len(retainedSet)

    if iteration < 4:
        interResult[iteration] = (noOfDSPoints, noOfCSCluster, noOfCSPoints, noOfRSPoints)
        iteration += 1

    chunkCount += 1

if chunkCount == 5:

    compKeys = list(compressStats.keys())
    discKeys = list(discardStats.keys())

    # merge two CS cluster with DS if the MD < 2* sqrt(d)
    for compressIndex in compKeys:

        stats = compressStats[compressIndex]
        md = 0
        minMD = sys.maxsize
        minDSIndex = -1
        centroid1 = stats[3]
        stdDev1 = stats[5]

        for discardIndex in discKeys:

            stat = discardStats[discardIndex]
            centroid2 = stat[3]
            stdDev2 = stat[5]

            # calculate the MD between the clusters using the centroid and the standard deviation
            dr = np.square(stdDev1) * np.square(stdDev2)
            nr = np.square(centroid1 - centroid2)
            md = np.sqrt(np.sum(nr / dr))

            if md < minMD:
                minMD = md
                minDSIndex = discardIndex

        if minMD < distance and minDSIndex > -1:

            mergeCsToDs(compressIndex, minDSIndex)
            compKeys.remove(compressIndex)
            del compressStats[compressIndex]

            compPoint = csCluster[compressIndex]
            for val in compPoint:
                totalIndex[val] = minDSIndex
            del csCluster[compressIndex]

noOfDSPoints = 0
for key, value in discardStats.items():
    noOfDSPoints += value[0]

noOfCSPoints = 0
for key, value in compressStats.items():
    noOfCSPoints += value[0]

noOfCSCluster = len(compressStats)
noOfRSPoints = len(retainedSet)

if iteration == 4:
    interResult[iteration] = (noOfDSPoints, len(compressStats), noOfCSPoints, len(retainedSet))

accuracy = normalized_mutual_info_score(totalIndex, originalIndex)
print("Accuracy", accuracy)

################################################# File Output ##########################################################
f = open('output.txt', 'w')
f.write("The intermediate results:")
f.write("\n")
for i in interResult:
    current = str(i+1)
    s = str("Round "+current+": "+str(interResult[i][0])+","+str(interResult[i][1])+","+str(interResult[i][2])+","+str(interResult[i][3]))
    f.write(str(s))
    f.write("\n")

f.write("\n")
f.write("The clustering results:")
f.write("\n")
for i in range(0, len(totalIndex)):
    s = str(i)+","+str(totalIndex[i])
    f.write(s)
    f.write("\n")

f.close()

print("Duration:", time.time()-startTime)

