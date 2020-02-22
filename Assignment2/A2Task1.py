import time
from pyspark import SparkContext
from itertools import combinations
import sys

start = time.time()
sc = SparkContext('local[*]','smallDataAnalysis')
sc.setLogLevel("ERROR")
first_file_path = 'C:\\Users\\sanka\\Desktop\\Study\\Sem3\\DM\\Assignment\\small2.csv'
#file_path = sys.argv[3]
#debFile = sc.textFile(file_path)
debFile = sc.textFile(first_file_path)
debHeader = debFile.first()

frequentItem = []
s = 9

#case = sys.argv[2]
case = 2


##A-priori to create frequent singletons, pairs up to k sizes
#taking the initial partition generate the candidate sets of size up to k

def apriori(keys):

    k = 1
    items = list(keys)
    singletonList = []

    # dictionary to store the items and the corresponding count as key-value pair
    countDict =  {}

    for item in items:
        for value in item:
            if value in countDict.keys():
                countDict[value] += 1
            else:
                countDict[value] = 1

    # counting the number of items corresponding to each element
    candidateItem = []
    for key in countDict.keys():

        support = (len(items)/rddLength)*s
        if (countDict.get(key) >= support):
            candidateItem.append((key,))
            singletonList.append(key)

    singletonList.sort()

    #incrementing the k value
    k += 1

    # generating combinations for k size
    tempDict = {'a':2,'b':1} #dummy initialization to begin while

    while(len(tempDict)>1):

        print("k", k)
        tempDict = {}

        # for loop to generate pair
        if (k == 2):

            tempPair = []
            tempList = []

            for i in range(0, len(singletonList)):
                for j in range((i + 1), len(singletonList)):
                    tempTuple = (singletonList[i], singletonList[j])
                    tempPair.append(tempTuple)
            tempPair.sort()

            for item in items:
                for value in tempPair:
                    if set(value).issubset(set(item)):
                        if value in tempDict.keys():
                            tempDict[value] += 1
                        else:
                            tempDict[value] = 1


            # check if the values are >= support threshold
            support = (len(items)/rddLength)*s
            for key in tempDict.keys():
                if (tempDict.get(key) >= support):
                    candidateItem.append(key)
                    tempList.append(key)
            tempList.sort()

        #for size greater than k
        else:

            tempCombo = tempList
            tempList = []

            #generating combinations of the list for size > k and checking if the subsets of the tempList are frequent
            for i in range(0, len(tempCombo)):
                for j in range((i + 1), len(tempCombo)):
                    tempTuple = sorted(tuple(set(tempCombo[i]+tempCombo[j])))

                    if len(tempTuple) == k and tempTuple not in tempList:
                        subsetCombo = list(combinations(tempTuple, k - 1))
                        subsetCount = 0
                        for item in subsetCombo:
                            if item in tempCombo:
                                subsetCount += 1 #counter to track the subsets
                        if subsetCount == len(subsetCombo):
                            tempList.append(tuple(tempTuple))

            tempList = list(set(tempList))
            tempList.sort()

            #checking if the generated combination is a subset of the given bucket in the partition
            for item in items:
                for value in tempList:
                    if set(value).issubset(item):
                        if value in tempDict.keys():
                            tempDict[value] += 1
                        else:
                            tempDict[value] = 1
            tempList = []

            # check if the values are >= support threshold
            support = (len(items) / rddLength) * s
            for key in tempDict.keys():
                if (tempDict.get(key) >= support):
                        candidateItem.append(key)
                        tempList.append(key) #storing only the candidate items for the next iteration
            tempList.sort()

        k += 1

    return candidateItem


##function definition for pass 2 of SON
def son2(mainChunk):

    freqDict = {}
    freqSet = []

    #consider the candidate frequent item set for the current size and compare with the mainChunk
    baskets = list(mainChunk)

   #check for the candidate item in the basket
    for basket in baskets:
        for candidate in candidateList:
            if set(candidate).issubset(set(basket)):
                if candidate in freqDict.keys():
                    freqDict[candidate] += 1
                else:
                    freqDict[candidate] = 1

    #appending the key and value pair to the set
    for key, value in freqDict.items():
        freqSet.append((key,value))

    return freqSet

########################################################################################################################

##case 1 - finding the user basket with unique business id
##user1 = [b1,b2,b3]
##for every user_id -> group the corresponding business_id and this should be unique

if case == 1:

    subsetRDD = debFile.filter(lambda x: x != debHeader) \
        .map(lambda line: line.split(",")) \
        .map(lambda x: (x[0], x[1])) \
        .groupByKey().mapValues(list).map(lambda u: u[1])

    rddLength = len(subsetRDD.collect())

    # pass1 - SON algorithm
    pass1RDD = subsetRDD.mapPartitions(apriori).distinct().collect()
    candidateList = pass1RDD
    candidateList.sort()
    print("Candidates:", candidateList)

    # pass2 - SON algorithm
    pass2RDD = subsetRDD.mapPartitions(son2).reduceByKey(lambda a,b:a+b).filter(lambda x:x[1]>=s).map(lambda x:x[0]).collect()
    frequentItem = pass2RDD
    print("Frequent Items:", frequentItem)

########################################################################################################################

##case 2 - finding the business basket with unique user id
##business1 = [u1,u2,u3]
##for every business_id -> group the corresponding user_id that is unique

else:
    #pass1 - SON algorithm
    userRDD = debFile.filter(lambda x:x!=debHeader)\
                      .map(lambda line:line.split(","))\
                      .map(lambda x:(x[1],x[0]))\
                      .groupByKey().mapValues(list).map(lambda b:b[1])

    rddLength = len(userRDD.collect())

    pass1RDD = userRDD.mapPartitions(apriori).distinct().collect()
    candidateList = pass1RDD
    candidateList.sort()
    print("Candidates:", candidateList)

    # pass2 - SON algorithm
    pass2RDD = userRDD.mapPartitions(son2).reduceByKey(lambda a,b:a+b).filter(lambda x:x[1]>=s).map(lambda x:x[0]).collect()
    frequentItem = pass2RDD
    print("Frequent Items:", frequentItem)

print("Duration:", time.time()-start)
