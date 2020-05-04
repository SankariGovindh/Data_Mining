import time
from pyspark import SparkContext
import sys
import math
from itertools import combinations

start = time.time()
sc = SparkContext('local[*]', 'yelpCFAnalysis')
sc.setLogLevel("ERROR")
training_path = sys.argv[1]
trainingData = sc.textFile(training_path)
trainHeader = trainingData.first()
validation_path = sys.argv[2]
validationData = sc.textFile(validation_path)
valHeader = validationData.first()
outputFile = sys.argv[3]

# global variable declaration
nei = 50
n = 40
band = 20
rows = 2
candidateList = []
bandList = []
userDict = {}
count = 0
candRDD = sc.emptyRDD()
test = 0
weightDict = {}
totalAverage = 0.0


# computing the weight between one business and another
def weightCompute(bList):
    b1 = bList[0]
    b2 = bList[1]
    b1Sum = 0.0
    b2Sum = 0.0
    b1Den = 0.0
    b2Den = 0.0
    weight = 0
    tempSum = 0.0
    tempVal = 0.0
    wNumerator = 0.0

    u1List = mapDict[b1]
    u2List = mapDict[b2]

    c1 = set(u1List)
    c2 = set(u2List)
    commonList = list(c1 & c2)

    if len(commonList) == 0:
        weight = 0

    else:
        # list of user weights [(u2,r2), (u3,r3),etc...]
        u1 = trainingList[b1]
        u2 = trainingList[b2]

        for user in commonList:
            r1 = [y[0] for y in u1].index(user)
            tempSum = float(u1[r1][1]) - businessAverage[b1]

            r2 = [y[0] for y in u2].index(user)
            tempVal = float(u2[r2][1]) - businessAverage[b2]

            wNumerator += (tempSum * tempVal)
            b1Den += (tempSum * tempSum)
            b2Den += (tempVal * tempVal)

        if (b1Den != 0 and b2Den != 0):
            weight = wNumerator / (math.sqrt(b1Den) * math.sqrt(b2Den))
        else:
            weight = 0

    return (b1, b2, weight)


# computing the prediction of the rating for the business b given by the user u
def prediction(predict):
    numerator = 0.0
    denominator = 0.0
    pUser = predict[0]
    pBusiness = predict[1]
    prediction = 0

    if pBusiness in weightDict:

        # handle the case to check if the user is present in training data set - userRDD to userDict
        if pUser in userRDD:

            neighbourList = weightDict[pBusiness]
            neighbourSize = len(neighbourList)
            neighbourList = sorted(neighbourList, key=lambda t: -t[1])

            if neighbourSize >= nei:
                neighbourList = neighbourList[:nei]

            for business in neighbourList:

                tempStr = (pUser, business[0])
                if tempStr in itemRDD:
                    itemRating = itemRDD[tempStr]
                    weight = business[1]
                    numerator += float(itemRating) * weight
                    denominator += abs(weight)

                else:
                    weight = business[1]
                    numerator += userAverage[pUser] * weight
                    denominator += abs(weight)

            prediction = float(numerator / denominator)

        # business present, user not there
        else:
            neighbourList = weightDict[pBusiness]
            neighbourSize = len(neighbourList)
            neighbourList = sorted(neighbourList, key=lambda t: -t[1])

            if neighbourSize >= nei:
                neighbourList = neighbourList[:nei]

            for business in neighbourList:
                weight = business[1]
                numerator += float(businessAverage[business[0]]) * weight
                denominator += abs(weight)

            prediction = float(numerator / denominator)

    # user and business not present in the training data set
    elif pBusiness not in weightDict and pUser not in userRDD:
        prediction = totalAverage

    # business not present in training data set and user present
    else:
        prediction = userAverage[pUser]

    return prediction


########################################################################################################################
# LSH#

# using hash function to permute
def hashPermute(businessData):
    processData = list(businessData)

    # using random variables a,b and the number of bins - m ; consider a map-reduce based approach
    previous = sys.maxsize

    m = 1153

    a = [601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739,
         743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883,
         887, 907, 911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997, 1009, 1013, 1019, 1021, 1031, 1033,
         1039, 1049, 1051, 1061, 1063, 1069, 1087, 1091, 1093, 1097, 1103, 1109, 1117, 1123, 1129, 1151, 1153, 1163,
         1171, 1181, 1187, 1193, 1201, 1213, 1217, 1223, 1229, 1231, 1237, 1249, 1259, 1277, 1279, 1283, 1289, 1291,
         1297, 1301, 1303, 1307, 1319]

    c = [2069, 2081, 2083, 2087, 2089, 2099, 2111, 2113, 2129, 2131, 2137, 2141, 2143, 2153, 2161, 2179, 2203, 2207,
         2213, 2221, 2237, 2239, 2243, 2251, 2267, 2269, 2273, 2281, 2287, 2293, 2297, 2309, 2311, 2333, 2339, 2341,
         2347, 2351, 2357, 2371, 2377, 2381, 2383, 2389, 2393, 2399, 2411, 2417, 2423, 2437, 2441, 2447, 2459, 2467,
         2473, 2477, 2503, 2521, 2531, 2539, 2543, 2549, 2551, 2557, 2579, 2591, 2593, 2609, 2617, 2621, 2633, 2647,
         2657, 2659, 2663, 2671, 2677, 2683, 2687, 2689, 2693, 2699, 2707, 2711, 2713, 2719, 2729, 2731, 2741, 2749,
         2753, 2767, 2777, 2789, 2791, 2797, 2801, 2803, 2819, 2833, 2837, 2843, 2851, 2857, 2861, 2879, 2887, 2897]

    p = [761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911,
         919, 929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997, 1009, 1013, 1019, 1021, 1031, 1033, 1039, 1049,
         1051, 1061, 1063, 1069, 1087, 1091, 1093, 1097, 1103, 1109, 1117, 1123, 1129, 1151, 1153, 1163, 1171, 1181,
         1187, 1193, 1201, 1213, 1217, 1223, 1229, 1231, 1237, 1249, 1259, 1277, 1279, 1283, 1289, 1291, 1297, 1301,
         1303, 1307, 1319, 1321, 1327, 1361, 1367, 1373, 1381, 1399, 1409, 1423, 1427, 1429, 1433, 1439, 1447, 1451,
         1453, 1459, 1471, 1481, 1483, 1487, 1489, 1493, 1499, 1511, 1523, 1531, 1543, 1549, 1553, 1559, 1567, 1571,
         1579, 1583, 1597, 1601, 1607, 1609, 1613, 1619, 1621, 1627, 1637, 1657, 1663, 1667, 1669]

    minVal = 0
    minHash = []

    # hashing the user id and generating the signature matrix
    for i in range(0, n):
        for user in processData[1]:
            hv = (((a[i] * userDict[user]) + c[i]) % p[i]) % m
            minVal = min(previous, hv)
            previous = hv
        minHash.append(minVal)

    return (processData[0], minHash)


# function to generate bands with r rows
def generateBand(input):
    business_id = input[0]
    signature = list(input[1])
    i = 0
    bandList = []
    for b in range(0, band):
        row = []
        for r in range(0, rows):
            row.append(signature[i])
            i = i + 1
        bandList.append((b, [(tuple(row), [business_id])]))

    return bandList


# comparing each column similarity
def similarityMatch(bl):
    candidateList = list(combinations(sorted(bl), 2))
    return candidateList


########################################################################################################################

# map to maintain a dictionary of users
trainingData = trainingData.filter(lambda x: x != trainHeader).map(lambda x: x.split(",")).persist()
userRDD = trainingData.map(lambda x: x[0]).collect()

for user in userRDD:
    if user in userDict.keys():
        userDict[user] += 1
    else:
        count += 1
        userDict[user] = count

# combine the mapped user index to the business id
mapRDD = trainingData.map(lambda x: (x[1], [x[0]])) \
    .reduceByKey(lambda a, b: a + b)

# business -> [u1,u2,etc...]
mapDict = mapRDD.collectAsMap()

hashRDD = mapRDD.map(lambda x: hashPermute(x)).partitionBy(10, lambda x: hash(x))

# step to generate b bands and r rows -> [0, [(1,3), (b1)]]
bandGenerated = hashRDD.flatMap(lambda x: generateBand(x)).reduceByKey(lambda a, b: a + b).collect()
for k in range(0, band):
    rdd = sc.parallelize(bandGenerated[k][1]).reduceByKey(lambda a, b: a + b).filter(lambda x: len(x[1]) > 1) \
        .flatMap(lambda x: similarityMatch(x[1]))

    candRDD = candRDD.union(rdd)

########################################################################################################################
# driver program
# business_id -> [(user_id, rating)]
trainingList = trainingData.map(lambda x: (x[1], [(x[0], x[2])])) \
    .reduceByKey(lambda a, b: a + b).collectAsMap()

# (user, business) -> rating
itemRDD = trainingData.map(lambda x: ((x[0], x[1]), x[2])) \
    .reduceByKey(lambda a, b: a + b).collectAsMap()

# computing average per business
businessAverage = trainingData.map(lambda x: (x[1], (float(x[2]), 1))).reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: x[0] / x[1]).collectAsMap()

# computing average per user
userAverage = trainingData.map(lambda x: (x[0], (float(x[2]), 1))).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: x[0] / x[1]).collectAsMap()

# calling the weightCompute function on the candidate pair that is generated, dictionary containing each business and the corresponding weight
candidatePair = candRDD.distinct()

# dummy = candidatePair.count()
# print("candidate generated", dummy)

# b1 -> [(b2, w2), (b3, w3),....,(bn,wn)]
weightDict = candidatePair.map(lambda x: weightCompute(x)).filter(lambda a: a[2] > 0) \
    .map(lambda x: [(x[0], [(x[1], x[2])]), (x[1], [(x[0], x[2])])]) \
    .flatMap(lambda x: x).reduceByKey(lambda a, b: a + b).collectAsMap()

# average rating of the entire training data set
totalSum = trainingData.map(lambda x: float(x[2])).sum()
totalCount = trainingData.count()
totalAverage = float(totalSum / totalCount)

# reading the data from the validation data set into the validationRDD ; ((business_id, user_id), rating)
validationData = validationData.filter(lambda x: x != valHeader).map(lambda x: x.split(",")).persist()
predictionRDD = validationData.map(lambda x: (x[0], x[1], x[2], prediction(x)))
output = predictionRDD.map(lambda x: (x[0],x[1],x[3])).collect()
mse = predictionRDD.map(lambda x: abs(float(x[2]) - x[3])).map(lambda x: (x ** 2)).sum()

validationLength = validationData.count()

rmse = math.sqrt(mse / validationLength)
print("rmse", rmse)

# writing output to file
f = open(outputFile, 'w+')
f.write("user_id, business_id, prediction")
for i in output:
    f.write("\n")
    f.write(i[0][0]+","+i[0][1]+","+str(i[1]))
f.close()

print("Duration", time.time() - start)

"""
predictionRDD = validationData.map(lambda x: x.split(",")) \
                              .map(lambda x: (x[0], x[1])).map(lambda x: prediction(x))

validationRDD = validationData.map(lambda x:x.split(","))\
                              .map(lambda x:((x[0],x[1]),float(x[2])))
rmseRDD = sc.emptyRDD()
rmseRDD = predictionRDD.join(validationRDD).map(lambda x:abs(x[1][0]-x[1][1])).map(lambda x:(x**2)).sum()
rmse=math.sqrt(rmseRDD/validationLength)

"""