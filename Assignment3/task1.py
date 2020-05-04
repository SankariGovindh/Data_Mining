import time
from pyspark import SparkContext
import sys
from itertools import combinations

start = time.time()
sc = SparkContext('local[*]', 'lshSimilarity')
sc.setLogLevel("ERROR")
file_path = sys.argv[1]
data = sc.textFile(file_path)
groundTruth = "pure_jaccard_similarity.csv"
truthData = sc.textFile(groundTruth)
outputFile = sys.argv[3]

n = 100
band = 50
rows = 2
candidateList = []
bandList = []
userDict = {}
count = 0
candRDD = sc.emptyRDD()


# using hash function to permute
def hashPermute(businessData):
    processData = businessData

    # using random variables a,b and the number of bins - m ; consider a map-reduce based approach
    previous = sys.maxsize

    # recall : 0.96 and precision : 1.00 - time : 92 seconds
    # a = [601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997, 1009, 1013, 1019, 1021, 1031, 1033, 1039, 1049, 1051, 1061, 1063, 1069, 1087, 1091, 1093, 1097, 1103, 1109, 1117, 1123, 1129, 1151, 1153, 1163, 1171, 1181, 1187, 1193, 1201, 1213, 1217, 1223, 1229, 1231, 1237, 1249, 1259, 1277, 1279, 1283, 1289, 1291, 1297, 1301, 1303, 1307, 1319]

    # c = [2069, 2081, 2083, 2087, 2089, 2099, 2111, 2113, 2129, 2131, 2137, 2141, 2143, 2153, 2161, 2179, 2203, 2207, 2213, 2221, 2237,2239, 2243, 2251, 2267, 2269, 2273, 2281, 2287, 2293, 2297, 2309, 2311, 2333, 2339, 2341, 2347, 2351, 2357, 2371, 2377, 2381,2383, 2389, 2393, 2399, 2411, 2417, 2423, 2437, 2441, 2447, 2459, 2467, 2473, 2477, 2503, 2521, 2531, 2539, 2543, 2549, 2551,2557, 2579, 2591, 2593, 2609, 2617, 2621, 2633, 2647, 2657, 2659, 2663, 2671, 2677, 2683, 2687, 2689, 2693, 2699, 2707, 2711,2713, 2719, 2729, 2731, 2741, 2749, 2753, 2767, 2777, 2789, 2791, 2797, 2801, 2803, 2819, 2833, 2837, 2843, 2851, 2857, 2861,2879, 2887, 2897]

    # p = [761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997, 1009, 1013, 1019, 1021, 1031, 1033, 1039, 1049, 1051, 1061, 1063, 1069, 1087, 1091, 1093, 1097, 1103, 1109, 1117, 1123, 1129, 1151, 1153, 1163, 1171, 1181, 1187, 1193, 1201, 1213, 1217, 1223, 1229, 1231, 1237, 1249, 1259, 1277, 1279, 1283, 1289, 1291, 1297, 1301, 1303, 1307, 1319, 1321, 1327, 1361, 1367, 1373, 1381, 1399, 1409, 1423, 1427, 1429, 1433, 1439, 1447, 1451, 1453, 1459, 1471, 1481, 1483, 1487, 1489, 1493, 1499, 1511, 1523, 1531, 1543, 1549, 1553, 1559, 1567, 1571, 1579, 1583, 1597, 1601, 1607, 1609, 1613, 1619, 1621, 1627, 1637, 1657, 1663, 1667, 1669]

    # reverse of a and p starting from 7000, recall :0.95 ,precision:1.0 ,time:91 seconds

    # recall : 0.88 and precision : 1.00 - time : 54 seconds - 60 - 30*2
    # c = [2897, 2887, 2879, 2861, 2857, 2851, 2843, 2837, 2833, 2819, 2803, 2801, 2797, 2791, 2789, 2777, 2767, 2753, 2749, 2741, 2731, 2729, 2719, 2713, 2711, 2707, 2699, 2693, 2689, 2687, 2683, 2677, 2671, 2663, 2659, 2657, 2647, 2633, 2621, 2617, 2609, 2593, 2591, 2579, 2557, 2551, 2549, 2543, 2539, 2531, 2521, 2503, 2477, 2473, 2467, 2459, 2447, 2441, 2437, 2423, 2417, 2411, 2399, 2393, 2389, 2383, 2381, 2377, 2371, 2357, 2351, 2347, 2341, 2339, 2333, 2311, 2309, 2297, 2293, 2287, 2281, 2273, 2269, 2267, 2251, 2243, 2239, 2237, 2221, 2213, 2207, 2203, 2179, 2161, 2153, 2143, 2141, 2137, 2131, 2129, 2113, 2111, 2099, 2089, 2087, 2083, 2081, 2069]

    # a = [2069, 2081, 2083, 2087, 2089, 2099, 2111, 2113, 2129, 2131, 2137, 2141, 2143, 2153, 2161, 2179, 2203, 2207,
    #      2213, 2221, 2237, 2239, 2243, 2251, 2267, 2269, 2273, 2281, 2287, 2293, 2297, 2309, 2311, 2333, 2339, 2341,
    #      2347, 2351, 2357, 2371, 2377, 2381, 2383, 2389, 2393, 2399, 2411, 2417, 2423, 2437, 2441, 2447, 2459, 2467,
    #      2473, 2477, 2503, 2521, 2531, 2539, 2543, 2549, 2551, 2557, 2579, 2591, 2593, 2609, 2617, 2621, 2633, 2647,
    #      2657, 2659, 2663, 2671, 2677, 2683, 2687, 2689, 2693, 2699, 2707, 2711, 2713, 2719, 2729, 2731, 2741, 2749,
    #      2753, 2767, 2777, 2789, 2791, 2797, 2801, 2803, 2819, 2833, 2837, 2843, 2851, 2857, 2861, 2879, 2887, 2897]

    # bands : 100 - 50*2
    # a - small, c - 2000 range and p - 7000 range ; recall:0.95 and precision: 1.00 - time: 88 seconds
    # m = 1153 / 671 - recall 0.95, time 88 / 89
    # m = 5000 - recall 0.92, time 89

    # bands : 60 - 30*2  - recall : 0.91, time - 54 seconds , a - 600 range, c - 2000 range and p - 7000 range : recall : 0.95 - 87 seconds
    a = [601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739,
         743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883,
         887, 907, 911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997, 1009, 1013, 1019, 1021, 1031, 1033,
         1039, 1049, 1051, 1061, 1063, 1069, 1087, 1091, 1093, 1097, 1103, 1109, 1117, 1123, 1129, 1151, 1153, 1163,
         1171, 1181, 1187, 1193, 1201, 1213, 1217, 1223, 1229, 1231, 1237, 1249, 1259, 1277, 1279, 1283, 1289, 1291,
         1297, 1301, 1303, 1307, 1319]

    # bands : 60 - 30*2 - recall : 0.91, time - 53 seconds
    # a = [283, 293, 307, 311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379, 383, 389, 397, 401, 409, 419, 421, 431, 433, 439, 443, 449, 457, 461, 463, 467, 479, 487, 491, 499, 503, 509, 521, 523, 541, 547, 557, 563, 569, 571, 577, 587, 593, 599, 601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911, 919]

    # bands : 60 - 30*2 - recall : 0.90, time - 54 seconds
    # a = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281, 283, 293, 307, 311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379, 383, 389, 397, 401, 409, 419, 421, 431, 433, 439, 443, 449, 457, 461, 463, 467, 479, 487, 491, 499, 503, 509]

    c = [2069, 2081, 2083, 2087, 2089, 2099, 2111, 2113, 2129, 2131, 2137, 2141, 2143, 2153, 2161, 2179, 2203, 2207,
         2213, 2221, 2237, 2239, 2243, 2251, 2267, 2269, 2273, 2281, 2287, 2293, 2297, 2309, 2311, 2333, 2339, 2341,
         2347, 2351, 2357, 2371, 2377, 2381, 2383, 2389, 2393, 2399, 2411, 2417, 2423, 2437, 2441, 2447, 2459, 2467,
         2473, 2477, 2503, 2521, 2531, 2539, 2543, 2549, 2551, 2557, 2579, 2591, 2593, 2609, 2617, 2621, 2633, 2647,
         2657, 2659, 2663, 2671, 2677, 2683, 2687, 2689, 2693, 2699, 2707, 2711, 2713, 2719, 2729, 2731, 2741, 2749,
         2753, 2767, 2777, 2789, 2791, 2797, 2801, 2803, 2819, 2833, 2837, 2843, 2851, 2857, 2861, 2879, 2887, 2897]

    p = [7411, 7417, 7433, 7451, 7457, 7459, 7477, 7481, 7487, 7489, 7499, 7507, 7517, 7523, 7529, 7537, 7541, 7547,
         7549, 7559, 7561, 7573, 7577, 7583, 7589, 7591, 7603, 7607, 7621, 7639, 7643, 7649, 7669, 7673, 7681, 7687,
         7691, 7699, 7703, 7717, 7723, 7727, 7741, 7753, 7757, 7759, 7789, 7793, 7817, 7823, 7829, 7841, 7853, 7867,
         7873, 7877, 7879, 7883, 7901, 7907, 7919, 7927, 7933, 7937, 7949, 7951, 7963, 7993, 8009, 8011, 8017, 8039,
         8053, 8059, 8069, 8081, 8087, 8089, 8093, 8101, 8111, 8117, 8123, 8147, 8161, 8167, 8171, 8179, 8191, 8209,
         8219, 8221, 8231, 8233, 8237, 8243, 8263, 8269, 8273, 8287, 8291, 8293, 8297, 8311, 8317, 8329, 8353, 8363,
         8369, 8377, 8387, 8389, 8419, 8423, 8429, 8431, 8443, 8447, 8461, 8467, 8501, 8513, 8521, 8527, 8537, 8539,
         8543, 8563, 8573, 8581, 8597, 8599, 8609, 8623, 8627, 8629, 8641, 8647, 8663, 8669, 8677, 8681, 8689, 8693,
         8699, 8707, 8713, 8719, 8731, 8737, 8741, 8747, 8753, 8761, 8779, 8783, 8803, 8807, 8819, 8821, 8831, 8837,
         8839, 8849, 8861, 8863, 8867, 8887]

    m = 671

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


# function to generate bands with r rows - check for the bands generated
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
def similarityMatch(businessList):
    candidateList = list(combinations(sorted(businessList), 2))
    return candidateList


# performing jaccard similarity with the original matrix
def jaccardSimilarity(candidate):
    candidateSet = list(candidate)
    c1 = set(mapDict[candidateSet[0]])
    c2 = set(mapDict[candidateSet[1]])
    intersection = len(c1 & c2)
    union = len(c1 | c2)
    jaccardValue = float(intersection) / float(union)

    return (((candidateSet[0], candidateSet[1]), jaccardValue))


########################################################################################################################

# map to maintain a dictionary of users
userRDD = data.map(lambda x: x.split(",")) \
    .map(lambda x: x[0]).collect()

for user in userRDD:
    if user in userDict.keys():
        userDict[user] += 1
    else:
        count += 1
        userDict[user] = count

# combine the mapped user index to the business id
mapRDD = data.map(lambda x: x.split(",")) \
    .map(lambda x: (x[1], [x[0]])) \
    .reduceByKey(lambda a, b: a + b)

hashRDD = mapRDD.map(lambda x: hashPermute(x)).partitionBy(10)
mapDict = mapRDD.collectAsMap()

# step to generate b bands and r rows -> [0, [(1,3), (b1)]]
bandGenerated = hashRDD.flatMap(lambda x: generateBand(x)).reduceByKey(lambda a, b: a + b).collect()
for k in range(0, band):
    rdd = sc.parallelize(bandGenerated[k][1]).reduceByKey(lambda a, b: a + b).filter(lambda x: len(x[1]) > 1) \
        .flatMap(lambda x: similarityMatch(x[1]))
    candRDD = candRDD.union(rdd)

candidatePair = candRDD.distinct()

# perform similarity between the rows and add the corresponding index to the candidate list - similarity >= 0.5
jr = candidatePair.map(lambda x: jaccardSimilarity(x)).filter(lambda x: x[1] >= 0.5)
jaccardRDD = jr.map(lambda x: (x[0][0], x[0][1])).collect()  # (b1,b2)

finalSorted = jr.collect()
finalSorted = sorted(finalSorted)

# false positive and false negative calculation
truthRDD = truthData.map(lambda x: x.split(",")).map(lambda x: (x[0], x[1])).collect()  # (b1,b2)
truePositive = len(set(jaccardRDD) & set(truthRDD))
precision = truePositive / len(jaccardRDD)
recall = truePositive / len(truthRDD)
print("precision:", precision)
print("recall:", recall)

# write the output to a CSV file with the business ids and the similarity
f = open(outputFile, 'w+')
f.write("business_id_1, business_id_2, similarity")
for i in finalSorted:
    f.write("\n")
    f.write(i[0][0]+","+i[0][1]+","+str(i[1]))
f.close()

print("Duration:", time.time() - start)