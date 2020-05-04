#path to the blackbox file included in C:\Python36\Lib
from blackbox import BlackBox
import sys
import time
import binascii

start = time.time()

# command line arguments
dataPath = "C:\\Users\\sanka\\Desktop\\Study\\Sem3\\DM\\Assignment\\Assignment5\\users.txt"
streamSize = 100
numOfAsks = 50
# ouputPath = sys.argv[4]

# global variables
A = [0] * 69997
hashList = 20
m = len(A)
fp = 0
tn = 0

a = [5011, 5021, 5023, 5039, 5051, 5059, 5077, 5081, 5087, 5099, 5101, 5107, 5113, 5119, 5147, 5153, 5167, 5171, 5179, 5189, 5197, 5209, 5227, 5231, 5233, 5237, 5261, 5273, 5279, 5281, 5297, 5303, 5309, 5323, 5333, 5347, 5351, 5381, 5387, 5393, 5399, 5407, 5413, 5417, 5419, 5431, 5437, 5441, 5443, 5449, 5471, 5477, 5479, 5483, 5501, 5503, 5507, 5519, 5521, 5527, 5531, 5557, 5563, 5569, 5573, 5581, 5591, 5623, 5639, 5641, 5647, 5651, 5653, 5657, 5659, 5669, 5683, 5689, 5693, 5701, 5711, 5717, 5737, 5741, 5743, 5749, 5779, 5783, 5791, 5801, 5807, 5813, 5821, 5827, 5839, 5843]
b = [2069, 2081, 2083, 2087, 2089, 2099, 2111, 2113, 2129, 2131, 2137, 2141, 2143, 2153, 2161, 2179, 2203, 2207, 2213, 2221, 2237,2239, 2243, 2251, 2267, 2269, 2273, 2281, 2287, 2293, 2297, 2309, 2311, 2333, 2339, 2341, 2347, 2351, 2357, 2371, 2377, 2381,2383, 2389, 2393, 2399, 2411, 2417, 2423, 2437, 2441, 2447, 2459, 2467, 2473, 2477, 2503, 2521, 2531, 2539, 2543, 2549, 2551,2557, 2579, 2591, 2593, 2609, 2617, 2621, 2633, 2647, 2657, 2659, 2663, 2671, 2677, 2683, 2687, 2689, 2693, 2699, 2707, 2711,2713, 2719, 2729, 2731, 2741, 2749, 2753, 2767, 2777, 2789, 2791, 2797, 2801, 2803, 2819, 2833, 2837, 2843, 2851, 2857, 2861,2879, 2887, 2897]

visited = set()
fprList = []

def myhashs(user):
    result = []
    userNum = int(binascii.hexlify(user.encode('utf8')),16)
    for i in range(0,hashList):
        result.append((a[i] * userNum + b[i])%m)
    return result

def bloomFilter(inputStream):

    global fp
    global tn

    for element in inputStream:
        interList = []
        elementNum = int(binascii.hexlify(element.encode('utf8')),16)
        for i in range(0,hashList):
            bitPosition = (a[i] * elementNum + b[i])%m
            if A[bitPosition] == 0:
                interList.append(A[bitPosition])
                A[bitPosition] = 1
            else:
                interList.append(A[bitPosition])

        temp = 0
        for val in interList:
           if val == 1:
               temp += 1

        if temp == hashList and element not in visited:
            fp += 1

        if 0 in interList:
            tn += 1

        visited.add(element)

    # end of for

    if fp != 0 or tn != 0:
        fprVal = (fp/(fp+tn))
    else:
        fprVal = 0

    # fpr calculation
    fprList.append(fprVal)

# simulating the streaming process
bx = BlackBox()
for _ in range(numOfAsks):
    streamUsers = bx.ask(dataPath,streamSize)
    bloomFilter(streamUsers)

# write file output with header - "Time,FPR"
f = open('task1.txt', 'w+')
f.write("Time,FPR")
for i in range(0,len(fprList)):
    f.write("\n")
    f.write(str(i)+","+str(fprList[i]))
f.close()


# duration within 100s for 30 asks
print("Duration:", time.time()-start)

