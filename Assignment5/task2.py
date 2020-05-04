#path to the blackbox file included in C:\Python36\Lib
from blackbox import BlackBox
import sys
import time
import binascii
import statistics


start = time.time()

# command line arguments
dataPath = "C:\\Users\\sanka\\Desktop\\Study\\Sem3\\DM\\Assignment\\Assignment5\\users.txt"
streamSize = 300
numOfAsks = 50
# ouputPath = sys.argv[4]

# global variables
hashList = 50
a = [449, 457, 461, 463, 467, 479, 487, 491, 499, 503, 509, 521, 523, 541, 547, 557, 563, 569, 571, 577, 587, 593, 599, 601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983]
b = [983, 977, 971, 967, 953, 947, 941, 937, 929, 919, 911, 907, 887, 883, 881, 877, 863, 859, 857, 853, 839, 829, 827, 823, 821, 811, 809, 797, 787, 773, 769, 761, 757, 751, 743, 739, 733, 727, 719, 709, 701, 691, 683, 677, 673, 661, 659, 653, 647, 643, 641, 631, 619, 617, 613, 607, 601, 599, 593, 587, 577, 571, 569, 563, 557, 547, 541, 523, 521, 509, 503, 499, 491, 487, 479, 467, 463, 461, 457, 449]
m = 821
ansList = []

def myhashs(user):
    result = []
    userNum = int(binascii.hexlify(user.encode('utf8')),16)
    for i in range(0,hashList):
        result.append((a[i] * userNum + b[i])%m)
    return result

def flajMartin(inputStream):

    # store the list of unique elements in the list
    groundTruth = len(set(inputStream))

    # perform Flajolet Martin algorithm
    estimateList = []
    for i in range(0,hashList):
        maxHashVal = 0
        for element in inputStream:
            elementNum = int(binascii.hexlify(element.encode('utf8')), 16)
            hashNum = (a[i] * elementNum + b[i]) % m
            hashVal = '{0:08b}'.format(hashNum)
            trailLen = len(str(hashVal)) - len(str(hashVal).rstrip('0'))
            maxHashVal = max(trailLen, maxHashVal)
        estimateVal = pow(2,maxHashVal)
        estimateList.append(estimateVal)

    # combining the estimates
    chunks = [estimateList[x:x + 10] for x in range(0, len(estimateList),10)]
    average = []
    for subList in chunks:
        temp = sum(subList)/len(subList)
        average.append(temp)

    fmVal = round(statistics.median(average))
    ansList.append((groundTruth,fmVal))

    # est = 0
    # gt = 0
    # for value in ansList:
    #     est += value[0]
    #     gt += value[1]
    # final = est / gt

# simulating the streaming process
bx = BlackBox()
for _ in range(numOfAsks):
    streamUsers = bx.ask(dataPath,streamSize)
    flajMartin(streamUsers)

# write file output with header - "Time,Ground Truth,Estimation"
f = open('task2.csv', 'w+')
f.write("Time,Ground Truth,Estimation")
for i in range(0,len(ansList)):
    f.write("\n")
    f.write(str(i)+","+str(ansList[i][0])+","+str(ansList[i][1]))
f.close()

# duration within 100s for 30 asks
print("Duration:", time.time()-start)

