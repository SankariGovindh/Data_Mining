from blackbox import BlackBox
import time
import sys
import random

start = time.time()

# command line arguments
dataPath = "C:\\Users\\sanka\\Desktop\\Study\\Sem3\\DM\\Assignment\\Assignment5\\users.txt"
streamSize = 100
numOfAsks = 30
# ouputPath = sys.argv[4]

# global variables
reservoirList = []
sequenceList = []
random.seed(553)

def reservoirSample(inputStream):

    global reservoirList
    global sequenceList

    if len(reservoirList) == 0:
        reservoirList = inputStream

    if len(sequenceList) == 0:
        sequenceList.append((len(inputStream), reservoirList[0], reservoirList[20], reservoirList[40], reservoirList[60], reservoirList[80]))
    else:
        # code to decide - keep / discard
        n = len(inputStream)
        for i in range(0,n):
            j = random.randint(0,100000) % 100
            if j < streamSize:
                # position within the streamSize found - so keep the element to maintain the probability s/n
                # already existing jth element in reservoirList discarded and the new element added
                reservoirList[j] = inputStream[i]

        sequenceNumber = (sequenceList[len(sequenceList) - 1][0]) + 100
        sequenceList.append((sequenceNumber, reservoirList[0], reservoirList[20], reservoirList[40], reservoirList[60], reservoirList[80]))

# simulating the streaming process
bx = BlackBox()
for _ in range(numOfAsks):
    streamUsers = bx.ask(dataPath,streamSize)
    reservoirSample(streamUsers)

# write file output with header - "Time,Ground Truth,Estimation"
f = open('task3.csv', 'w+')
f.write("seqnum,0_id,20_id,40_id,60_id,80_id")
for val in sequenceList:
    f.write("\n")
    f.write(str(val[0])+","+str(val[1])+","+str(val[2])+","
            +str(val[3])+","+str(val[4])+","+str(val[5]))
f.close()

# duration within 100s for 30 asks
print("Duration:", time.time()-start)