import time
from pyspark import SparkContext
import json
import sys

startTime = time.time()
sc = SparkContext('local[*]','partitionAnalysis')
sc.setLogLevel("ERROR")
review_file_path = 'C:\\Users\\sanka\\Desktop\\Study\\Sem3\\DM\\Assignment\\review.json'
review = sc.textFile(review_file_path)
reviewRDD = sc.textFile(review_file_path)


##common mapper for both the default and custom partitioning
businessRDD = reviewRDD.map(json.loads)\
                       .map(lambda x: (x['business_id'],1))\
                       .persist()

##finding the top 10 business and the corresponding reviews using the default partition function

defaultFnStart = time.time()
buzzList    = businessRDD.reduceByKey(lambda a,b: a+b) \
                         .takeOrdered(10,lambda sort: (-sort[1], sort[0]))

defaultPartition =  businessRDD.getNumPartitions()
defaultItems = businessRDD.glom().map(len).collect()
defaultTimeTaken = time.time()-defaultFnStart

print("Time in default partitioning:", defaultTimeTaken)

##finding the top 10 business and the corresponding reviews using custom partition function

numOfPartitions = 10

def customPartition(business):
    print(business)
    return hash(business) % numOfPartitions

#numOfPartitions = 160
subRDD = businessRDD.partitionBy(numOfPartitions, customPartition)
customPartition = subRDD.getNumPartitions()
custFnStart = time.time()
reviewList = subRDD.reduceByKey(lambda  a,b:a+b)\
                   .takeOrdered(10, lambda arrange: (-arrange[1], arrange[0]))

customItems = subRDD.glom().map(len).collect()
customTimeTaken = time.time()-custFnStart

## converting list to json format
partDict = {
    "default":{
        "n_partition": defaultPartition,
        "n_items": defaultItems,
        "exe_time": defaultTimeTaken
    },
    "customized":{
        "n_partition": customPartition,
        "n_items": customItems,
        "exe_time": customTimeTaken
    }
}

json_object = json.dumps(partDict)
with open("partitionResult.json", "w") as outfile:
    outfile.write(json_object)

execTime = time.time()-startTime
print("Total execution time taken:", execTime)