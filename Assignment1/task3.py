import time
from pyspark import SparkContext
import json
import sys

start = time.time()
sc = SparkContext('local[*]','averageAnalysis')
sc.setLogLevel("ERROR")
#review_file_path = 'C:\\Users\\sanka\\Desktop\\Study\\Sem3\\DM\\Assignment\\review.json'
review_file_path = sys.argv[1]
review = sc.textFile(review_file_path)
#business_file_path = 'C:\\Users\\sanka\\Desktop\\Study\\Sem3\\DM\\Assignment\\business.json'
business_file_path = sys.argv[2]
business = sc.textFile(business_file_path)

##finding the average stars per city - review.json and business.json
businessRDD = business.map(json.loads).map(lambda x:(x['business_id'], x['city'])).partitionBy(10)
reviewRDD = review.map(json.loads).map(lambda x:(x['business_id'], x['stars'])).partitionBy(10)
averageRDD = businessRDD.join(reviewRDD)\
                       .map(lambda x:(x[1][0],(x[1][1],1)))\
                       .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))\
                       .mapValues(lambda x:x[0]/x[1])

##finding the highest city with average sort value - Python way

startTime = time.time()
averageList = averageRDD.collect()
sortedAverage = sorted(averageList, key = lambda sort: (-sort[1], sort[0]))
for i in range(0,10):
    print("Average for top 10 cities - Python way",sortedAverage[i])
m1 = time.time()-startTime

##finding the highest city with average sort value - Spark way

sparkTime = time.time()
sparkAverage = averageRDD.sortBy(lambda high: (-high[1], high[0]))\
                         .take(10)
print("Average for top 10 cities - Spark way", sparkAverage)
m2 = time.time()-sparkTime

##sorting part A
joinedList = averageRDD.sortBy(lambda x: (-x[1],x[0])) \
                       .collect()

##writing the list of cities to a file with headers city,stars - descending order and lexicographic
averageResult = open("task3a.txt", "w")
averageResult.write("city, stars")
for value in joinedList:
    averageResult.write(str(value))
averageResult.close()

## converting list to json format
sortTime = {
    "m1": m1,
    "m2": m2
}

json_object = json.dumps(sortTime, indent=4)
with open("sortTime.json", "w") as outfile:
    outfile.write(json_object)

print("time taken task3:", time.time()-start)


