import time
from pyspark import SparkContext
import json
import sys

start = time.time()
sc = SparkContext('local[*]','reviewAnalysis')
sc.setLogLevel("ERROR")
#input_file_path = 'C:\\Users\\sanka\\Desktop\\Study\\Sem3\\DM\\Assignment\\review.json'
input_file_path = sys.argv[1]
reviewRDD = sc.textFile(input_file_path,10).map(json.loads)

# ##total number of reviews
totalReview = reviewRDD.count()

##total number of reviews in 2018
yearlyReview  = reviewRDD.map(lambda x: (x['date'])) \
                         .filter(lambda year: "2018" in year).collect()

# ##number of distinct user reviews
userReview = reviewRDD.map(lambda x: (x['user_id'],1)).partitionBy(10, lambda key: hash(key))

distinctUser = userReview.map(lambda value:(value[0],value[1]))\
                         .reduceByKey(lambda a,b:a+b).collect()

# ##top 10 users with largest number of reviews and the number of reviews each - categorising based on user_id
topReviews = userReview.reduceByKey(lambda a,b: a+b) \
                       .takeOrdered(10,lambda sort: (-sort[1], sort[0]))

# ##number of distinct businesses that have been reviewed
bussReview = reviewRDD.map(lambda x: (x['business_id'],1)).partitionBy(10, lambda key: hash(key))

distinctBusiness = bussReview.map(lambda value:(value[0],value[1]))\
                             .reduceByKey(lambda a,b:a+b).collect()

# ##top 10 businesses that had the largest number of reviews and the number of reviews each
buzzReviews = bussReview.reduceByKey(lambda a,b: a+b) \
                       .takeOrdered(10,lambda sort: (-sort[1],sort[0]))

## converting list to json format
result = {
    "n_review": totalReview,
    "n_review_2018": len(yearlyReview),
    "n_user": len(distinctUser),
    "top10_user": topReviews,
    "n_business": len(distinctBusiness),
    "top10_business": buzzReviews
}

json_object = json.dumps(result)
with open("reviewResults.json", "w") as outfile:
	outfile.write(json_object)

print("time taken task1:", time.time()-start)
