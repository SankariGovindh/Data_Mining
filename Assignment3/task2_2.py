import time
from pyspark import SparkContext
import xgboost as xgb
import numpy as np
from sklearn.metrics import mean_squared_error
import json

start = time.time()
sc = SparkContext('local[*]', 'modelBasedCF')
sc.setLogLevel("ERROR")

training_path = "C:\\Users\\sanka\\Desktop\\Study\\Sem3\\DM\\Assignment\\Assignment3\\data\\yelp_train.csv"
trainingData = sc.textFile(training_path)
#trainHeader = trainingData.first()
validation_path = "C:\\Users\\sanka\\Desktop\\Study\\Sem3\\DM\\Assignment\\Assignment3\\data\\yelp_val.csv"
validationData = sc.textFile(validation_path)
#valHeader = validationData.first()
busFile = "C:\\Users\\sanka\\Desktop\\Study\\Sem3\\DM\\Assignment\\Assignment3\\data\\business.json"
businessJSON = sc.textFile(busFile)
usFile = "C:\\Users\\sanka\\Desktop\\Study\\Sem3\\DM\\Assignment\\Assignment3\\data\\user.json"
userJSON = sc.textFile(usFile)
#output = sys.argv[3]

# global variable declaration
resultRDD = []
count = 0
bCount = 0
userDict = {}
busDict = {}

# matching features with the training and the validation data
def featureMatch(rowData):

    # user and business exist
    if rowData[0][0] in userRDD and rowData[0][1] in businessRDD:
        tupleVal = userRDD[rowData[0][0]]
        averageStars = float(tupleVal[0])
        userReview = float(tupleVal[1])
        busTuple = businessRDD[rowData[0][1]]
        busStars = float(busTuple[0])
        busReview = float(busTuple[1])
        userID = userDict[rowData[0][0]]
        busID = busDict[rowData[0][1]]

    # user not present in user.json
    elif rowData[0][0] not in userRDD and rowData[0][1] in businessRDD:
        busTuple = businessRDD[rowData[0][1]]
        busStars = float(busTuple[0])
        busReview = float(busTuple[1])
        userReview = userRevAvg
        averageStars = userAverage
        busID = busDict[rowData[0][1]]
        userID = userDict[rowData[0][0]]

    # business not present in business.json
    elif rowData[0][0] in userRDD and rowData[0][1] not in businessRDD:
        tupleVal = userRDD[rowData[0][0]]
        averageStars = float(tupleVal[0])
        userReview = float(tupleVal[1])
        busReview = busRevAvg
        busStars = businessAverage
        userID = userDict[rowData[0][0]]
        busID = busDict[rowData[0][1]]

        # business and user not present
    else:
        averageStars = userAverage
        userReview = userRevAvg
        busReview = busRevAvg
        busStars = businessAverage
        userID = userDict[rowData[0][0]]
        busID = busDict[rowData[0][1]]

    return (userID, busID, averageStars, userReview, busStars, busReview)


# combining the training RDD with the feature set
businessRDD = businessJSON.map(json.loads).map(lambda x: [x['business_id'], (x['stars'], x['review_count'])]).collectAsMap()
userRDD = userJSON.map(json.loads).map(lambda x: [x['user_id'], (x['average_stars'], x['review_count'])]).reduceByKey(lambda a, b: a + b).collectAsMap()

# (user_id, business_id), rating
trainingData = trainingData.map(lambda line: line.split(",")).persist()
validationData = validationData.map(lambda line: line.split(",")).persist()

# computing average across all user
userAv =  userJSON.map(json.loads).map(lambda x: x['average_stars']).sum()
userCount = userJSON.map(json.loads).count()
userAverage = float(userAv/userCount)

# computing average across all business
busAv =  businessJSON.map(json.loads).map(lambda x: x['stars']).sum()
busCount = businessJSON.map(json.loads).count()
businessAverage = float(busAv/busCount)

# total average for user review count
userReview = userJSON.map(json.loads).map(lambda x: x['review_count']).sum()
userRevAvg = float(userReview / userCount)

# total average for business review count
busReview = businessJSON.map(json.loads).map(lambda x: x['review_count']).sum()
busRevAvg = float(busReview / busCount)

vUser = set(validationData.map(lambda x: x[0]).collect())
vBusiness = set(validationData.map(lambda x: x[1]).collect())
tUser = set(trainingData.map(lambda x: x[0]).collect())
tBusiness = set(trainingData.map(lambda x: x[1]).collect())
allUser = list(tUser | vUser)
allBusiness = list(tBusiness | vBusiness)

# mapping user and business id with the user string
for user in allUser:
    if user in userDict.keys():
        userDict[user] += 1
    else:
        count += 1
        userDict[user] = count

for business in allBusiness:
    if business in busDict.keys():
        busDict[business] += 1
    else:
        bCount += 1
        busDict[business] = bCount

userKey = list(userDict.keys())
userValue = list(userDict.values())
busKey = list(busDict.keys())
busValue = list(busDict.values())

trainingRDD = trainingData.map(lambda x: ((x[0], x[1]), (x[2]))) \
                          .map(lambda x: featureMatch(x)).collect()

trainRating = trainingData.map(lambda x: float(x[2])).collect()

validationRDD = validationData.map(lambda x: ((x[0], x[1]), (x[2]))) \
                              .map(lambda x: featureMatch(x)).collect()

inputList = validationData.map(lambda x: (x[0], x[1])).collect()

validRating = validationData.map(lambda x: float(x[2])).collect()

########################################################################################################################

# convert RDD to numpy array
trainArray = np.array(trainingRDD)
trArray = np.array(trainRating)
validationArray = np.array(validationRDD)
vrArray = np.array(validRating)

X_train, Y_train = trainArray, trArray
X_test, Y_test = validationArray, vrArray

regressor = xgb.XGBRegressor()
xgbModel = regressor.fit(X_train, Y_train)
prediction = xgbModel.predict(X_test)


def result(x,y):
    for i in range(0, len(inputList)):
        resultRDD.append((x[i][0], x[i][1], y[i]))

#(user_id, business_id, prediction)
result(inputList, prediction)

# mse = validationData.map(lambda x: (x[0], x[1], x[2], result())).map(lambda x: abs(float(x[2]) - x[3])).map(lambda x: (x ** 2)).sum()
# validationLength = validationData.count()
# rmse = math.sqrt(mse / validationLength)
# print("rmse", rmse)

# writing output to file
f = open('output.csv', 'w+')
f.write("user_id, business_id, prediction")
for i in resultRDD:
    f.write("\n")
    f.write(i[0] + "," + i[1] + "," + str(i[2]))
f.close()

# RMSE calculation
print("RMSE:", np.sqrt(mean_squared_error(Y_test, prediction)))

# Time taken for execution
print("Duration:", time.time() - start)