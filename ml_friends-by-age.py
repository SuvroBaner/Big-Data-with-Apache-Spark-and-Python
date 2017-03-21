# Import the relevant packages from pyspark
from pyspark import SparkConf, SparkContext

# Create the required Configuration and Context-
conf = SparkConf().setMaster("local").setAppName("FriendsByAge") # so, this program will run locally and will be called as FriendsByAge
sc = SparkContext(conf = conf)

# Let's read the file and store it in the RDD called 'lines' here

lines = sc.textFile("file:///SparkCourse/fakefriends.csv")  # the input data will get stored line by line

# Now, let's create a parser and parse the input data line by line-

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

rdd = lines.map(parseLine)  # each line from the text file gets parsed and gets stored in a new RDD called 'rdd'
# So, rdd contains a key/value pair of (age, numFriends)

#  Now perform operation which would give totalsByAge... the key will be Age, and the value will be (totalNumFriends, no. of occurances)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# the first part, the input goes as the value i.e. numFriends and then the transformed to (numFriends, 1) where 1 is the occurance
# the second part, the transform happens by the Key i.e. the age. The input is x, y i.e. (numFriends, 1) and it keeps summing up
# for the pair of (numFriends, 1) by the key.

averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])  # here, the input is (numFriends, n) and the transformation is to
# compute the average, i.e. numFriends/n

# Time for the action-
results = averagesByAge.collect()
# so, results is a list of tuples (age, average friends)

# Let's print it-

for result in results:
    print result