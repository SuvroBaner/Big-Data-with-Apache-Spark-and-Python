# SparkContext : Fundamental to create RDDs
# SparkConf : It helps create SparkContext, like to run on a standalone computer or a cluster.
# collections : Python package for sorting

from pyspark import SparkConf, SparkContext
import collections

# Create the Spark Context
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# set master node as local machine (one thread)
# Appname is the job which will run on the Spark UI

# Now let's load up the data file-
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
# textFile breaks the file line by line, so that every line of text corresponds to one value in the RDD (over here it is lines)
# Userid MovieId Rating Timestamp
#196	242	3	881250949
#186	302	3	891717742
#22	377	1	878887116
#244	51	2	880606923

# Extract (MAP) the data we care about-
ratings = lines.map(lambda x: x.split()[2]) # it extracts 3rd element i.e. (they are movie ratings) and puts that into the new RDD called ratings
#3
#3
#1
#2 etc...
# Note the lines RDD remain untouched. This was a TRANSFORM process.

# Now, let's perform ACTION on this RDD, i.e. count by value
result = ratings.countByValue()  # how many times each unique value in the RDD occurs.
# (3, 2)
# (1, 1)
# (2, 1) etc.
# Note result is not an RDD, it's a python object as we perform Action here. In fact it is a tuple.

# Sort and display the results
sortedResults = collections.OrderedDict(sorted(result.items()))  # creating an ordered dictionary and sorting by key (review)
for key, value in sortedResults.items():  # iterate through every key value pair and print them out
    print("%s %i" % (key, value))

# On the command prompt run the following-
# spark-submit ml_ratings-counter.py