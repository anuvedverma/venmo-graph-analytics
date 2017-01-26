from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext


# To run on cluster:
# conf = SparkConf().setAppName("spark-tests").setMaster("spark://ip-172-31-0-135:7077")
# sc = SparkContext(conf=conf)
# file = sc.textFile("hdfs://ec2-52-33-8-227.us-west-2.compute.amazonaws.com:9000/user/test.txt")

# To run locally:
# Create a local StreamingContext with two working thread and batch interval of 1 second
# sc = SparkContext("local[2]", "spark-streaming-test")
sc = SparkContext("local", "spark-tests")
file = sc.textFile("sample.txt").cache()  # Should be some file on your system
counts = file.flatMap(lambda line: line.split(" "))\
           .map(lambda word: (word, 1))\
           .reduceByKey(lambda a, b: a + b)
res = counts.collect()
for val in res:
     print val
