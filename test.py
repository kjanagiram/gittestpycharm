from pyspark import SparkConf
from pyspark import SparkContext

conf=SparkConf().setMaster("local").setAppName("testap")
sc=SparkContext(conf=conf)

sc.setLogLevel("ERROR")

data=sc.textFile("C:\\Users\\SKR\\Desktop\\temp.txt")

lines =data.flatMap(lambda x: x.split(","))

words=lines.map(lambda x: (x,1))

wordcount=words.reduceByKey(lambda x,y: x+y)

for c in wordcount.collect():
    print(c)
