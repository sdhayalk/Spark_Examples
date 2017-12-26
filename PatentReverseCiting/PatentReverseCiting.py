from pyspark import SparkContext, SparkConf
import sys

inputs = sys.argv[1]
outputs = sys.argv[2]

conf = SparkConf().setAppName("patent reverse citing")
conf.set("spark.hadoop.validateOutputSpecs", "false")
context = SparkContext(conf=conf)

rdd = context.textFile(inputs)\
			 .filter(lambda x:x!='"CITING","CITED"')\
			 .map(lambda x:x.split(','))\
			 .map(lambda x:(x[1].encode("utf-8"), x[0].encode("utf-8")))\
			 .reduceByKey(lambda x,y: x+','+y)\
			 .coalesce(1)\
			 .saveAsTextFile(outputs)
