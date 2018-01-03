from pyspark import SparkContext, SparkConf
import sys

def appendWord(x, y):
	if x == y:
		return x
	else:
		return x + ',' + y

def removeDuplicates(x):
	str_array = x[1].split(',')
	str_set = set(str_array)
	return [x[0], str_set]

def length(x):
	arr = x.split(',')
	return len(arr)

inputs = sys.argv[1]
outputs = sys.argv[2]

conf = SparkConf().setAppName('anagram lister')
conf.set("spark.hadoop.validateOutputSpecs", "false")
context = SparkContext(conf=conf)
context._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", " ")

rdd = context.textFile(inputs)\
			 .filter(lambda x:x!=' ')\
			 .map(lambda x:x.encode("utf-8"))\
			 .map(lambda x:x.lower())\
			 .map(lambda x:(''.join(sorted(x)), x))\
			 .reduceByKey(lambda x,y: appendWord(x,y))\
			 .map(lambda x:removeDuplicates(x))\
			 .filter(lambda x:len(x[1])>=2)\
			 .coalesce(1)\
			 .saveAsTextFile(outputs)
