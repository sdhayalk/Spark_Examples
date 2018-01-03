from pyspark import SparkContext, SparkConf
import sys

inputs = sys.argv[1]
outputs = sys.argv[2]

conf = SparkConf().setAppName('word count')
conf.set("spark.hadoop.validateOutputSpecs", "false")
context = SparkContext(conf=conf)
context._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", " ")

rdd = context.textFile(inputs)				\
			 .filter(lambda x:x!='')		\
			 .map(lambda x:(x.lower(), 1))	\
			 .reduceByKey(lambda x,y:x+y)	\
			 .coalesce(1)					\
			 .saveAsTextFile(outputs)

