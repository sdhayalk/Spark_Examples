import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount	{
	def main(args: Array[String])	{
		val inputFile = args(0)
		val outputFile = args(1)
		val conf = new SparkConf().setAppName("word count")
		val sc = new SparkContext(conf)

		val rdd = sc.textFile(inputFile)
		val rddSplit = rdd.flatMap(line => line.split(" "))
		val rddWordTuple = rddSplit.flatMap(word => (word, 1))
		val result = rddWordTuple.reduceByKey((count1, count2) => count1 + count2)

		result.saveAsTextFile(outputFile)
	}
}