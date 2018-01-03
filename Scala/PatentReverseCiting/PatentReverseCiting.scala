import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PatentReverseCiting	{
	def main(args: Array[String])	{
		val inputFile = args(0)
		val outputFile = args(1)
		val conf = new SparkConf().setAppName("patent reverse citing")
		val sc = new SparkContext(conf)

		val inputData = sc.textFile(inputFile).filter(x => x != "\"CITING\",\"CITED\"")
		val rdd = inputData.map(line => line.split(","))
		val rddReverseTuple = rdd.map(x => (x(1), x(0)))
		val result = rddReverseTuple.reduceByKey((cite1, cite2) => cite1 + ',' + cite2)

		result.saveAsTextFile(outputFile)
	}
}