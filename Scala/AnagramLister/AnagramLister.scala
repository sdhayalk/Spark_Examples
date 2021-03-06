import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AnagramLister	{
	def reduceDuplicates(args: (String, String)) :(String, Array[String]) = {
		val key = args._1
		val value = args._2

		val valueSplitted = value.split(",")
		val valueSet = valueSplitted.toSet
		val valueArray = valueSet.toArray

		(key, valueArray)
	}
	

	def main(args: Array[String])	{
		val inputFile = args(0)
		val outputFile = args(1)
		val conf = new SparkConf().setAppName("anagram lister")		
		val sc = new SparkContext(conf)

		val inputData = sc.textFile(inputFile)
		val rdd = inputData.flatMap(line => line.split(" "))
		val anagramMap = rdd.map(x => (x.sorted, x))
		val anagramMapTuple = anagramMap.reduceByKey((x,y) => x+","+y)
		val reduceDup = anagramMapTuple.map(x => reduceDuplicates(x))
		val result = reduceDup.filter(x => x._2.length > 1)

		result.saveAsTextFile(outputFile)
	}
}