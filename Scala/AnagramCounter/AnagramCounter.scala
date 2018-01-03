import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AnagramCounter	{
	def reduceDuplicates(args: (String, String))	{
		val key = args._1
		val value = args._2

		val valueArray = value.split(" ")
		var valueSet = valueArray.toSet

		var valueString = ""
		for(element <- valueSet)	{
			valueString = valueString + element
		}

		println(key, valueString)

	}

	def main(args: Array[String])	{
		val inputFile = args(0)
		val outputFile = args(1)
		val conf = new SparkConf().setAppName("anagram counter")		
		val sc = new SparkContext(conf)

		val inputData = sc.textFile(inputFile)
		val rdd = inputData.flatMap(line => line.split(" "))
		val anagramMap = rdd.map(x => (x.sorted, x))
		val anagramMapTuple = anagramMap.reduceByKey((x,y) => x+","+y)
		val reduceDup = anagramMap.map(x => reduceDuplicates(x))


		result.saveAsTextFile(outputFile)

	}
}