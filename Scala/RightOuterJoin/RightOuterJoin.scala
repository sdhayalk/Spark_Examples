import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object RightOuterJoin	{
	def main(args: Array[String])	{
		val table1File = args[0]
		val table2File = args[1]
		val joinKeyIndex1 = args[2].toInt
		val joinKeyIndex2 = args[3].toInt
		val delimiter = args[4]

		val conf = new SparkConf().setAppName("right outer join")		
		val sc = new SparkContext(conf)

		val table1 = sc.textFile(table1File).map(line => line.split(delimiter))
		val table2 = sc.textFile(table2File).map(line => line.split(delimiter))
		val joinKey1 = table1.map(x => x(joinKeyIndex1))
		val joinKey2 = table2.map(x => x(joinKeyIndex2))

		val joinKeyIntersection = joinKey1.intersection(joinKey2)
		val bcast = sc.broadcast(joinKeyIntersection.collect)

		val resultTable2 = table2.map(x => (x(joinKeyIndex2), x.mkString(",")))
		val resultTable1_1 = table1.filter(x1 => bcast.value.filter(y1 => y1 == x1(joinKeyIndex1)).length > 0).map(x=>(x(joinKeyIndex1), x.mkString(",")))
		val joinKey1MinusjoinKeyIntersection = joinKey2.filter(k2 => bcast.value.filter(k1 => k1==k2).length == 0)

		var nullString = ""
		var firstElement = true
		for(i <- 1 to table1.first.length)	{
			if(firstElement)	{
				nullString = "null"
				firstElement = false
			}
			else	{
				nullString = nullString + ",null"
			}
		}

		val resultTable1_2 = joinKey1MinusjoinKeyIntersection.map(x => (x, nullString))
		val resultTable1 = resultTable1_1.union(resultTable1_2)
		val result = resultTable2.union(resultTable1).reduceByKey((x,y) => x+","+y)

		result.saveAsTextFile(outputFile)
	}
}