import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object LeftOuterJoin	{
	def main(args: Array[String])	{
		val table1File = args[0]
		val table2File = args[1]
		val joinKeyIndex1 = args[2].toInt
		val joinKeyIndex2 = args[3].toInt
		val delimiter = args[4]

		val conf = new SparkConf().setAppName("left outer join")		
		val sc = new SparkContext(conf)

		val table1 = sc.textFile(table1File).map(line => line.split(delimiter))
		val table2 = sc.textFile(table2File).map(line => line.split(delimiter))
		val joinKey1 = table1.map(x => x(joinKeyIndex1))
		val joinKey2 = table2.map(x => x(joinKeyIndex2))

		val joinKeyIntersection = joinKey1.intersection(joinKey2)
		val bcast = sc.broadcast(joinKeyIntersection.collect)

		val resultTable1 = table1.map(x => (x(joinKeyIndex1), x.mkString(",")))
		val resultTable2_1 = table2.filter(x2 => bcast.value.filter(y2 => y2 == x2(joinKeyIndex2)).length > 0).map(x=>(x(joinKeyIndex2), x.mkString(",")))
		val joinKey2MinusjoinKeyIntersection = joinKey1.filter(k1 => bcast.value.filter(k2 => k2==k1).length == 0)

		var nullString = ""
		var firstElement = true
		for(i <- 1 to table2.first.length)	{
			if(firstElement)	{
				nullString = "null"
				firstElement = false
			}
			else	{
				nullString = nullString + ",null"
			}
		}

		val resultTable2_2 = joinKey2MinusjoinKeyIntersection.map(x => (x, nullString))
		val resultTable2 = resultTable2_1.union(resultTable2_2)
		val result = resultTable1.union(resultTable2).reduceByKey((x,y) => x+","+y)

		result.saveAsTextFile(outputFile)
	}
}