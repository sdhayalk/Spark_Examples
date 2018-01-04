import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object LeftOuterJoin	{

	def fillTupleOrNull(args: (org.apache.spark.rdd.RDD[String], , Array[String])) : (String, String) = {
		val primaryKey1 = args[0]
		val bcast = args[1]
		val x = args[2]
		val primaryKeyIndex2 = args[3]

		val count = bcast.value.filter(y1 => y1 == x(primaryKeyIndex1)).length > 0


	}

	def main(args: Array[String])	{
		val table1File = args[0]
		val table2File = args[1]
		val primaryKeyIndex1 = args[2].toInt
		val primaryKeyIndex2 = args[3].toInt
		val delimiter = args[4]

		val conf = new SparkConf().setAppName("left outer join")		
		val sc = new SparkContext(conf)

		val table1 = sc.textFile(table1File).map(line => line.split(delimiter))
		val table2 = sc.textFile(table2File).map(line => line.split(delimiter))
		val primaryKey1 = table1.map(x => x(primaryKeyIndex1))
		val primaryKey2 = table2.map(x => x(primaryKeyIndex2))

		val primaryKeyIntersection = primaryKey1.intersection(primaryKey2)
		val bcast = sc.broadcast(primaryKeyIntersection.collect)

		val resultTable1 = table1.map(x => (x(primaryKeyIndex2), x.mkString(",")))
		val resultTable2 = table2.map(x => fillTupleOrNull(primaryKey1, bcast, x))





		val resultTable1 = table1.filter(x1 => bcast.value.filter(y1 => y1 == x1(primaryKeyIndex1)).length > 0).map(x=>(x(primaryKeyIndex1), x.mkString(",")))
		val resultTable2 = table2.filter(x2 => bcast.value.filter(y2 => y2 == x2(primaryKeyIndex2)).length > 0).map(x=>(x(primaryKeyIndex2), x.mkString(",")))

		val result = resultTable1.union(resultTable2).reduceByKey((x,y) => x+","+y)

		result.saveAsTextFile(outputFile)
	}
}