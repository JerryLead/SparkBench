package sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xulijie on 16-4-25.
  */
object SpecifySchemaTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SpecifySchemaTest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val people = sc.textFile("src/main/resources/people.txt")
    val schemaString = "name age"

    val schema = StructType(
      schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true))
    )

    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    peopleDataFrame.registerTempTable("people")
    val results = sqlContext.sql("SELECT name FROM people")
    results.map(t => "Name: " + t(0)).collect().foreach(println)

  }

}
