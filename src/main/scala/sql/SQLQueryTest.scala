package sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xulijie on 16-4-25.
  */
object SQLQueryTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SQLQuery").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val people = sqlContext.read.json("data/sql/people.json")
    people.registerTempTable("people")
    val results = sqlContext.sql("SELECT * FROM people")
    results.show()
  }

}
