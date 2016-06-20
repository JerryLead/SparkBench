package sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lijie on 16-5-18.
  */
object AppendColumn {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SQLQuery").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val df = sc.makeRDD(Array((1, "A"), (2, "B"), (3, "C"))).toDF("Id", "Name")
    // val result = df.select($"*", concat($"*").as("UDF"))
    val result = df.select($"*", concat($"Id", $"Name").as("UDF"))
    result.show()
  }
}
