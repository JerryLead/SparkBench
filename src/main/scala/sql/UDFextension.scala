package sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lijie on 16-5-18.
  */
object UDFextension {
  def curryFunc(link: String) = udf((col: Int) => col + link + col)
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("UDFextension").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val df = sc.makeRDD(1 to 5).toDF("a")
    val func = udf((col: Int, link: String) => col + link + col)
    // val result = df.select($"a", func($"a", lit("-")))
    val result = df.select($"a", curryFunc("-")($"a"))
    result.show()
  }

}
