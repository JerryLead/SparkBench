package sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xulijie on 16-4-25.
  */
object DataFrameOps {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrameOps")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json("src/main/resources/peopleArray.json")
    df.show()
    df.printSchema()
    /*
    df.select("name").show()
    df.select(df("name"), df("age") + 1).show()
    df.filter(df("age") > 21).show()
    df.groupBy("age").count().show()
    */
  }

}
