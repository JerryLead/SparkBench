package sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xulijie on 16-4-26.
  */
object LoadSaveData {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Load/Save Data").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.load("src/main/resources/users.parquet")
    df.show()
    df.select("name", "favorite_color").write.save("/tmp/namesAndFavColors.parquet")


  }

}
