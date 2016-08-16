package basic

import org.apache.spark.sql.SparkSession

/**
  * Created by xulijie on 16-6-30.
  */
object SparkWordCount {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("SparkWordCount")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "104857600")
      .getOrCreate()

    val filePath = "/Users/xulijie/Documents/data/RandomText/randomText-10MB.txt"
    val textFile = spark.sparkContext.textFile(filePath)
    val result = textFile.flatMap(_.split("[ |\\.]"))
      .map(word => (word, 1)).reduceByKey(_ + _)

    println(result.toDebugString)
    result.collect().foreach(println)
    spark.stop()
  }
}
