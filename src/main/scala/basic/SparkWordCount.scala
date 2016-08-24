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

    spark.sparkContext.setLogLevel("DEBUG")

    val filePath = "/Users/xulijie/Documents/data/RandomText/randomText-100MB.txt"
    val textFile = spark.sparkContext.textFile(filePath)
    val result = textFile.flatMap(_.split("[\\.]"))
      .map(word => (word, 1)).reduceByKey(_ + _)

    println(result.toDebugString)

    val outputDir = "/Users/xulijie/Documents/data/WordCount/randomtText-100MB.txt"

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(outputDir), hadoopConf)

    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(outputDir), true)
    } catch {
      case t : Throwable => println(t)
    }

    result.saveAsTextFile(outputDir)
    spark.stop()
  }
}
