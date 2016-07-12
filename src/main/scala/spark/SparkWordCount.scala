package spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by xulijie on 16-7-12.
  */
object SparkWordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkWordCount <inputFile> <outputDir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Spark WordCount")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))
    val result = textFile.flatMap(_.split("[ |\\.]"))
      .map(word => (word, 1)).reduceByKey(_ + _)

    result.saveAsTextFile(args(1))
  }
}
