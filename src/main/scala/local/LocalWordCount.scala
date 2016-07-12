package local

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by xulijie on 16-6-16.
  */
object LocalWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)


    val filePath = "src/main/scala/local/LocalWordCount.scala"
    val textFile = sc.textFile(filePath)
    val result = textFile.flatMap(_.split("[ |\\.]"))
      .map(word => (word, 1)).reduceByKey(_ + _)

    result.collect().foreach(println)
  }
}
