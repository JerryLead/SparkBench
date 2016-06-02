package yarn.cluster

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by lijie on 16-6-2.
  */
object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val filePath = "src/main/scala/yarn/cluster/WordCount.scala"
    val textFile = sc.textFile(filePath)
    val result = textFile.flatMap(_.split("[ |\\.]"))
      .map(word => (word, 1)).reduceByKey(_ + _)

    result.foreach(println)
  }

}
