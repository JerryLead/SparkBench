package yarn.cluster

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by lijie on 16-6-2.
  * --class yarn.cluster.WordCount --master yarn --deploy-mode client --executor-memory 2g --executor-cores 2 --queue experiments target/SparkBench-1.0-SNAPSHOT.jar
  */
object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount")
    // conf.setMaster("local")
    val sc = new SparkContext(conf)

    val filePath = "src/main/scala/yarn/cluster/WordCount.scala"
    val textFile = sc.textFile(filePath)
    val result = textFile.flatMap(_.split("[ |\\.]"))
      .map(word => (word, 1)).reduceByKey(_ + _)

    result.foreach(println)
  }

}
