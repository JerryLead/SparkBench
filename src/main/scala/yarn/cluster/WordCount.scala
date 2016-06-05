package yarn.cluster

import org.apache.spark.{SparkContext, SparkConf}

/**
  * SET HADOOP_CONF_DIR = SparkBench/hadoop-conf-dir
  * SET HADOOP_USER_NAME=lijie
  * Created by lijie on 16-6-2.
  * --class yarn.cluster.WordCount --master yarn --deploy-mode client --executor-memory 2g --executor-cores 2 --queue experiments target/SparkBench-1.0-SNAPSHOT.jar
  */
object WordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("WordCount")
    conf.set("spark.yarn.jar", "hdfs://master:9000/spark_lib/spark-assembly-1.6.2-SNAPSHOT-hadoop2.6.4.jar")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "hdfs://master:9000/sparklogs")
    conf.set("spark.yarn.historyServer.address", "master:18080")
    // conf.setMaster("local-cluster[2, 1, 2048]")
    val sc = new SparkContext(conf)

    // val filePath = "src/main/scala/yarn/cluster/WordCount.scala"
    val filePath = "hdfs://master:9000/user/lijie/input"
    val textFile = sc.textFile(filePath)
    val result = textFile.flatMap(_.split("[ |\\.]"))
      .map(word => (word, 1)).reduceByKey(_ + _)

    result.collect().foreach(println)
    sc.stop()
  }

}
