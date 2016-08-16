package utils

import org.apache.spark.sql.SparkSession

/**
  * Created by xulijie on 16-8-15.
  */
object SparkSessionUtil {

  def localSparkSession(master: String, appName: String) = {
    SparkSession
      .builder
      .master(master)
      .appName(appName)
      .getOrCreate()
  }

  def clusterSparkSession(master: String, appName: String) = {
    SparkSession
      .builder
      .master(master)
      .appName(appName)
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "hdfs://master:9000/sparklogs")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.memory", "1g")
      .config("spark.yarn.historyServer.address", "master:18080")
      .config("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails")
      .config("spark.yarn.jar", "hdfs://master:9000/spark_lib/spark-assembly-1.6.3-SNAPSHOT-hadoop2.7.1.jar")
      .getOrCreate()
  }

  def clusterDynamicSparkSession(master: String, appName: String) = {
    SparkSession
      .builder
      .master(master)
      .appName(appName)
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "hdfs://master:9000/sparklogs")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.memory", "1g")
      .config("spark.yarn.historyServer.address", "master:18080")
      // .config("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails")
      // .config("spark.yarn.jar", "hdfs://master:9000/spark_lib/spark-assembly-1.6.3-SNAPSHOT-hadoop2.7.1.jar")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.dynamicAllocation.executorIdleTimeout", "60s")
      .config("spark.dynamicAllocation.initialExecutors", "1")
      .config("spark.dynamicAllocation.minExecutors", "0")
      .config("spark.dynamicAllocation.maxExecutors", "4")
      .config("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")
      .config("spark.shuffle.service.enabled", "true")
      // set executor size
      .config("spark.executor.cores", "2")
      .config("spark.executor.memory", "2g")
      .getOrCreate()
  }
}
