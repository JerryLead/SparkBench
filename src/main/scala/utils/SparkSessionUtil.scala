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








spark.dynamicAllocation.enabled             true
spark.dynamicAllocation.executorIdleTimeout 60s
spark.dynamicAllocation.initialExecutors    1
spark.dynamicAllocation.minExecutors        0
spark.dynamicAllocation.maxExecutors        4
spark.dynamicAllocation.schedulerBacklogTimeout  1s
spark.shuffle.service.enabled               true

spark.executor.cores                        2
spark.executor.memory                       2g
}
