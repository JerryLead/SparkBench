package yarn.cluster

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by xulijie on 16-6-7.
  * --class yarn.cluster.WordCount --master yarn --deploy-mode client --executor-memory 2g --executor-cores 2 --queue experiments target/SparkBench-1.0-SNAPSHOT.jar
  */
object HBaseTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseTest")
    val sc = new SparkContext(sparkConf)

    // please ensure HBASE_CONF_DIR is on classpath of spark driver
    // e.g: set it through spark.driver.extraClassPath property
    // in spark-defaults.conf or through --driver-class-path
    // command line option of spark-submit

    val conf = HBaseConfiguration.create()

    if (args.length < 1) {
      System.err.println("Usage: HBaseTest <table_name>")
      System.exit(1)
    }

    // Other options for configuring scan behavior are available. More information available at
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
    conf.set(TableInputFormat.INPUT_TABLE, args(0))

    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(args(0))) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(args(0)))
      admin.createTable(tableDesc)
    }

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD.count()

    sc.stop()
    admin.close()
  }
}
