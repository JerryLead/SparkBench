## Test the cases under src/main/scala/spark

SET HADOOP_CONF_DIR = SparkBench/hadoop-conf-dir
SET HADOOP_USER_NAME=lijie

--class yarn.cluster.WordCount --master yarn --deploy-mode client --executor-memory 2g --executor-cores 2 --queue experiments target/SparkBench-1.0-SNAPSHOT.jar
