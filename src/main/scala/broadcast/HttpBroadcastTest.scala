package broadcast

import org.apache.spark.{SparkConf, SparkContext}

/**
  * BroadcastTest
  */
object HttpBroadcastTest {
  def main(args: Array[String]) {
    val blockSize = "4096"

    val sparkConf = new SparkConf().setAppName("HttpBroadcast Test")
      .set("spark.broadcast.factory", s"org.apache.spark.broadcast.HttpBroadcastFactory")
      .set("spark.broadcast.blockSize", blockSize)
    val sc = new SparkContext(sparkConf)

    val slices = 5
    val num = 1000000

    val arr1 = (0 until num).toArray

    for (i <- 0 until 3) {
      println("Iteration " + i)
      println("===========")
      val startTime = System.nanoTime
      val barr1 = sc.broadcast(arr1)
      val observedSizes = sc.parallelize(1 to 10, slices).map(_ => barr1.value.size)
      // Collect the small RDD so we can print the observed sizes locally.
      observedSizes.collect().foreach(i => println(i))
      println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6))
    }

    sc.stop()
  }
}
