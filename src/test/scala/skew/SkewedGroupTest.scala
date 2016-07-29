package skew

import java.util.Random

/**
  * Created by xulijie on 16-7-18.
  */
object SkewedGroupTest {
  def main(args: Array[String]) {

    var numMappers = 20
    var numKVPairs = 1000
    var valSize = 1000
    var numReducers = 8
    var ratio = 2.0

    val pairs1 =(0 until numMappers).flatMap { p =>
      val ranGen = new Random
      var result = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        val offset = ranGen.nextInt(1000) * numReducers
        if (ranGen.nextDouble < ratio / (numReducers + ratio - 1)) {
          // give ratio times higher chance of generating key 0 (for reducer 0)
          result(i) = (offset, byteArr)
        } else {
          // generate a key for one of the other reducers
          val key = 1 + ranGen.nextInt(numReducers - 1) + offset
          result(i) = (key, byteArr)
        }
      }
      result
    }

    pairs1.foreach(kv => println(kv._1))
  }
}
