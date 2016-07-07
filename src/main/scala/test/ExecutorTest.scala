package test

/**
  * Created by xulijie on 16-6-25.
  */
object ExecutorTest {

  var minNumExecutors = 0
  var maxNumExecutors = Integer.MAX_VALUE

  def setExecutorNum(numTasks: Int, tasksPerExecutor: Int): Unit = {
    //if (minNumExecutors == 0 && maxNumExecutors == Integer.MAX_VALUE) {
      val initialExecutorNum = (numTasks + tasksPerExecutor - 1) / tasksPerExecutor
      if (numTasks <= 8) {
        maxNumExecutors = initialExecutorNum * 2
      } else if (numTasks <= 32) {
        maxNumExecutors = initialExecutorNum * 2
      } else {
        maxNumExecutors = initialExecutorNum * 2
      }
    //}
  }

  def main(args: Array[String]) {

    var tasksPerExecutor = 1

    while (tasksPerExecutor <= 64) {
      println("taskPerExecutor = " + tasksPerExecutor)
      for (numTasks <- 1 to 1000) {
        setExecutorNum(numTasks, tasksPerExecutor)
        println("numTasks = " + numTasks + "/" + maxNumExecutors * tasksPerExecutor + ", maxNumExecutor = "
          + maxNumExecutors)
      }
      tasksPerExecutor *= 2
    }

  }

}
