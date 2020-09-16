package whu.edu.cn.query.util

import scala.collection.mutable.ArrayBuffer

class RuntimeData {
  def apply(unit: Unit): Unit = {}

  var sum:Int = 0
  //默认线程数
  var defThreadCount:Int = 5
  //已经执行完成的线程数
  var finishThreadCount:Int = 0

  def this(_sum: Int, _defThreadCount:Int, _finishThreadCount:Int)= {
    this()
    sum = _sum
    //默认线程数
    defThreadCount = _defThreadCount
    //已经执行完成的线程数
    finishThreadCount = _finishThreadCount
  }

  def getThreadCount(array: Array[Int]): Int = {
    if (array.length < defThreadCount) return array.length
    defThreadCount
  }

}

object RuntimeData{
  def sum(array: Array[Int]): Int = {
    val rd = new RuntimeData(0, 5, 0)
    val threadCount = rd.getThreadCount(array)
    //println("thread count:" + threadCount)
    val lenPerThread = array.length / threadCount
    for(i <- 0 until threadCount){
      val index = i
      new Thread(){
        override def run(): Unit = {
          var s:Int = 0
          val start = index * lenPerThread
          val end = start + lenPerThread
          for(j <- start until end) s += array(j)
          println("Thread " + Thread.currentThread() + "is running...")
          synchronized(rd){
            rd.sum += s
            rd.finishThreadCount += 1
          }
        }
      }.start()
    }

    while (rd.finishThreadCount != threadCount) {
      try {
        Thread.sleep(1);
      }catch {
        case ex: InterruptedException  => {
          ex.printStackTrace() // 打印到标准err
          System.err.println("exception===>: ...")
        }
      }
    }

    rd.sum
  }

  def main(args: Array[String]): Unit = {
    val array = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    var sum = 0
    val serialStart = System.currentTimeMillis()
    array.foreach(sum += _)
    val serialEnd = System.currentTimeMillis()
    println(sum + ":" + (serialEnd - serialStart))

    val parallelStart = System.currentTimeMillis()
    sum = RuntimeData.sum(array)
    val parallelEnd = System.currentTimeMillis()
    println(sum + ":" + (parallelEnd - parallelStart))

  }

}
