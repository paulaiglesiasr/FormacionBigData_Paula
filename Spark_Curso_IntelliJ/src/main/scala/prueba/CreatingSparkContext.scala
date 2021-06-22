package prueba

import org.apache.spark.{SparkConf, SparkContext}

// Scala object
object CreatingSparkContext {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/Users/paula.iglesias/Downloads/winutils-master/winutils-master/hadoop-2.6.0/bin")

    val sparkConf = new SparkConf()
    sparkConf.setAppName("First Spark Application")
    sparkConf.setMaster("local")

    val sc = new SparkContext(sparkConf)

    val array = Array(1, 2, 3, 4, 5, 6, 7, 8, 8, 9, 0)

    val arrayRDD = sc.parallelize(array, 2)


    // System.setProperty("hadoop.home.dir", "D:\\SOFTWARE\\BIGDATA\\hadoop-common-2.2.0-bin-master\\")

    println("Num of elements in RDD: ", arrayRDD.count())
    arrayRDD.foreach(println)
  }
}
