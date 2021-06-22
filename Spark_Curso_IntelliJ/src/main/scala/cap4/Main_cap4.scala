package cap4

import org.apache.spark.{SparkConf, SparkContext}

object Main_cap4 {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:/Users/paula.iglesias/Downloads/winutils-master/winutils-master/hadoop-2.7.1/bin")
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Spark Application")
    sparkConf.setMaster("local")
    val sc = new SparkContext(sparkConf)

    // Cap 3

    ejercicios4.ej_libro_tempview()
  }
}
