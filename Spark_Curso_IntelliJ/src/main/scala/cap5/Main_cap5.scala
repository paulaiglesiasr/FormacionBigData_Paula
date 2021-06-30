package cap5

import org.apache.spark.{SparkConf, SparkContext}

object Main_cap5 {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:/Users/paula.iglesias/Documents/winutils-master/hadoop-2.7.1")
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Spark Application")
    sparkConf.setMaster("local")
    val sc = new SparkContext(sparkConf)

    // Cap 5

    //PostreSQL.helloWorld()
    //MySQL.helloWorld()
    MySQL_empleados.consulta1()
  }
}
