package cap2

import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:/Users/paula.iglesias/Downloads/winutils-master/winutils-master/hadoop-2.6.0/bin")
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Spark Application")
    sparkConf.setMaster("local")
    val sc = new SparkContext(sparkConf)

    // Cap 2

    //MnMCount.ej1(Array("C:/Users/paula.iglesias/Desktop/Datasets_CursoSpark/mnm_dataset.txt"))
    //MnMCount.ej2(Array("C:/Users/paula.iglesias/Desktop/Datasets_CursoSpark/mnm_dataset.txt"))

    Quijote.main()
  }
}
