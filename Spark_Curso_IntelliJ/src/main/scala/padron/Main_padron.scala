package padron

import cap5.EjBasico_y_comparacionSQLAPI
import org.apache.spark.{SparkConf, SparkContext}

object Main_padron {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:/Users/paula.iglesias/Documents/winutils-master/hadoop-2.7.1")

    // Cap 5

    //"COD_DISTRITO";"DESC_DISTRITO";"COD_DIST_BARRIO";"DESC_BARRIO";"COD_BARRIO";"COD_DIST_SECCION";"COD_SECCION";"COD_EDAD_INT";"EspanolesHombres";"EspanolesMujeres";"ExtranjerosHombres";"ExtranjerosMujeres"

    //Ej1_CreacionTablas.main()
    //Ej1_CreacionTablas_expresionregular.main()

    Ej2_parquet.main()
  }
}
