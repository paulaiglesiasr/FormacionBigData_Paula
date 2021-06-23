package cap4

import org.apache.spark.sql.SparkSession

object Parquet {
  def read_parquet_into_sql(){

    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .getOrCreate()
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl" +
                      " USING parquet" +
                      " OPTIONS (" +
                                " path \"Datos/cap3_parquet.parquet/\" )")
    
    spark.sql("SELECT * FROM us_delay_flights_tbl").show()
  }
}
