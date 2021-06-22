package cap3

import org.apache.spark.sql.SparkSession

object ejercicios3 {
  def ej_parquet(){

    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .getOrCreate()

    val data = Seq(("James ","","Smith","36636","M",3000),
      ("Michael ","Rose","","40288","M",4000),
      ("Robert ","","Williams","42114","M",4000),
      ("Maria ","Anne","Jones","39192","F",4000),
      ("Jen","Mary","Brown","","F",-1))

    val columns = Seq("firstname","middlename","lastname","dob","gender","salary")

    import spark.sqlContext.implicits._
    val df = data.toDF(columns:_*).persist()


    df
      .coalesce(4)
      .write
      .format("parquet")
      .mode("overwrite")
      .save("../../Datasets_CursoSpark/cap3_parquet.parquet")


  }
}
