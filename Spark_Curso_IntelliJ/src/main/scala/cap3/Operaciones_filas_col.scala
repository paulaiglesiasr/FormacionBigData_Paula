package cap3

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object Operaciones_filas_col {
  def op(args: Array[String]){

    val spark = SparkSession
      .builder
      .appName("Example-3_7")
      .getOrCreate()

    if (args.length <= 0) {
      println("usage Example3_7 <file path to blogs.json>")
      System.exit(1)
    }

    // Get the path to the JSON file
    val jsonFile = args(0)

    // Define our schema programmatically
    val schema = StructType(Array(StructField("Id", IntegerType, false),
                  StructField("First", StringType, false),
                  StructField("Last", StringType, false),
                  StructField("Url", StringType, false),
                  StructField("Published", StringType, false),
                  StructField("Hits", IntegerType, false),
                  StructField("Campaigns", ArrayType(StringType), false)))

    // Create a DataFrame by reading from the JSON file
    // with a predefined schema
    val blogsDF = spark.read.schema(schema).json(jsonFile)

    //Añade una columna con condicion sobre otra
    blogsDF.withColumn("Big Hitters", (expr("Hits > 10000")))
    .show()

    // Concatenate three columns, create a new column, and show the
    // newly created concatenated column
    blogsDF
      .withColumn("AuthorsId", concat(expr("First"), expr("Last"), expr("Id")))
      .select(col("AuthorsId"))
      .show(4)

    // Pag 57

    // Create a Row
    val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
      Array("twitter", "LinkedIn"))
    // Access using index for individual items
    print("\n1:\n")
    print(blogRow.toString())
    print("\n2:\n")
    print(blogRow.mkString("\n"))
    print("\n3:\n")
    print(blogRow(6))

    // Create DataFrame as from row
    import org.apache.spark.sql.Row
    //val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
    //val authorsDF = rows.toDF("Author", "State")
    //authorsDF.show()

    //Renombrando columnas
    val newFireDF = blogsDF.withColumnRenamed("Hits", "NewHits")
    newFireDF
      .select("NewHits")
      .where(col("NewHits") > 10000)
      .show(5, false)

  }

}
